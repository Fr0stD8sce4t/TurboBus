#include "turbobus/executor.h"

#include <cuda_runtime.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace turbobus {

namespace {

void CheckCuda(cudaError_t result, const char* message) {
  if (result != cudaSuccess) {
    throw std::runtime_error(std::string(message) + ": " +
                             cudaGetErrorString(result));
  }
}

void IgnoreCuda(cudaError_t result) {
  (void)result;
}

double Gbps(std::size_t bytes, double milliseconds) {
  if (milliseconds <= 0.0) {
    return 0.0;
  }
  const double seconds = milliseconds / 1000.0;
  const double gib = static_cast<double>(bytes) / (1024.0 * 1024.0 * 1024.0);
  return gib / seconds;
}

void AddRelayStats(TransferStats* stats, int relay_device, std::size_t bytes,
                   std::size_t chunks) {
  for (std::size_t i = 0; i < stats->relay_devices.size(); ++i) {
    if (stats->relay_devices[i] == relay_device) {
      stats->relay_device_bytes[i] += bytes;
      stats->relay_device_chunks[i] += chunks;
      return;
    }
  }
  stats->relay_devices.push_back(relay_device);
  stats->relay_device_bytes.push_back(bytes);
  stats->relay_device_chunks.push_back(chunks);
}

PathStats MakePathStats(const PathAssignment& assignment, std::size_t bytes) {
  PathStats stats;
  stats.kind = assignment.path.kind;
  stats.direction = assignment.path.direction;
  stats.target_device = assignment.path.target_device;
  stats.relay_device = assignment.path.relay_device;
  stats.bytes = bytes;
  stats.chunks = assignment.chunks.size();
  return stats;
}

bool IsDirectPath(PathKind kind) {
  return kind == PathKind::DirectH2D || kind == PathKind::DirectD2H;
}

bool IsRelayPath(PathKind kind) {
  return kind == PathKind::RelayH2DThenP2P || kind == PathKind::RelayP2PThenD2H;
}

TransferStats BuildInitialStats(const TransferPlan& plan) {
  TransferStats stats;
  stats.bytes = plan.total_bytes;
  for (const auto& assignment : plan.assignments) {
    std::size_t assignment_bytes = 0;
    for (const auto& chunk : assignment.chunks) {
      assignment_bytes += chunk.bytes;
    }
    if (IsDirectPath(assignment.path.kind)) {
      stats.direct_chunks += assignment.chunks.size();
      stats.direct_bytes += assignment_bytes;
    } else if (IsRelayPath(assignment.path.kind)) {
      stats.relay_chunks += assignment.chunks.size();
      stats.relay_bytes += assignment_bytes;
      AddRelayStats(&stats, assignment.path.relay_device, assignment_bytes,
                    assignment.chunks.size());
    }
    stats.path_stats.push_back(MakePathStats(assignment, assignment_bytes));
  }
  return stats;
}

std::size_t MaxSourceEnd(const TransferPlan& plan) {
  std::size_t end = 0;
  for (const auto& assignment : plan.assignments) {
    for (const auto& chunk : assignment.chunks) {
      end = std::max(end, chunk.src_offset + chunk.bytes);
    }
  }
  return end;
}

std::size_t MaxDestinationEnd(const TransferPlan& plan) {
  std::size_t end = 0;
  for (const auto& assignment : plan.assignments) {
    for (const auto& chunk : assignment.chunks) {
      end = std::max(end, chunk.dst_offset + chunk.bytes);
    }
  }
  return end;
}

}  // namespace

struct CudaRelayExecutor::Impl {
  struct PathTiming {
    std::size_t stats_index = 0;
    cudaEvent_t start = nullptr;
    cudaEvent_t stop = nullptr;
  };

  struct RelayState {
    int relay_device = kHostDevice;
    cudaStream_t h2d_stream = nullptr;
    cudaStream_t p2p_stream = nullptr;
    std::vector<void*> staging_slots;
    std::vector<cudaEvent_t> h2d_done_events;
    std::vector<cudaEvent_t> p2p_done_events;
    std::vector<cudaEvent_t> slot_ready_events;
    std::size_t next_slot_index = 0;
  };

  int target_device = 0;
  RuntimeOptions options;
  cudaStream_t direct_stream = nullptr;
  std::unordered_map<int, RelayState> relays;
  std::atomic<std::uint64_t> next_id{1};
  std::unordered_map<std::uint64_t, std::vector<cudaEvent_t>> transfer_events;
  std::unordered_map<std::uint64_t, std::pair<cudaEvent_t, cudaEvent_t>> timing_events;
  std::unordered_map<std::uint64_t, cudaStream_t> timing_streams;
  std::unordered_map<std::uint64_t, std::vector<PathTiming>> path_timing_events;
  std::unordered_map<std::uint64_t, std::chrono::steady_clock::time_point> start_times;
  std::unordered_map<std::uint64_t, TransferStats> completed_stats;
  std::unordered_set<std::uint64_t> waiting_transfers;
  std::condition_variable waiters_idle;
  std::mutex mutex;

  PathTiming CreatePathTiming(std::size_t stats_index, cudaStream_t stream,
                              const char* label) {
    PathTiming path_timing;
    path_timing.stats_index = stats_index;
    CheckCuda(cudaEventCreate(&path_timing.start),
              (std::string("cudaEventCreate ") + label + " path start failed")
                  .c_str());
    CheckCuda(cudaEventCreate(&path_timing.stop),
              (std::string("cudaEventCreate ") + label + " path stop failed")
                  .c_str());
    CheckCuda(cudaEventRecord(path_timing.start, stream),
              (std::string("cudaEventRecord ") + label + " path start failed")
                  .c_str());
    return path_timing;
  }

  void ClearStagingSlotOnChunk(void* slot, std::size_t bytes, cudaStream_t stream,
                               const char* label) {
    if (!options.clear_relay_staging_on_chunk) {
      return;
    }
    CheckCuda(cudaMemsetAsync(slot, 0, bytes, stream),
              (std::string(label) + " cudaMemsetAsync staging clear failed")
                  .c_str());
  }

  cudaEvent_t RecordCompletion(cudaStream_t stream, const char* label) {
    cudaEvent_t completion = nullptr;
    CheckCuda(cudaEventCreateWithFlags(&completion, cudaEventDisableTiming),
              (std::string("cudaEventCreate ") + label + " completion failed")
                  .c_str());
    CheckCuda(cudaEventRecord(completion, stream),
              (std::string("cudaEventRecord ") + label + " completion failed")
                  .c_str());
    return completion;
  }

  void SynchronizeStreamsForDestroy() noexcept {
    if (direct_stream != nullptr) {
      IgnoreCuda(cudaStreamSynchronize(direct_stream));
    }
    for (auto& item : timing_streams) {
      if (item.second != nullptr) {
        IgnoreCuda(cudaStreamSynchronize(item.second));
      }
    }
    for (auto& [_, relay] : relays) {
      IgnoreCuda(cudaSetDevice(relay.relay_device));
      if (relay.h2d_stream != nullptr) {
        IgnoreCuda(cudaStreamSynchronize(relay.h2d_stream));
      }
      if (relay.p2p_stream != nullptr) {
        IgnoreCuda(cudaStreamSynchronize(relay.p2p_stream));
      }
    }
  }

  PathTiming SubmitDirectPath(const PathAssignment& assignment,
                              std::size_t assignment_index,
                              const char* source_bytes, char* target_bytes) {
    CheckCuda(cudaSetDevice(target_device), "cudaSetDevice direct failed");
    auto path_timing =
        CreatePathTiming(assignment_index, direct_stream, "direct");
    const cudaMemcpyKind copy_kind =
        assignment.path.kind == PathKind::DirectH2D ? cudaMemcpyHostToDevice
                                                    : cudaMemcpyDeviceToHost;
    for (const auto& chunk : assignment.chunks) {
      CheckCuda(cudaMemcpyAsync(target_bytes + chunk.dst_offset,
                                source_bytes + chunk.src_offset, chunk.bytes,
                                copy_kind, direct_stream),
                "direct cudaMemcpyAsync failed");
    }
    CheckCuda(cudaEventRecord(path_timing.stop, direct_stream),
              "cudaEventRecord direct path stop failed");
    return path_timing;
  }

  PathTiming SubmitRelayH2DPath(const PathAssignment& assignment,
                                std::size_t assignment_index,
                                const char* host_bytes, char* target_bytes) {
    auto relay_it = relays.find(assignment.path.relay_device);
    if (relay_it == relays.end()) {
      throw std::runtime_error("transfer plan references an uninitialized relay");
    }

    auto& relay = relay_it->second;
    CheckCuda(cudaSetDevice(relay.relay_device), "cudaSetDevice relay submit failed");
    auto path_timing =
        CreatePathTiming(assignment_index, relay.h2d_stream, "relay");
    std::size_t slot_index = relay.next_slot_index;
    for (const auto& chunk : assignment.chunks) {
      if (chunk.bytes > options.chunk_bytes) {
        throw std::runtime_error("chunk is larger than relay staging slot");
      }
      slot_index %= relay.staging_slots.size();
      void* slot = relay.staging_slots[slot_index];
      cudaEvent_t h2d_done = relay.h2d_done_events[slot_index];
      cudaEvent_t p2p_done = relay.p2p_done_events[slot_index];

      if (relay.slot_ready_events[slot_index] != nullptr) {
        CheckCuda(cudaStreamWaitEvent(relay.h2d_stream,
                                      relay.slot_ready_events[slot_index], 0),
                  "cudaStreamWaitEvent staging slot reuse failed");
      }

      CheckCuda(cudaMemcpyAsync(slot, host_bytes + chunk.src_offset, chunk.bytes,
                                cudaMemcpyHostToDevice, relay.h2d_stream),
                "relay h2d cudaMemcpyAsync failed");
      CheckCuda(cudaEventRecord(h2d_done, relay.h2d_stream),
                "cudaEventRecord relay h2d_done failed");

      CheckCuda(cudaStreamWaitEvent(relay.p2p_stream, h2d_done, 0),
                "cudaStreamWaitEvent relay p2p failed");
      CheckCuda(cudaMemcpyPeerAsync(target_bytes + chunk.dst_offset,
                                    target_device, slot, relay.relay_device,
                                    chunk.bytes, relay.p2p_stream),
                "relay p2p cudaMemcpyPeerAsync failed");
      ClearStagingSlotOnChunk(slot, chunk.bytes, relay.p2p_stream, "relay h2d");
      CheckCuda(cudaEventRecord(p2p_done, relay.p2p_stream),
                "cudaEventRecord relay p2p_done failed");
      relay.slot_ready_events[slot_index] = p2p_done;
      ++slot_index;
    }
    relay.next_slot_index = slot_index % relay.staging_slots.size();
    CheckCuda(cudaEventRecord(path_timing.stop, relay.p2p_stream),
              "cudaEventRecord relay path stop failed");
    return path_timing;
  }

  PathTiming SubmitRelayD2HPath(const PathAssignment& assignment,
                                std::size_t assignment_index,
                                const char* target_bytes, char* host_bytes) {
    auto relay_it = relays.find(assignment.path.relay_device);
    if (relay_it == relays.end()) {
      throw std::runtime_error("transfer plan references an uninitialized relay");
    }

    auto& relay = relay_it->second;
    CheckCuda(cudaSetDevice(relay.relay_device), "cudaSetDevice relay submit failed");
    auto path_timing =
        CreatePathTiming(assignment_index, relay.p2p_stream, "relay");
    std::size_t slot_index = relay.next_slot_index;
    for (const auto& chunk : assignment.chunks) {
      if (chunk.bytes > options.chunk_bytes) {
        throw std::runtime_error("chunk is larger than relay staging slot");
      }
      slot_index %= relay.staging_slots.size();
      void* slot = relay.staging_slots[slot_index];
      cudaEvent_t p2p_done = relay.p2p_done_events[slot_index];
      cudaEvent_t h2d_done = relay.h2d_done_events[slot_index];

      if (relay.slot_ready_events[slot_index] != nullptr) {
        CheckCuda(cudaStreamWaitEvent(relay.p2p_stream,
                                      relay.slot_ready_events[slot_index], 0),
                  "cudaStreamWaitEvent d2h staging slot reuse failed");
      }

      CheckCuda(cudaMemcpyPeerAsync(slot, relay.relay_device,
                                    target_bytes + chunk.src_offset,
                                    target_device, chunk.bytes,
                                    relay.p2p_stream),
                "relay reverse p2p cudaMemcpyPeerAsync failed");
      CheckCuda(cudaEventRecord(p2p_done, relay.p2p_stream),
                "cudaEventRecord relay reverse p2p_done failed");

      CheckCuda(cudaStreamWaitEvent(relay.h2d_stream, p2p_done, 0),
                "cudaStreamWaitEvent relay d2h failed");
      CheckCuda(cudaMemcpyAsync(host_bytes + chunk.dst_offset, slot, chunk.bytes,
                                cudaMemcpyDeviceToHost, relay.h2d_stream),
                "relay d2h cudaMemcpyAsync failed");
      ClearStagingSlotOnChunk(slot, chunk.bytes, relay.h2d_stream, "relay d2h");
      CheckCuda(cudaEventRecord(h2d_done, relay.h2d_stream),
                "cudaEventRecord relay d2h_done failed");
      relay.slot_ready_events[slot_index] = h2d_done;
      ++slot_index;
    }
    relay.next_slot_index = slot_index % relay.staging_slots.size();
    CheckCuda(cudaEventRecord(path_timing.stop, relay.h2d_stream),
              "cudaEventRecord relay d2h path stop failed");
    return path_timing;
  }

  TransferHandle SubmitTransfer(const BufferView& source,
                                const BufferView& destination,
                                const TransferPlan& plan,
                                TransferDirection direction) {
    if (source.ptr == nullptr || destination.ptr == nullptr) {
      throw std::invalid_argument("source and destination pointers must not be null");
    }
    if (source.bytes < MaxSourceEnd(plan) ||
        destination.bytes < MaxDestinationEnd(plan)) {
      throw std::invalid_argument("buffer is smaller than transfer plan");
    }
    if (direction == TransferDirection::H2D) {
      if (source.kind != MemoryKind::HostPinned) {
        throw std::invalid_argument("source buffer must be pinned host memory");
      }
      if (destination.kind != MemoryKind::Device ||
          destination.device != target_device) {
        throw std::invalid_argument("destination buffer must be on target_device");
      }
    } else {
      if (source.kind != MemoryKind::Device || source.device != target_device) {
        throw std::invalid_argument("source buffer must be on target_device");
      }
      if (destination.kind != MemoryKind::HostPinned) {
        throw std::invalid_argument("destination buffer must be pinned host memory");
      }
    }

    const auto* source_bytes = static_cast<const char*>(source.ptr);
    auto* destination_bytes = static_cast<char*>(destination.ptr);
    const auto submitted_at = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mutex);
    const std::uint64_t id = next_id.fetch_add(1);
    std::vector<cudaEvent_t> completion_events;
    std::vector<PathTiming> path_timing_events;
    cudaEvent_t timing_start = nullptr;
    cudaEvent_t timing_stop = nullptr;
    cudaStream_t timing_stream = nullptr;
    TransferStats stats = BuildInitialStats(plan);

    try {
      CheckCuda(cudaSetDevice(target_device), "cudaSetDevice timing failed");
      CheckCuda(cudaStreamCreateWithFlags(&timing_stream, cudaStreamNonBlocking),
                "cudaStreamCreate transfer timing failed");
      CheckCuda(cudaEventCreate(&timing_start), "cudaEventCreate timing start failed");
      CheckCuda(cudaEventCreate(&timing_stop), "cudaEventCreate timing stop failed");
      CheckCuda(cudaEventRecord(timing_start, timing_stream),
                "cudaEventRecord timing start failed");

      for (std::size_t assignment_index = 0;
           assignment_index < plan.assignments.size(); ++assignment_index) {
        const auto& assignment = plan.assignments[assignment_index];
        if (assignment.path.direction != direction) {
          throw std::runtime_error(
              "transfer plan direction does not match submit direction");
        }

        if (IsDirectPath(assignment.path.kind)) {
          auto path_timing = SubmitDirectPath(
              assignment, assignment_index, source_bytes, destination_bytes);
          cudaEvent_t completion = RecordCompletion(direct_stream, "direct");
          completion_events.push_back(completion);
          path_timing_events.push_back(path_timing);
          continue;
        }

        PathTiming path_timing;
        cudaStream_t completion_stream = nullptr;
        if (assignment.path.kind == PathKind::RelayH2DThenP2P) {
          path_timing = SubmitRelayH2DPath(assignment, assignment_index,
                                           source_bytes, destination_bytes);
          completion_stream = relays.at(assignment.path.relay_device).p2p_stream;
        } else if (assignment.path.kind == PathKind::RelayP2PThenD2H) {
          path_timing = SubmitRelayD2HPath(assignment, assignment_index,
                                           source_bytes, destination_bytes);
          completion_stream = relays.at(assignment.path.relay_device).h2d_stream;
        } else {
          throw std::runtime_error("unsupported relay path kind");
        }
        cudaEvent_t completion = RecordCompletion(completion_stream, "relay");
        completion_events.push_back(completion);
        path_timing_events.push_back(path_timing);
      }

      CheckCuda(cudaSetDevice(target_device), "cudaSetDevice timing stop failed");
      for (auto event : completion_events) {
        CheckCuda(cudaStreamWaitEvent(timing_stream, event, 0),
                  "cudaStreamWaitEvent timing stop failed");
      }
      CheckCuda(cudaEventRecord(timing_stop, timing_stream),
                "cudaEventRecord timing stop failed");

      transfer_events.emplace(id, std::move(completion_events));
      timing_events.emplace(id, std::make_pair(timing_start, timing_stop));
      timing_streams.emplace(id, timing_stream);
      this->path_timing_events.emplace(id, std::move(path_timing_events));
      timing_start = nullptr;
      timing_stop = nullptr;
      timing_stream = nullptr;
      start_times.emplace(id, submitted_at);
      completed_stats.emplace(id, stats);
    } catch (...) {
      for (auto event : completion_events) {
        IgnoreCuda(cudaEventDestroy(event));
      }
      for (auto& path_timing : path_timing_events) {
        if (path_timing.start != nullptr) {
          IgnoreCuda(cudaEventDestroy(path_timing.start));
        }
        if (path_timing.stop != nullptr) {
          IgnoreCuda(cudaEventDestroy(path_timing.stop));
        }
      }
      if (timing_start != nullptr) {
        IgnoreCuda(cudaEventDestroy(timing_start));
      }
      if (timing_stop != nullptr) {
        IgnoreCuda(cudaEventDestroy(timing_stop));
      }
      if (timing_stream != nullptr) {
        IgnoreCuda(cudaStreamDestroy(timing_stream));
      }
      throw;
    }

    TransferHandle handle;
    handle.id = id;
    handle.status = TransferStatus::Submitted;
    handle.stats = stats;
    return handle;
  }

  void Destroy() noexcept {
    std::unique_lock<std::mutex> lock(mutex);
    waiters_idle.wait(lock, [this]() { return waiting_transfers.empty(); });
    SynchronizeStreamsForDestroy();
    for (auto& item : transfer_events) {
      for (auto event : item.second) {
        if (event != nullptr) {
          IgnoreCuda(cudaEventDestroy(event));
        }
      }
    }
    transfer_events.clear();
    for (auto& item : timing_events) {
      if (item.second.first != nullptr) {
        IgnoreCuda(cudaEventDestroy(item.second.first));
      }
      if (item.second.second != nullptr) {
        IgnoreCuda(cudaEventDestroy(item.second.second));
      }
    }
    timing_events.clear();
    for (auto& item : timing_streams) {
      if (item.second != nullptr) {
        IgnoreCuda(cudaStreamDestroy(item.second));
      }
    }
    timing_streams.clear();
    for (auto& item : path_timing_events) {
      for (auto& path_timing : item.second) {
        if (path_timing.start != nullptr) {
          IgnoreCuda(cudaEventDestroy(path_timing.start));
        }
        if (path_timing.stop != nullptr) {
          IgnoreCuda(cudaEventDestroy(path_timing.stop));
        }
      }
    }
    path_timing_events.clear();
    start_times.clear();
    completed_stats.clear();

    if (direct_stream != nullptr) {
      IgnoreCuda(cudaStreamDestroy(direct_stream));
      direct_stream = nullptr;
    }
    for (auto& [_, relay] : relays) {
      IgnoreCuda(cudaSetDevice(relay.relay_device));
      for (void* slot : relay.staging_slots) {
        if (slot != nullptr) {
          if (options.chunk_bytes > 0) {
            IgnoreCuda(cudaMemset(slot, 0, options.chunk_bytes));
          }
          IgnoreCuda(cudaFree(slot));
        }
      }
      for (auto event : relay.h2d_done_events) {
        if (event != nullptr) {
          IgnoreCuda(cudaEventDestroy(event));
        }
      }
      for (auto event : relay.p2p_done_events) {
        if (event != nullptr) {
          IgnoreCuda(cudaEventDestroy(event));
        }
      }
      if (relay.h2d_stream != nullptr) {
        IgnoreCuda(cudaStreamDestroy(relay.h2d_stream));
      }
      if (relay.p2p_stream != nullptr) {
        IgnoreCuda(cudaStreamDestroy(relay.p2p_stream));
      }
    }
    relays.clear();
  }
};

CudaRelayExecutor::CudaRelayExecutor() : impl_(new Impl()) {}

CudaRelayExecutor::~CudaRelayExecutor() {
  if (impl_ != nullptr) {
    impl_->Destroy();
    delete impl_;
    impl_ = nullptr;
  }
}

void CudaRelayExecutor::Init(int target_device, const std::vector<int>& relay_devices,
                             const RuntimeOptions& options) {
  impl_->Destroy();
  impl_->target_device = target_device;
  impl_->options = options;

  if (options.chunk_bytes == 0) {
    throw std::invalid_argument("chunk_bytes must be greater than zero");
  }
  if (options.staging_slots <= 0) {
    throw std::invalid_argument("staging_slots must be greater than zero");
  }

  try {
    CheckCuda(cudaSetDevice(target_device), "cudaSetDevice target failed");
    CheckCuda(cudaStreamCreateWithFlags(&impl_->direct_stream, cudaStreamNonBlocking),
              "cudaStreamCreate direct failed");

    for (const int relay_device : relay_devices) {
      if (relay_device == target_device) {
        continue;
      }

      Impl::RelayState relay;
      relay.relay_device = relay_device;
      relay.staging_slots.resize(static_cast<std::size_t>(options.staging_slots), nullptr);
      relay.h2d_done_events.resize(static_cast<std::size_t>(options.staging_slots), nullptr);
      relay.p2p_done_events.resize(static_cast<std::size_t>(options.staging_slots), nullptr);
      relay.slot_ready_events.resize(static_cast<std::size_t>(options.staging_slots),
                                     nullptr);

      CheckCuda(cudaSetDevice(relay_device), "cudaSetDevice relay failed");
      CheckCuda(cudaStreamCreateWithFlags(&relay.h2d_stream, cudaStreamNonBlocking),
                "cudaStreamCreate relay h2d failed");
      CheckCuda(cudaStreamCreateWithFlags(&relay.p2p_stream, cudaStreamNonBlocking),
                "cudaStreamCreate relay p2p failed");

      for (int i = 0; i < options.staging_slots; ++i) {
        CheckCuda(cudaMalloc(&relay.staging_slots[static_cast<std::size_t>(i)],
                             options.chunk_bytes),
                  "cudaMalloc relay staging failed");
        CheckCuda(cudaMemset(relay.staging_slots[static_cast<std::size_t>(i)], 0,
                             options.chunk_bytes),
                  "cudaMemset relay staging clear failed");
        CheckCuda(cudaEventCreateWithFlags(&relay.h2d_done_events[static_cast<std::size_t>(i)],
                                           cudaEventDisableTiming),
                  "cudaEventCreate relay h2d_done failed");
        CheckCuda(cudaEventCreateWithFlags(&relay.p2p_done_events[static_cast<std::size_t>(i)],
                                           cudaEventDisableTiming),
                  "cudaEventCreate relay p2p_done failed");
      }

      impl_->relays.emplace(relay_device, std::move(relay));
    }

    CheckCuda(cudaSetDevice(target_device), "cudaSetDevice restore target failed");
  } catch (...) {
    impl_->Destroy();
    throw;
  }
}

TransferHandle CudaRelayExecutor::Submit(const BufferView& host, const BufferView& target,
                                         const TransferPlan& plan) {
  return impl_->SubmitTransfer(host, target, plan, TransferDirection::H2D);
}

TransferHandle CudaRelayExecutor::SubmitD2H(const BufferView& target,
                                            const BufferView& host,
                                            const TransferPlan& plan) {
  return impl_->SubmitTransfer(target, host, plan, TransferDirection::D2H);
}

void CudaRelayExecutor::Wait(const TransferHandle& handle) {
  std::vector<cudaEvent_t> completion_events;
  std::pair<cudaEvent_t, cudaEvent_t> timing_events;
  cudaStream_t timing_stream = nullptr;
  std::vector<CudaRelayExecutor::Impl::PathTiming> path_timing_events;
  std::chrono::steady_clock::time_point start_time;
  TransferStats stats;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->waiting_transfers.count(handle.id) != 0) {
      throw std::invalid_argument("transfer handle is already being waited");
    }
    auto event_it = impl_->transfer_events.find(handle.id);
    if (event_it == impl_->transfer_events.end()) {
      throw std::invalid_argument("unknown transfer handle");
    }
    auto timing_it = impl_->timing_events.find(handle.id);
    if (timing_it == impl_->timing_events.end()) {
      throw std::invalid_argument("unknown timing handle");
    }
    auto timing_stream_it = impl_->timing_streams.find(handle.id);
    if (timing_stream_it == impl_->timing_streams.end()) {
      throw std::invalid_argument("unknown timing stream handle");
    }
    auto path_timing_it = impl_->path_timing_events.find(handle.id);
    if (path_timing_it == impl_->path_timing_events.end()) {
      throw std::invalid_argument("unknown path timing handle");
    }
    auto start_it = impl_->start_times.find(handle.id);
    auto stats_it = impl_->completed_stats.find(handle.id);
    if (start_it == impl_->start_times.end() ||
        stats_it == impl_->completed_stats.end()) {
      throw std::invalid_argument("unknown transfer stats handle");
    }
    impl_->waiting_transfers.insert(handle.id);
    completion_events = std::move(event_it->second);
    timing_events = timing_it->second;
    timing_stream = timing_stream_it->second;
    path_timing_events = std::move(path_timing_it->second);
    start_time = start_it->second;
    stats = stats_it->second;
    impl_->transfer_events.erase(event_it);
    impl_->timing_events.erase(timing_it);
    impl_->timing_streams.erase(timing_stream_it);
    impl_->path_timing_events.erase(path_timing_it);
    impl_->start_times.erase(start_it);
    impl_->completed_stats.erase(stats_it);
  }
  try {
    CheckCuda(cudaEventSynchronize(timing_events.second),
              "cudaEventSynchronize timing stop failed");
    const auto end = std::chrono::steady_clock::now();
    const auto microseconds =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start_time)
            .count();
    stats.submit_to_complete_ms = static_cast<double>(microseconds) / 1000.0;
    stats.submit_gib_per_second =
        Gbps(stats.bytes, stats.submit_to_complete_ms);
    float cuda_milliseconds = 0.0f;
    CheckCuda(cudaEventElapsedTime(&cuda_milliseconds, timing_events.first,
                                   timing_events.second),
              "cudaEventElapsedTime transfer failed");
    stats.cuda_elapsed_ms = static_cast<double>(cuda_milliseconds);
    stats.gib_per_second = Gbps(stats.bytes, stats.cuda_elapsed_ms);
    for (const auto& path_timing : path_timing_events) {
      if (path_timing.stats_index >= stats.path_stats.size()) {
        continue;
      }
      float path_milliseconds = 0.0f;
      CheckCuda(cudaEventElapsedTime(&path_milliseconds, path_timing.start,
                                     path_timing.stop),
                "cudaEventElapsedTime path transfer failed");
      auto& path_stats = stats.path_stats[path_timing.stats_index];
      path_stats.cuda_elapsed_ms = static_cast<double>(path_milliseconds);
      path_stats.gib_per_second =
          Gbps(path_stats.bytes, path_stats.cuda_elapsed_ms);
    }
    for (auto event : completion_events) {
      CheckCuda(cudaEventDestroy(event), "cudaEventDestroy completion failed");
    }
    CheckCuda(cudaEventDestroy(timing_events.first),
              "cudaEventDestroy timing start failed");
    CheckCuda(cudaEventDestroy(timing_events.second),
              "cudaEventDestroy timing stop failed");
    for (auto& path_timing : path_timing_events) {
      CheckCuda(cudaEventDestroy(path_timing.start),
                "cudaEventDestroy path timing start failed");
      CheckCuda(cudaEventDestroy(path_timing.stop),
                "cudaEventDestroy path timing stop failed");
    }
    CheckCuda(cudaStreamDestroy(timing_stream), "cudaStreamDestroy timing failed");
  } catch (...) {
    for (auto event : completion_events) {
      if (event != nullptr) {
        IgnoreCuda(cudaEventDestroy(event));
      }
    }
    if (timing_events.first != nullptr) {
      IgnoreCuda(cudaEventDestroy(timing_events.first));
    }
    if (timing_events.second != nullptr) {
      IgnoreCuda(cudaEventDestroy(timing_events.second));
    }
    for (auto& path_timing : path_timing_events) {
      if (path_timing.start != nullptr) {
        IgnoreCuda(cudaEventDestroy(path_timing.start));
      }
      if (path_timing.stop != nullptr) {
        IgnoreCuda(cudaEventDestroy(path_timing.stop));
      }
    }
    if (timing_stream != nullptr) {
      IgnoreCuda(cudaStreamDestroy(timing_stream));
    }
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->waiting_transfers.erase(handle.id);
    impl_->waiters_idle.notify_all();
    throw;
  }
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->completed_stats[handle.id] = stats;
    impl_->waiting_transfers.erase(handle.id);
    impl_->waiters_idle.notify_all();
  }
}

TransferStats CudaRelayExecutor::GetStats(const TransferHandle& handle) {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  const auto stats_it = impl_->completed_stats.find(handle.id);
  if (stats_it == impl_->completed_stats.end()) {
    throw std::invalid_argument("unknown transfer handle");
  }
  TransferStats stats = stats_it->second;
  impl_->completed_stats.erase(stats_it);
  return stats;
}

}  // namespace turbobus
