#include "turbobus/runtime.h"

#include <algorithm>
#include <mutex>
#include <stdexcept>

namespace turbobus {

namespace {

std::vector<int> CanonicalRelayDevices(const std::vector<int>& relay_devices) {
  std::vector<int> normalized = relay_devices;
  std::sort(normalized.begin(), normalized.end());
  normalized.erase(std::unique(normalized.begin(), normalized.end()),
                   normalized.end());
  return normalized;
}

}  // namespace

TurboBusRuntime::TurboBusRuntime(RuntimeOptions options) : options_(options) {}

TurboBusRuntime::~TurboBusRuntime() = default;

void TurboBusRuntime::Init(int target_device, const std::vector<int>& relay_devices) {
  target_device_ = target_device;
  requested_relays_ = CanonicalRelayDevices(relay_devices);
  profile_ = {};
  planner_profile_ = {};
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    last_plan_ = {};
  }
  has_profile_ = false;

  topology_ = topology_manager_.Discover(target_device_, requested_relays_,
                                         options_.enable_peer_access);

  enabled_relays_.clear();
  enabled_relays_.reserve(topology_.relay_candidates.size());
  for (const auto& relay : topology_.relay_candidates) {
    enabled_relays_.push_back(relay.device_id);
  }

  executor_.Init(target_device_, enabled_relays_, options_);
  initialized_ = true;
}

ProfileResult TurboBusRuntime::Profile(std::size_t bytes, bool force) {
  if (!initialized_) {
    throw std::runtime_error("runtime is not initialized");
  }
  if (force || !has_profile_ || !options_.profile_cache_enabled) {
    profile_ = profiler_.Profile(target_device_, enabled_relays_, bytes);
    planner_profile_ = profile_;
    has_profile_ = true;
  }
  return profile_;
}

void TurboBusRuntime::SetCachedProfile(const ProfileResult& profile) {
  if (!initialized_) {
    throw std::runtime_error("runtime is not initialized");
  }
  profile_ = profile;
  planner_profile_ = profile_;
  has_profile_ = true;
}

TransferHandle TurboBusRuntime::FetchPlanToGpu(void* host_ptr, std::size_t host_bytes,
                                               void* target_gpu_ptr,
                                               std::size_t target_bytes,
                                               const TransferPlan& plan) {
  if (!initialized_) {
    throw std::runtime_error("runtime is not initialized");
  }
  BufferView source;
  source.ptr = host_ptr;
  source.bytes = host_bytes;
  source.device = kHostDevice;
  source.kind = MemoryKind::HostPinned;

  BufferView destination;
  destination.ptr = target_gpu_ptr;
  destination.bytes = target_bytes;
  destination.device = target_device_;
  destination.kind = MemoryKind::Device;

  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    last_plan_ = plan;
  }
  return executor_.Submit(source, destination, plan);
}

TransferHandle TurboBusRuntime::OffloadPlanToCpu(void* target_gpu_ptr,
                                                 std::size_t target_bytes,
                                                 void* host_ptr,
                                                 std::size_t host_bytes,
                                                 const TransferPlan& plan) {
  if (!initialized_) {
    throw std::runtime_error("runtime is not initialized");
  }
  BufferView source;
  source.ptr = target_gpu_ptr;
  source.bytes = target_bytes;
  source.device = target_device_;
  source.kind = MemoryKind::Device;

  BufferView destination;
  destination.ptr = host_ptr;
  destination.bytes = host_bytes;
  destination.device = kHostDevice;
  destination.kind = MemoryKind::HostPinned;

  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    last_plan_ = plan;
  }
  return executor_.SubmitD2H(source, destination, plan);
}

DummyComputeStats TurboBusRuntime::RunDummyCompute(void* device_ptr, std::size_t elements,
                                                   int iterations) {
  if (!initialized_) {
    throw std::runtime_error("runtime is not initialized");
  }
  return turbobus::RunDummyCompute(target_device_, device_ptr, elements, iterations);
}

void TurboBusRuntime::Wait(const TransferHandle& handle) {
  executor_.Wait(handle);
}

TransferStats TurboBusRuntime::GetStats(const TransferHandle& handle) {
  return executor_.GetStats(handle);
}

const ProfileResult& TurboBusRuntime::CachedProfile() const {
  return profile_;
}

const ProfileResult& TurboBusRuntime::PlannerProfile() const {
  return planner_profile_;
}

TransferPlan TurboBusRuntime::LastPlan() const {
  std::lock_guard<std::mutex> lock(state_mutex_);
  return last_plan_;
}

}  // namespace turbobus
