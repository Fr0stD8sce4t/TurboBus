from __future__ import annotations


class RuntimeBenchmarkState:
    def __init__(self, *, session, buffers, core) -> None:
        self.session = session
        self.buffers = buffers
        self.core = core


class RuntimeBuffers:
    def __init__(self, *, cpu_buffer, gpu_buffer, target_allocation=None) -> None:
        self.cpu_buffer = cpu_buffer
        self.gpu_buffer = gpu_buffer
        self.target_allocation = target_allocation

    def release(self) -> None:
        target_releaser = getattr(self.target_allocation, "release", None)
        if callable(target_releaser):
            target_releaser()
        releaser = getattr(self.cpu_buffer, "release", None)
        if callable(releaser):
            releaser()


class NativeCudaDeviceAllocation:
    def __init__(self, *, ptr: int, size_bytes: int, backend) -> None:
        self.ptr = int(ptr)
        self.size_bytes = int(size_bytes)
        self._backend = backend
        self._released = False

    def release(self) -> None:
        if self._released:
            return
        self._backend.free_device_memory(self.ptr)
        self._released = True


def make_state_offload_core(
    *,
    session,
    buffers,
    args,
    benchmark_name: str,
    spec,
    workload_kind,
    active_bucket_count,
) :
    core = make_state_offload_core_from_spec(
        session=session,
        buffers=buffers,
        args=args,
        benchmark_name=benchmark_name,
        spec=spec,
        workload_kind=workload_kind,
        metadata=state_offload_metadata(
            args,
            benchmark_name=benchmark_name,
            active_bucket_count=active_bucket_count,
        ),
    )
    return core


def make_state_offload_core_from_spec(
    *,
    session,
    buffers,
    args,
    benchmark_name: str,
    spec,
    workload_kind,
    metadata: dict[str, object],
):
    from turbobus.state_offload import PackedStateRegistry

    core = session.make_state_offload(
        spec,
        buffers.cpu_buffer,
        buffers.gpu_buffer,
        workload_kind=workload_kind,
        metadata=metadata,
        intent_prefix=f"{args.intent_prefix}-{args.run_id}",
        wait_timeout_seconds=args.wait_timeout_seconds,
    )
    core.register_registry(
        PackedStateRegistry(
            prefix="bucket-",
            cpu_tensor=buffers.cpu_buffer,
            gpu_tensor=buffers.gpu_buffer,
            bucket_bytes=int(args.bucket_bytes),
            bucket_count=int(args.bucket_count),
        )
    )
    return core


def state_offload_metadata(
    args,
    *,
    benchmark_name: str,
    active_bucket_count,
) -> dict[str, object]:
    return {
        "benchmark": benchmark_name,
        "policy": args.policy,
        "storage_layout": args.storage_layout,
        "bucket_count": int(args.bucket_count),
        "active_buckets": active_bucket_count(args),
        "bucket_bytes": int(args.bucket_bytes),
        "chunk_bytes": int(args.chunk_bytes),
    }


def allocate_torch_runtime_buffers(
    *,
    args,
    byte_count: int,
    benchmark_name: str,
    require_torch,
) -> RuntimeBuffers:
    torch = require_torch()
    run_id = str(args.run_id).replace("/", "-")
    cpu_buffer_id = args.cpu_buffer_id or f"{benchmark_name}-cpu-{run_id}"
    gpu_buffer_id = args.gpu_buffer_id or f"{benchmark_name}-gpu-{run_id}"

    from turbobus.backends.cuda import default_cuda_backend
    from turbobus.client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer

    cpu_buffer = None
    target_allocation = None
    try:
        cpu_buffer = SharedPinnedCpuBuffer.allocate(
            buffer_id=cpu_buffer_id,
            job_id=args.job_id,
            size_bytes=byte_count,
            name_prefix=f"turbobus-{benchmark_name}",
        )
        source = torch.empty(byte_count, dtype=torch.uint8, pin_memory=True)
        source.random_(0, 256)
        cpu_buffer.write(source.numpy().tobytes())

        torch.cuda.set_device(int(args.target_gpu))
        default_cuda_backend.set_device(int(args.target_gpu))
        target_allocation = NativeCudaDeviceAllocation(
            ptr=default_cuda_backend.allocate_device_memory(byte_count),
            size_bytes=byte_count,
            backend=default_cuda_backend,
        )
        gpu_buffer = CudaIpcDeviceBuffer.from_device_pointer(
            buffer_id=gpu_buffer_id,
            job_id=args.job_id,
            device_index=int(args.target_gpu),
            size_bytes=byte_count,
            device_ptr=target_allocation.ptr,
            backend=default_cuda_backend,
        )
        return RuntimeBuffers(
            cpu_buffer=cpu_buffer,
            gpu_buffer=gpu_buffer,
            target_allocation=target_allocation,
        )
    except Exception:
        if target_allocation is not None:
            target_allocation.release()
        if cpu_buffer is not None:
            cpu_buffer.release()
        raise
