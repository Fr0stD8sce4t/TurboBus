from __future__ import annotations

from typing import Any, Iterable, Mapping

from .. import native_plan, native_runtime, tensor_validation


class CudaNativeBackend:
    """Backend facade for the current CUDA native extension."""

    def __init__(
        self,
        *,
        native_runtime_module=native_runtime,
        native_plan_module=native_plan,
        tensor_validation_module=tensor_validation,
    ) -> None:
        self._native_runtime = native_runtime_module
        self._native_plan = native_plan_module
        self._tensor_validation = tensor_validation_module

    def bind_runtime(self, native_module: Any, torch_module: Any) -> None:
        self._native_runtime.bind_native_runtime(native_module)
        self._tensor_validation.bind_torch(torch_module)

    def require_available(self) -> None:
        self._native_runtime.require_extension()

    def require_torch(self) -> None:
        self._tensor_validation.require_torch()

    def set_device(self, device_index: int) -> None:
        device = int(device_index)
        if device < 0:
            raise ValueError("device_index must be non-negative")
        self.require_available()
        setter = getattr(self._native_runtime.native_module(), "set_device", None)
        if not callable(setter):
            raise RuntimeError("native runtime does not support CUDA device selection")
        setter(device)

    def create_runtime(self, options: Any) -> Any:
        self.require_available()
        return self._native_runtime.native_module().Runtime(options.to_native())

    def initialize_runtime(
        self,
        runtime: Any,
        target_device: int,
        relay_gpus: Iterable[int],
    ) -> None:
        initializer = getattr(runtime, "init", None)
        if not callable(initializer):
            raise RuntimeError("native runtime does not support initialization")
        initializer(int(target_device), [int(gpu) for gpu in relay_gpus])

    def profile(
        self,
        runtime: Any,
        profile_bytes: int,
        *,
        force: bool = False,
    ) -> Any:
        profiler = getattr(runtime, "profile", None)
        if not callable(profiler):
            raise RuntimeError("native runtime does not support profiling")
        bytes_to_profile = int(profile_bytes)
        if bytes_to_profile <= 0:
            raise ValueError("profile_bytes must be positive")
        return profiler(bytes_to_profile, bool(force))

    def set_cached_profile(self, runtime: Any, profile: Any) -> None:
        setter = getattr(runtime, "set_cached_profile", None)
        if not callable(setter):
            raise RuntimeError("native runtime does not support cached profiles")
        setter(profile)

    def make_ranges(
        self,
        ranges: Iterable,
        source_bytes: int,
        destination_bytes: int,
    ) -> list:
        return self._native_plan.native_ranges(ranges, source_bytes, destination_bytes)

    def make_transfer_plan(self, plan: Any) -> Any:
        return self._native_plan.native_transfer_plan(plan)

    def transfer_mode_value(self, mode: Any) -> Any:
        return self._native_runtime.runtime_transfer_mode_value(mode)

    def register_host_memory(self, host_ptr: int, bytes_: int) -> None:
        ptr = int(host_ptr)
        size_bytes = int(bytes_)
        if ptr <= 0:
            raise ValueError("host_ptr must be positive")
        if size_bytes <= 0:
            raise ValueError("bytes must be positive")
        self.require_available()
        registrar = getattr(
            self._native_runtime.native_module(),
            "register_host_memory",
            None,
        )
        if not callable(registrar):
            raise RuntimeError("native runtime does not support host memory registration")
        registrar(ptr, size_bytes)

    def unregister_host_memory(self, host_ptr: int) -> None:
        ptr = int(host_ptr)
        if ptr <= 0:
            raise ValueError("host_ptr must be positive")
        self.require_available()
        unregister = getattr(
            self._native_runtime.native_module(),
            "unregister_host_memory",
            None,
        )
        if not callable(unregister):
            raise RuntimeError("native runtime does not support host memory registration")
        unregister(ptr)

    def allocate_device_memory(self, bytes_: int) -> int:
        size_bytes = int(bytes_)
        if size_bytes <= 0:
            raise ValueError("bytes must be positive")
        self.require_available()
        allocator = getattr(
            self._native_runtime.native_module(),
            "allocate_device_memory",
            None,
        )
        if not callable(allocator):
            raise RuntimeError("native runtime does not support device memory allocation")
        ptr = int(allocator(size_bytes))
        if ptr <= 0:
            raise RuntimeError("native runtime returned an invalid device pointer")
        return ptr

    def free_device_memory(self, device_ptr: int) -> None:
        ptr = int(device_ptr)
        if ptr <= 0:
            raise ValueError("device_ptr must be positive")
        self.require_available()
        freer = getattr(
            self._native_runtime.native_module(),
            "free_device_memory",
            None,
        )
        if not callable(freer):
            raise RuntimeError("native runtime does not support device memory allocation")
        freer(ptr)

    def export_device_ipc_mapping(self, device_ptr: int) -> dict[str, int | bytes]:
        ptr = int(device_ptr)
        if ptr <= 0:
            raise ValueError("device_ptr must be positive")
        self.require_available()
        exporter = getattr(
            self._native_runtime.native_module(),
            "export_device_ipc_mapping",
            None,
        )
        if not callable(exporter):
            raise RuntimeError("native runtime does not support CUDA IPC handles")
        exported = exporter(ptr)
        if not isinstance(exported, Mapping):
            raise RuntimeError(
                "native runtime returned an invalid CUDA IPC export mapping"
            )
        handle = _require_cuda_ipc_handle_size(bytes(exported["cuda_ipc_handle"]))
        allocation_base_ptr = int(exported["allocation_base_ptr"])
        allocation_size_bytes = int(exported["allocation_size_bytes"])
        device_offset_bytes = int(exported["device_offset_bytes"])
        if allocation_base_ptr <= 0:
            raise ValueError("allocation_base_ptr must be positive")
        if allocation_size_bytes <= 0:
            raise ValueError("allocation_size_bytes must be positive")
        if device_offset_bytes < 0:
            raise ValueError("device_offset_bytes must be non-negative")
        return {
            "cuda_ipc_handle": handle,
            "allocation_base_ptr": allocation_base_ptr,
            "allocation_size_bytes": allocation_size_bytes,
            "device_offset_bytes": device_offset_bytes,
        }

    def open_device_ipc_handle(self, cuda_ipc_handle: bytes | bytearray | str) -> int:
        handle = _coerce_cuda_ipc_handle(cuda_ipc_handle)
        self.require_available()
        opener = getattr(
            self._native_runtime.native_module(),
            "open_device_ipc_handle",
            None,
        )
        if not callable(opener):
            raise RuntimeError("native runtime does not support CUDA IPC handles")
        ptr = int(opener(handle))
        if ptr <= 0:
            raise RuntimeError("native runtime returned an invalid CUDA IPC pointer")
        return ptr

    def close_device_ipc_handle(self, device_ptr: int) -> None:
        ptr = int(device_ptr)
        if ptr <= 0:
            raise ValueError("device_ptr must be positive")
        self.require_available()
        closer = getattr(
            self._native_runtime.native_module(),
            "close_device_ipc_handle",
            None,
        )
        if not callable(closer):
            raise RuntimeError("native runtime does not support CUDA IPC handles")
        closer(ptr)

    def verify_transfer(
        self,
        *,
        target_device: int,
        direction: str,
        host_ptr: int,
        host_bytes: int,
        device_ptr: int,
        device_bytes: int,
        ranges: Iterable,
    ) -> dict[str, object]:
        normalized_direction = str(direction).lower()
        if normalized_direction not in {"h2d", "d2h"}:
            raise ValueError("direction must be h2d or d2h")
        host_size = int(host_bytes)
        device_size = int(device_bytes)
        if host_size <= 0:
            raise ValueError("host_bytes must be positive")
        if device_size <= 0:
            raise ValueError("device_bytes must be positive")
        native_ranges = self.make_ranges(
            ranges,
            source_bytes=host_size if normalized_direction == "h2d" else device_size,
            destination_bytes=device_size if normalized_direction == "h2d" else host_size,
        )
        self.require_available()
        verifier = getattr(self._native_runtime.native_module(), "verify_transfer", None)
        if not callable(verifier):
            raise RuntimeError("native runtime does not support transfer verification")
        return dict(
            verifier(
                int(target_device),
                normalized_direction,
                int(host_ptr),
                host_size,
                int(device_ptr),
                device_size,
                native_ranges,
            )
        )

    def fetch_plan_to_gpu(
        self,
        runtime: Any,
        host_ptr: int,
        host_bytes: int,
        target_ptr: int,
        target_bytes: int,
        plan: Any,
    ) -> Any:
        submitter = getattr(runtime, "fetch_plan_to_gpu", None)
        if not callable(submitter):
            raise RuntimeError("native runtime does not support exact transfer plans")
        return submitter(host_ptr, host_bytes, target_ptr, target_bytes, plan)

    def offload_plan_to_cpu(
        self,
        runtime: Any,
        target_ptr: int,
        target_bytes: int,
        host_ptr: int,
        host_bytes: int,
        plan: Any,
    ) -> Any:
        submitter = getattr(runtime, "offload_plan_to_cpu", None)
        if not callable(submitter):
            raise RuntimeError("native runtime does not support exact transfer plans")
        return submitter(target_ptr, target_bytes, host_ptr, host_bytes, plan)

    def wait(self, runtime: Any, handle: Any) -> None:
        waiter = getattr(runtime, "wait", None)
        if not callable(waiter):
            raise RuntimeError("native runtime does not support transfer waiting")
        waiter(handle)

    def stats(self, runtime: Any, handle: Any) -> Any:
        statter = getattr(runtime, "stats", None)
        if not callable(statter):
            raise RuntimeError("native runtime does not support transfer stats")
        return statter(handle)


default_cuda_backend = CudaNativeBackend()


def _coerce_cuda_ipc_handle(handle: bytes | bytearray | str) -> bytes:
    if isinstance(handle, str):
        try:
            raw_handle = bytes.fromhex(handle)
        except ValueError as exc:
            raise ValueError("cuda_ipc_handle string must be hex encoded") from exc
        return _require_cuda_ipc_handle_size(raw_handle)
    return _require_cuda_ipc_handle_size(bytes(handle))


def _require_cuda_ipc_handle_size(handle: bytes) -> bytes:
    if len(handle) != 64:
        raise ValueError("cuda_ipc_handle must be 64 bytes")
    return handle


__all__ = ["CudaNativeBackend", "default_cuda_backend"]
