from __future__ import annotations

import time

from .validation import validate_intent_ranges_fit_buffers
from ..buffer_registration import ExecutableBuffer
from ..client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer
from ..schema import TransferIntent


def buffer_registration_fingerprint(buffer: ExecutableBuffer) -> tuple[object, ...]:
    registration = buffer.buffer_registration()
    metadata = tuple(
        sorted((str(key), str(value)) for key, value in registration.metadata.items())
    )
    return (
        registration.buffer_id,
        registration.job_id,
        registration.kind,
        registration.size_bytes,
        registration.device_index,
        registration.address,
        registration.pinned,
        registration.handle_type,
        metadata,
    )


def runtime_session_buffer_metadata(
    buffer: ExecutableBuffer,
    *,
    session_id: str,
    runtime_owned: bool,
) -> dict[str, object]:
    registration = buffer.buffer_registration()
    metadata = dict(registration.metadata)
    metadata["runtime_session_id"] = str(session_id)
    metadata["runtime_owned"] = bool(runtime_owned)
    metadata["runtime_buffer_kind"] = _runtime_buffer_kind(buffer)
    metadata["runtime_lifecycle_pool"] = True
    return metadata


def runtime_buffer_lifecycle_registration(
    buffer: ExecutableBuffer,
    *,
    session_id: str,
    runtime_owned: bool,
    registered_at: float,
    registration_count: int,
) -> dict[str, object]:
    registration = buffer.buffer_registration()
    return {
        "buffer_id": registration.buffer_id,
        "job_id": registration.job_id,
        "session_id": str(session_id),
        "runtime_buffer_kind": _runtime_buffer_kind(buffer),
        "kind": str(registration.kind),
        "size_bytes": int(registration.size_bytes),
        "device_index": registration.device_index,
        "pinned": bool(registration.pinned),
        "handle_type": str(registration.handle_type),
        "runtime_owned": bool(runtime_owned),
        "registered_at": float(registered_at),
        "registration_count": int(registration_count),
        "state": "registered",
        "metadata": runtime_session_buffer_metadata(
            buffer,
            session_id=session_id,
            runtime_owned=runtime_owned,
        ),
    }


def runtime_buffer_lifecycle_intent_use(
    intent: TransferIntent,
    *,
    buffer_id: str,
    role: str,
) -> dict[str, object]:
    return {
        "intent_id": str(intent.intent_id),
        "job_id": str(intent.job_id),
        "session_id": str(intent.session_id),
        "buffer_id": str(buffer_id),
        "role": str(role),
        "direction": str(intent.direction),
        "bytes_total": int(intent.total_bytes),
        "range_count": len(tuple(intent.ranges)),
        "submitted_at": float(time.time()),
        "state": "active",
    }


def validate_runtime_buffer_backing(buffer: ExecutableBuffer) -> None:
    if isinstance(buffer, SharedPinnedCpuBuffer):
        if buffer.closed:
            raise ValueError("shared CPU buffer is closed")
        _ = buffer.address
        return
    if isinstance(buffer, CudaIpcDeviceBuffer):
        device_ptr = buffer.device_ptr
        if device_ptr is None or int(device_ptr) <= 0:
            raise ValueError("CUDA buffer must have a live device_ptr")
        return
    raise TypeError("buffer must be a SharedPinnedCpuBuffer or CudaIpcDeviceBuffer")


def validate_intent_uses_runtime_buffers(
    intent: TransferIntent,
    *,
    source: ExecutableBuffer,
    target: ExecutableBuffer,
) -> None:
    direction = str(intent.direction).lower()
    if direction == "h2d":
        if not isinstance(source, SharedPinnedCpuBuffer):
            raise ValueError("h2d intent source must be a registered CPU buffer")
        if not isinstance(target, CudaIpcDeviceBuffer):
            raise ValueError("h2d intent destination must be a registered CUDA buffer")
    elif direction == "d2h":
        if not isinstance(source, CudaIpcDeviceBuffer):
            raise ValueError("d2h intent source must be a registered CUDA buffer")
        if not isinstance(target, SharedPinnedCpuBuffer):
            raise ValueError("d2h intent destination must be a registered CPU buffer")
    else:
        raise ValueError("intent direction must be h2d or d2h")
    validate_intent_ranges_fit_buffers(
        intent,
        source_bytes=source.size_bytes,
        target_bytes=target.size_bytes,
    )


def _runtime_buffer_kind(buffer: ExecutableBuffer) -> str:
    if isinstance(buffer, SharedPinnedCpuBuffer):
        return "shared_pinned_cpu"
    if isinstance(buffer, CudaIpcDeviceBuffer):
        return "cuda_ipc_device"
    return "executable_buffer"


__all__ = [
    "buffer_registration_fingerprint",
    "runtime_buffer_lifecycle_intent_use",
    "runtime_buffer_lifecycle_registration",
    "runtime_session_buffer_metadata",
    "validate_runtime_buffer_backing",
    "validate_intent_uses_runtime_buffers",
]
