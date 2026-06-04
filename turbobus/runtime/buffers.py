from __future__ import annotations

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


__all__ = [
    "buffer_registration_fingerprint",
    "validate_intent_uses_runtime_buffers",
]
