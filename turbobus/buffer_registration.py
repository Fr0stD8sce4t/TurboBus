from __future__ import annotations

from collections.abc import Iterable

from .client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer
from .schema import BufferRegistration
from .transfer import TransferRange
from .transfer_execution import require_ok


ExecutableBuffer = SharedPinnedCpuBuffer | CudaIpcDeviceBuffer


def register_buffer(daemon_client, registration: BufferRegistration) -> None:
    response = daemon_client.register_buffer(
        buffer_id=registration.buffer_id,
        job_id=registration.job_id,
        kind=registration.kind,
        size_bytes=registration.size_bytes,
        device_index=registration.device_index,
        address=registration.address,
        pinned=registration.pinned,
        handle_type=registration.handle_type,
        metadata=registration.metadata,
    )
    require_ok(response, "daemon buffer registration failed")


def register_executable_buffer(
    daemon_client,
    buffer: ExecutableBuffer,
) -> BufferRegistration:
    registration = buffer.buffer_registration()
    register_buffer(daemon_client, registration)
    return registration


def register_executable_buffers(
    daemon_client,
    buffers: Iterable[ExecutableBuffer],
) -> tuple[BufferRegistration, ...]:
    return tuple(register_executable_buffer(daemon_client, buffer) for buffer in buffers)


def ranges_or_full_buffer(
    ranges: Iterable[TransferRange | tuple[int, int, int] | dict] | None,
    source_bytes: int,
    target_bytes: int,
) -> tuple[TransferRange | tuple[int, int, int] | dict, ...]:
    if ranges is not None:
        return tuple(ranges)
    bytes_to_copy = min(int(source_bytes), int(target_bytes))
    if bytes_to_copy <= 0:
        raise ValueError("transfer buffers must be non-empty")
    return (TransferRange(src_offset=0, dst_offset=0, bytes=bytes_to_copy),)


__all__ = [
    "ExecutableBuffer",
    "ranges_or_full_buffer",
    "register_buffer",
    "register_executable_buffer",
    "register_executable_buffers",
]
