from __future__ import annotations

from dataclasses import dataclass, field

from ..backends.cuda import default_cuda_backend
from ..client import SharedPinnedCpuBuffer
from ..schema import BufferRegistration, WorkerBufferHandle, WorkerDataPlaneRequest
from . import validation as worker_validation
from .models import WorkerTransferRequest


class WorkerDataPlaneResourceError(RuntimeError):
    pass


@dataclass(frozen=True)
class WorkerDataPlaneResources:
    request: WorkerDataPlaneRequest
    cpu_buffer: SharedPinnedCpuBuffer
    device_ptr: int
    device_bytes: int
    ticket_id: str
    plan_generation: int
    cuda_host_registered: bool = False
    cuda_backend: object | None = field(default=None, repr=False, compare=False)
    device_index: int | None = None
    device_ipc_base_ptr: int | None = None
    open_evidence: dict[str, object] | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    _closed: bool = field(default=False, init=False, repr=False, compare=False)
    _device_ipc_closed: bool = field(
        default=False,
        init=False,
        repr=False,
        compare=False,
    )
    _close_evidence: dict[str, object] | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )

    @property
    def host_ptr(self) -> int:
        self._require_open()
        return self.cpu_buffer.address

    @property
    def host_bytes(self) -> int:
        self._require_open()
        return self.cpu_buffer.size_bytes

    @property
    def source_cpu_buffer(self) -> SharedPinnedCpuBuffer:
        return self.cpu_buffer

    @property
    def source_host_ptr(self) -> int:
        return self.host_ptr

    @property
    def source_bytes(self) -> int:
        return self.host_bytes

    @property
    def target_device_ptr(self) -> int:
        self._require_open()
        return self.device_ptr

    @property
    def target_device_bytes(self) -> int:
        self._require_open()
        return self.device_bytes

    @property
    def cpu_buffer_role(self) -> str:
        return "source" if self.request.direction == "h2d" else "destination"

    @property
    def device_buffer_role(self) -> str:
        return "destination" if self.request.direction == "h2d" else "source"

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        if self._closed:
            return
        try:
            self.cpu_buffer.close()
        finally:
            try:
                self.close_device_ipc_handle()
            finally:
                object.__setattr__(self, "_closed", True)
                object.__setattr__(
                    self,
                    "_close_evidence",
                    self.close_evidence(),
                )

    def close_device_ipc_handle(self) -> None:
        if self._device_ipc_closed:
            return
        try:
            backend = self.cuda_backend
            if backend is not None:
                _set_cuda_device_index(backend, self.device_index)
                device_ipc_base_ptr = self.device_ipc_base_ptr or self.device_ptr
                backend.close_device_ipc_handle(device_ipc_base_ptr)
        finally:
            object.__setattr__(self, "_device_ipc_closed", True)

    def as_dict(self) -> dict[str, object]:
        cpu_handle = (
            self.request.src_handle
            if self.request.direction == "h2d"
            else self.request.dst_handle
        )
        device_handle = (
            self.request.dst_handle
            if self.request.direction == "h2d"
            else self.request.src_handle
        )
        return {
            "transfer_id": self.request.transfer_id,
            "lease_id": self.request.lease_id,
            "ticket_id": self.ticket_id,
            "plan_generation": self.plan_generation,
            "session_id": self.request.session_id,
            "job_id": self.request.job_id,
            "direction": self.request.direction,
            "src_buffer_id": self.request.src_handle.buffer_id,
            "src_handle_type": self.request.src_handle.handle_type,
            "dst_buffer_id": self.request.dst_handle.buffer_id,
            "dst_handle_type": self.request.dst_handle.handle_type,
            "cpu_buffer_id": cpu_handle.buffer_id,
            "cpu_handle_type": cpu_handle.handle_type,
            "host_ptr": self.host_ptr,
            "host_bytes": self.host_bytes,
            "cpu_buffer_role": self.cpu_buffer_role,
            "device_buffer_id": device_handle.buffer_id,
            "device_handle_type": device_handle.handle_type,
            "device_ptr": self.device_ptr,
            "device_bytes": self.device_bytes,
            "device_index": self.device_index,
            "device_buffer_role": self.device_buffer_role,
            "cuda_host_registered": self.cuda_host_registered,
            "device_ipc_closed": self._device_ipc_closed,
            "closed": self.closed,
            "open_evidence": (
                None if self.open_evidence is None else dict(self.open_evidence)
            ),
            "cuda_ipc_lifecycle": self.cuda_ipc_lifecycle(),
        }

    def close_evidence(self) -> dict[str, object]:
        if self._close_evidence is not None:
            return dict(self._close_evidence)
        cpu_handle = (
            self.request.src_handle
            if self.request.direction == "h2d"
            else self.request.dst_handle
        )
        device_handle = (
            self.request.dst_handle
            if self.request.direction == "h2d"
            else self.request.src_handle
        )
        return {
            "transfer_id": self.request.transfer_id,
            "lease_id": self.request.lease_id,
            "ticket_id": self.ticket_id,
            "plan_generation": self.plan_generation,
            "session_id": self.request.session_id,
            "job_id": self.request.job_id,
            "direction": self.request.direction,
            "cpu_buffer_id": cpu_handle.buffer_id,
            "cpu_handle_type": cpu_handle.handle_type,
            "cpu_buffer_role": self.cpu_buffer_role,
            "cpu_buffer_closed": self.cpu_buffer.closed,
            "cpu_cuda_registered": self.cpu_buffer.cuda_registered,
            "device_buffer_id": device_handle.buffer_id,
            "device_handle_type": device_handle.handle_type,
            "device_index": self.device_index,
            "device_buffer_role": self.device_buffer_role,
            "device_ipc_closed": self._device_ipc_closed,
            "resources_closed": self.closed,
            "open_evidence": (
                None if self.open_evidence is None else dict(self.open_evidence)
            ),
            "cuda_ipc_lifecycle": self.cuda_ipc_lifecycle(),
        }

    def cuda_ipc_lifecycle(self) -> dict[str, object]:
        cpu_handle = (
            self.request.src_handle
            if self.request.direction == "h2d"
            else self.request.dst_handle
        )
        device_handle = (
            self.request.dst_handle
            if self.request.direction == "h2d"
            else self.request.src_handle
        )
        open_evidence = (
            {} if self.open_evidence is None else dict(self.open_evidence)
        )
        span_validation = open_evidence.get("cuda_ipc_span_validation")
        allocation_size = device_handle.metadata.get("allocation_size_bytes")
        offset_bytes = device_handle.metadata.get("device_offset_bytes", 0)
        lifecycle = {
            "source": "worker_cuda_ipc_lifecycle",
            "transfer_id": self.request.transfer_id,
            "lease_id": self.request.lease_id,
            "ticket_id": self.ticket_id,
            "plan_generation": self.plan_generation,
            "session_id": self.request.session_id,
            "job_id": self.request.job_id,
            "direction": self.request.direction,
            "src_buffer_id": self.request.src_handle.buffer_id,
            "dst_buffer_id": self.request.dst_handle.buffer_id,
            "cpu_buffer_id": cpu_handle.buffer_id,
            "cpu_handle_type": cpu_handle.handle_type,
            "cpu_buffer_role": self.cpu_buffer_role,
            "cpu_buffer_opened": bool(open_evidence.get("cpu_buffer_opened", False)),
            "cpu_buffer_closed": bool(self.cpu_buffer.closed),
            "cuda_host_registration_requested": bool(
                open_evidence.get("cuda_host_registration_requested", False)
            ),
            "cuda_host_registered": bool(self.cpu_buffer.cuda_registered),
            "device_buffer_id": device_handle.buffer_id,
            "device_handle_type": device_handle.handle_type,
            "device_buffer_role": self.device_buffer_role,
            "device_index": self.device_index,
            "device_bytes": self.device_bytes,
            "device_ipc_opened": bool(open_evidence.get("device_ipc_opened", False)),
            "device_ipc_closed": bool(self._device_ipc_closed),
            "resources_closed": bool(self.closed),
            "device_offset_bytes": int(offset_bytes),
            "ticket_owner_binding": {
                "job_id": self.request.job_id,
                "session_id": self.request.session_id,
                "ticket_id": self.ticket_id,
                "lease_id": self.request.lease_id,
                "plan_generation": self.plan_generation,
            },
        }
        if allocation_size is not None:
            lifecycle["allocation_size_bytes"] = int(allocation_size)
        if self.device_ipc_base_ptr is not None:
            lifecycle["device_ipc_base_ptr"] = int(self.device_ipc_base_ptr)
        if self.device_ptr is not None:
            lifecycle["device_ptr"] = int(self.device_ptr)
        if isinstance(span_validation, dict):
            lifecycle["cuda_ipc_span_validation"] = dict(span_validation)
            lifecycle["span_validated"] = bool(span_validation.get("validated", False))
        else:
            lifecycle["span_validated"] = False
        return lifecycle

    def _require_open(self) -> None:
        if self._closed:
            raise WorkerDataPlaneResourceError("worker data-plane resources are closed")


class WorkerDataPlaneResourceBinding:
    def __init__(
        self,
        worker_request: WorkerTransferRequest,
        *,
        backend=default_cuda_backend,
        register_cuda_host: bool = True,
    ) -> None:
        if not isinstance(worker_request, WorkerTransferRequest):
            raise TypeError("worker_request must be a WorkerTransferRequest")
        worker_validation.validate_daemon_issued_ticket(worker_request.ticket)
        worker_validation.validate_ticket_matches_worker_request(
            worker_request.ticket,
            worker_request.authorization,
            worker_request.data_plane,
        )
        self.worker_request = worker_request
        self.request = worker_request.data_plane
        self.backend = backend
        self.register_cuda_host = bool(register_cuda_host)
        self._resources: WorkerDataPlaneResources | None = None
        self._device_ptr: int | None = None
        self._device_ipc_base_ptr: int | None = None
        self._device_index: int | None = None
        self._failure_evidence: dict[str, object] | None = None

    @property
    def failure_evidence(self) -> dict[str, object] | None:
        if self._failure_evidence is None:
            return None
        return dict(self._failure_evidence)

    def __enter__(self) -> WorkerDataPlaneResources:
        if (
            self._resources is not None
            or self._device_ptr is not None
            or self._device_ipc_base_ptr is not None
        ):
            raise WorkerDataPlaneResourceError("worker data-plane resources already bound")
        cpu_buffer: SharedPinnedCpuBuffer | None = None
        cpu_handle = _cpu_handle_for_request(self.request)
        device_handle = _device_handle_for_request(self.request)
        self._failure_evidence = None
        open_evidence = _resource_binding_evidence(
            request=self.request,
            ticket_id=self.worker_request.ticket.ticket_id,
            plan_generation=int(self.worker_request.ticket.metadata["plan_generation"]),
            cpu_handle=cpu_handle,
            device_handle=device_handle,
            register_cuda_host=self.register_cuda_host,
        )
        try:
            self._device_index = device_handle.device_index
            _set_cuda_device_for_handle(self.backend, device_handle)
            cpu_buffer = SharedPinnedCpuBuffer.open_from_registration(
                _registration_from_worker_handle(cpu_handle)
            )
            open_evidence["cpu_buffer_opened"] = True
            open_evidence["cpu_buffer_closed"] = bool(cpu_buffer.closed)
            if self.register_cuda_host:
                cpu_buffer.register_for_cuda(self.backend)
            open_evidence["cuda_host_registered"] = bool(cpu_buffer.cuda_registered)
            self._device_ipc_base_ptr, self._device_ptr = _open_cuda_ipc_device_handle(
                self.backend,
                device_handle,
            )
            open_evidence["device_ipc_opened"] = True
            open_evidence["device_ipc_base_ptr"] = int(self._device_ipc_base_ptr)
            open_evidence["device_ptr"] = int(self._device_ptr)
            span_evidence = _validate_cuda_ipc_device_span(
                self.request,
                device_handle=device_handle,
                device_ipc_base_ptr=self._device_ipc_base_ptr,
                device_ptr=self._device_ptr,
            )
            open_evidence["cuda_ipc_span_validation"] = span_evidence
            self._resources = WorkerDataPlaneResources(
                request=self.request,
                cpu_buffer=cpu_buffer,
                device_ptr=self._device_ptr,
                device_bytes=device_handle.size_bytes,
                ticket_id=self.worker_request.ticket.ticket_id,
                plan_generation=int(
                    self.worker_request.ticket.metadata["plan_generation"]
                ),
                cuda_host_registered=self.register_cuda_host,
                cuda_backend=self.backend,
                device_index=self._device_index,
                device_ipc_base_ptr=self._device_ipc_base_ptr,
                open_evidence=open_evidence,
            )
            return self._resources
        except Exception as exc:
            failure_evidence = dict(open_evidence)
            failure_evidence["failure_source"] = "worker_resource_binding"
            failure_evidence["error"] = str(exc) or exc.__class__.__name__
            failure_cleanup: dict[str, object] = {}
            try:
                if self._device_ipc_base_ptr is not None:
                    try:
                        _set_cuda_device_index(self.backend, self._device_index)
                        self.backend.close_device_ipc_handle(
                            self._device_ipc_base_ptr
                        )
                        failure_cleanup["device_ipc_closed_after_failure"] = True
                    except Exception as close_exc:
                        failure_cleanup["device_ipc_close_error"] = (
                            str(close_exc) or close_exc.__class__.__name__
                        )
                    finally:
                        self._device_ptr = None
                        self._device_ipc_base_ptr = None
                if cpu_buffer is not None:
                    try:
                        _set_cuda_device_index(self.backend, self._device_index)
                        cpu_buffer.close()
                        failure_cleanup["cpu_buffer_closed_after_failure"] = bool(
                            cpu_buffer.closed
                        )
                        failure_cleanup["cpu_cuda_registered_after_failure"] = bool(
                            cpu_buffer.cuda_registered
                        )
                    except Exception as close_exc:
                        failure_cleanup["cpu_buffer_close_error"] = (
                            str(close_exc) or close_exc.__class__.__name__
                        )
            finally:
                self._resources = None
                self._device_index = None
                self._device_ipc_base_ptr = None
            if failure_cleanup:
                failure_evidence["failure_cleanup"] = failure_cleanup
            failure_evidence["cuda_ipc_lifecycle"] = _cuda_ipc_failure_lifecycle(
                request=self.request,
                ticket_id=self.worker_request.ticket.ticket_id,
                plan_generation=int(
                    self.worker_request.ticket.metadata["plan_generation"]
                ),
                device_handle=device_handle,
                cpu_handle=cpu_handle,
                open_evidence=open_evidence,
                failure_cleanup=failure_cleanup,
            )
            self._failure_evidence = failure_evidence
            raise WorkerDataPlaneResourceError(
                f"failed to bind worker data-plane resources: {exc}"
            ) from exc

    def __exit__(self, exc_type, exc, traceback) -> None:
        try:
            resources = self._resources
            self._resources = None
            if resources is not None:
                resources.close()
                self._device_ptr = None
                self._device_ipc_base_ptr = None
        finally:
            if self._device_ipc_base_ptr is not None:
                _set_cuda_device_index(self.backend, self._device_index)
                self.backend.close_device_ipc_handle(self._device_ipc_base_ptr)
                self._device_ptr = None
                self._device_ipc_base_ptr = None
            self._device_index = None


class WorkerDataPlaneResourceBinder:
    def __init__(
        self,
        *,
        backend=default_cuda_backend,
        register_cuda_host: bool = True,
    ) -> None:
        self.backend = backend
        self.register_cuda_host = bool(register_cuda_host)

    def bind(
        self,
        worker_request: WorkerTransferRequest,
    ) -> WorkerDataPlaneResourceBinding:
        return WorkerDataPlaneResourceBinding(
            worker_request,
            backend=self.backend,
            register_cuda_host=self.register_cuda_host,
        )


def _registration_from_worker_handle(handle: WorkerBufferHandle) -> BufferRegistration:
    if not isinstance(handle, WorkerBufferHandle):
        raise TypeError("handle must be a WorkerBufferHandle")
    if handle.handle_type != "shared_pinned_cpu":
        raise WorkerDataPlaneResourceError(
            "worker shared CPU binding requires a shared_pinned_cpu source handle"
        )
    return BufferRegistration(
        buffer_id=handle.buffer_id,
        job_id=handle.job_id,
        kind=handle.kind,
        size_bytes=handle.size_bytes,
        device_index=handle.device_index,
        address=handle.address,
        pinned=handle.pinned,
        handle_type=handle.handle_type,
        metadata=handle.metadata,
    )


def _cpu_handle_for_request(request: WorkerDataPlaneRequest) -> WorkerBufferHandle:
    return request.src_handle if request.direction == "h2d" else request.dst_handle


def _device_handle_for_request(request: WorkerDataPlaneRequest) -> WorkerBufferHandle:
    return request.dst_handle if request.direction == "h2d" else request.src_handle


def _open_cuda_ipc_device_handle(
    backend,
    handle: WorkerBufferHandle,
) -> tuple[int, int]:
    if not isinstance(handle, WorkerBufferHandle):
        raise TypeError("handle must be a WorkerBufferHandle")
    if handle.handle_type != "cuda_ipc_device":
        raise WorkerDataPlaneResourceError(
            "worker device binding requires a cuda_ipc_device handle"
        )
    base_ptr = backend.open_device_ipc_handle(handle.metadata["cuda_ipc_handle"])
    offset_bytes = int(handle.metadata.get("device_offset_bytes", 0))
    if offset_bytes < 0:
        raise WorkerDataPlaneResourceError(
            "worker device binding requires non-negative device_offset_bytes"
        )
    return int(base_ptr), int(base_ptr) + offset_bytes


def _validate_cuda_ipc_device_span(
    request: WorkerDataPlaneRequest,
    *,
    device_handle: WorkerBufferHandle,
    device_ipc_base_ptr: int,
    device_ptr: int,
) -> dict[str, object]:
    if device_handle.handle_type != "cuda_ipc_device":
        raise WorkerDataPlaneResourceError(
            "worker CUDA IPC span validation requires a cuda_ipc_device handle"
        )
    metadata = dict(device_handle.metadata)
    allocation_size = int(metadata["allocation_size_bytes"])
    offset_bytes = int(metadata.get("device_offset_bytes", 0))
    device_bytes = int(device_handle.size_bytes)
    if allocation_size <= 0:
        raise WorkerDataPlaneResourceError(
            "worker CUDA IPC span validation requires positive allocation_size_bytes"
        )
    if offset_bytes < 0:
        raise WorkerDataPlaneResourceError(
            "worker CUDA IPC span validation requires non-negative device_offset_bytes"
        )
    if device_bytes <= 0:
        raise WorkerDataPlaneResourceError(
            "worker CUDA IPC span validation requires positive device buffer size"
        )
    if int(device_ptr) != int(device_ipc_base_ptr) + offset_bytes:
        raise WorkerDataPlaneResourceError(
            "worker CUDA IPC opened pointer does not match declared device offset"
        )
    view_end = offset_bytes + device_bytes
    if view_end > allocation_size:
        raise WorkerDataPlaneResourceError(
            "worker CUDA IPC device view exceeds exported allocation span"
        )
    role = "destination" if request.direction == "h2d" else "source"
    for item in request.ranges:
        range_offset = int(item["dst_offset"] if request.direction == "h2d" else item["src_offset"])
        range_end = range_offset + int(item["bytes"])
        if range_offset < 0 or range_end > device_bytes:
            raise WorkerDataPlaneResourceError(
                "worker CUDA IPC range exceeds validated device view"
            )
    return {
        "validated": True,
        "device_buffer_id": device_handle.buffer_id,
        "device_buffer_role": role,
        "allocation_size_bytes": allocation_size,
        "device_offset_bytes": offset_bytes,
        "device_view_bytes": device_bytes,
        "device_view_end_bytes": view_end,
        "device_ipc_base_ptr": int(device_ipc_base_ptr),
        "device_ptr": int(device_ptr),
        "range_count": len(request.ranges),
    }


def _set_cuda_device_for_handle(backend, handle: WorkerBufferHandle) -> None:
    _set_cuda_device_index(backend, handle.device_index)


def _set_cuda_device_index(backend, device_index: int | None) -> None:
    if device_index is None:
        return
    setter = getattr(backend, "set_device", None)
    if callable(setter):
        setter(int(device_index))


def _resource_binding_evidence(
    *,
    request: WorkerDataPlaneRequest,
    ticket_id: str,
    plan_generation: int,
    cpu_handle: WorkerBufferHandle,
    device_handle: WorkerBufferHandle,
    register_cuda_host: bool,
) -> dict[str, object]:
    return {
        "transfer_id": str(request.transfer_id),
        "lease_id": str(request.lease_id),
        "ticket_id": str(ticket_id),
        "plan_generation": int(plan_generation),
        "session_id": str(request.session_id),
        "job_id": str(request.job_id),
        "direction": str(request.direction),
        "src_buffer_id": str(request.src_handle.buffer_id),
        "src_handle_type": str(request.src_handle.handle_type),
        "dst_buffer_id": str(request.dst_handle.buffer_id),
        "dst_handle_type": str(request.dst_handle.handle_type),
        "cpu_buffer_id": str(cpu_handle.buffer_id),
        "cpu_handle_type": str(cpu_handle.handle_type),
        "cpu_buffer_role": "source" if request.direction == "h2d" else "destination",
        "device_buffer_id": str(device_handle.buffer_id),
        "device_handle_type": str(device_handle.handle_type),
        "device_buffer_role": (
            "destination" if request.direction == "h2d" else "source"
        ),
        "device_index": device_handle.device_index,
        "cpu_buffer_opened": False,
        "cuda_host_registration_requested": bool(register_cuda_host),
        "cuda_host_registered": False,
        "device_ipc_opened": False,
    }


def _cuda_ipc_failure_lifecycle(
    *,
    request: WorkerDataPlaneRequest,
    ticket_id: str,
    plan_generation: int,
    device_handle: WorkerBufferHandle,
    cpu_handle: WorkerBufferHandle,
    open_evidence: dict[str, object],
    failure_cleanup: dict[str, object],
) -> dict[str, object]:
    span_validation = open_evidence.get("cuda_ipc_span_validation")
    lifecycle = {
        "source": "worker_cuda_ipc_lifecycle",
        "transfer_id": str(request.transfer_id),
        "lease_id": str(request.lease_id),
        "ticket_id": str(ticket_id),
        "plan_generation": int(plan_generation),
        "session_id": str(request.session_id),
        "job_id": str(request.job_id),
        "direction": str(request.direction),
        "src_buffer_id": str(request.src_handle.buffer_id),
        "dst_buffer_id": str(request.dst_handle.buffer_id),
        "cpu_buffer_id": str(cpu_handle.buffer_id),
        "cpu_handle_type": str(cpu_handle.handle_type),
        "cpu_buffer_role": "source" if request.direction == "h2d" else "destination",
        "cpu_buffer_opened": bool(open_evidence.get("cpu_buffer_opened", False)),
        "cpu_buffer_closed": bool(
            failure_cleanup.get("cpu_buffer_closed_after_failure", False)
        ),
        "cuda_host_registration_requested": bool(
            open_evidence.get("cuda_host_registration_requested", False)
        ),
        "cuda_host_registered": bool(open_evidence.get("cuda_host_registered", False)),
        "device_buffer_id": str(device_handle.buffer_id),
        "device_handle_type": str(device_handle.handle_type),
        "device_buffer_role": (
            "destination" if request.direction == "h2d" else "source"
        ),
        "device_index": device_handle.device_index,
        "device_bytes": int(device_handle.size_bytes),
        "device_ipc_opened": bool(open_evidence.get("device_ipc_opened", False)),
        "device_ipc_closed": bool(
            failure_cleanup.get("device_ipc_closed_after_failure", False)
        ),
        "resources_closed": False,
        "device_offset_bytes": int(device_handle.metadata.get("device_offset_bytes", 0)),
        "failure_cleanup": dict(failure_cleanup),
        "ticket_owner_binding": {
            "job_id": str(request.job_id),
            "session_id": str(request.session_id),
            "ticket_id": str(ticket_id),
            "lease_id": str(request.lease_id),
            "plan_generation": int(plan_generation),
        },
    }
    allocation_size = device_handle.metadata.get("allocation_size_bytes")
    if allocation_size is not None:
        lifecycle["allocation_size_bytes"] = int(allocation_size)
    if "device_ipc_base_ptr" in open_evidence:
        lifecycle["device_ipc_base_ptr"] = int(open_evidence["device_ipc_base_ptr"])
    if "device_ptr" in open_evidence:
        lifecycle["device_ptr"] = int(open_evidence["device_ptr"])
    if isinstance(span_validation, dict):
        lifecycle["cuda_ipc_span_validation"] = dict(span_validation)
        lifecycle["span_validated"] = bool(span_validation.get("validated", False))
    else:
        lifecycle["span_validated"] = False
    return lifecycle


__all__ = [
    "WorkerDataPlaneResourceBinder",
    "WorkerDataPlaneResourceBinding",
    "WorkerDataPlaneResourceError",
    "WorkerDataPlaneResources",
]
