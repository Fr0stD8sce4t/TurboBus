from __future__ import annotations

import os
import time
from dataclasses import dataclass, replace
from typing import Any

from ..backends.cuda import default_cuda_backend
from ..profiling.daemon_format import profile_from_daemon_entry
from ..runtime_options import RuntimeOptions
from . import validation as worker_validation
from .models import (
    WorkerTransferRequest,
    WorkerTransferResult,
    WorkerTransferState,
)
from .resources import WorkerDataPlaneResourceBinder, WorkerDataPlaneResources
from .staging_pool import WorkerStagingSlot


class CudaWorkerExecutor:
    """CUDA worker executor for daemon-authorized worker-managed transfers."""

    def __init__(
        self,
        *,
        backend=default_cuda_backend,
        options: RuntimeOptions | None = None,
    ) -> None:
        self.backend = backend
        self.options = options or RuntimeOptions()
        self._runtime_cache: dict[tuple[object, ...], object] = {}
        self._inflight: dict[str, CudaWorkerTransferHandle] = {}
        self._terminal: dict[str, CudaWorkerTransferHandle] = {}

    def execute(
        self,
        request: WorkerTransferRequest,
        staging_slot: WorkerStagingSlot,
    ) -> WorkerTransferResult:
        _validate_request_and_slot(request, staging_slot)
        with WorkerDataPlaneResourceBinder(backend=self.backend).bind(request) as resources:
            return self.execute_bound(request, staging_slot, resources)

    def execute_bound(
        self,
        request: WorkerTransferRequest,
        staging_slot: WorkerStagingSlot,
        resources: WorkerDataPlaneResources,
    ) -> WorkerTransferResult:
        return self.wait(self.submit_bound(request, staging_slot, resources))

    def submit_bound(
        self,
        request: WorkerTransferRequest,
        staging_slot: WorkerStagingSlot,
        resources: WorkerDataPlaneResources,
    ) -> "CudaWorkerTransferHandle":
        _validate_request_and_slot(request, staging_slot)
        if not isinstance(resources, WorkerDataPlaneResources):
            raise TypeError("resources must be WorkerDataPlaneResources")
        if resources.request != request.data_plane:
            return CudaWorkerTransferHandle.failed_before_submit(
                request=request,
                staging_slot=staging_slot,
                resources=resources,
                target_device=-1,
                plan_payload={},
                resource_evidence=_resource_evidence(request, resources),
                error=ValueError("bound resources do not match the worker request"),
            )
        if request.transfer_id in self._inflight:
            raise RuntimeError("worker transfer is already in flight")
        target_device = _target_device_for_request(request)
        if target_device is None:
            return CudaWorkerTransferHandle.failed_before_submit(
                request=request,
                staging_slot=staging_slot,
                resources=resources,
                target_device=-1,
                plan_payload={},
                resource_evidence=_resource_evidence(request, resources),
                error=ValueError("CUDA worker executor requires a GPU device index"),
            )
        resource_evidence = _resource_evidence(request, resources)
        plan_payload: dict[str, object] = {}
        try:
            plan_payload = _worker_plan_payload(request, int(target_device))
            _trace_cuda_worker_stage(
                "cuda_executor_plan_ready",
                transfer_id=request.transfer_id,
                target_device=target_device,
            )
            native_plan = self.backend.make_transfer_plan(plan_payload)
            runtime, runtime_reused, runtime_cache_key = self._runtime_for_request(
                request,
                target_device=int(target_device),
            )
            if request.data_plane.direction == "h2d":
                _trace_cuda_worker_stage(
                    "cuda_executor_fetch_start",
                    transfer_id=request.transfer_id,
                    host_bytes=resources.host_bytes,
                    device_bytes=resources.device_bytes,
                )
                native_handle = self.backend.fetch_plan_to_gpu(
                    runtime,
                    resources.host_ptr,
                    resources.host_bytes,
                    resources.device_ptr,
                    resources.device_bytes,
                    native_plan,
                )
            else:
                _trace_cuda_worker_stage(
                    "cuda_executor_offload_start",
                    transfer_id=request.transfer_id,
                    host_bytes=resources.host_bytes,
                    device_bytes=resources.device_bytes,
                )
                native_handle = self.backend.offload_plan_to_cpu(
                    runtime,
                    resources.device_ptr,
                    resources.device_bytes,
                    resources.host_ptr,
                    resources.host_bytes,
                    native_plan,
                )
        except Exception as exc:
            handle = CudaWorkerTransferHandle.failed_before_submit(
                request=request,
                staging_slot=staging_slot,
                resources=resources,
                target_device=int(target_device),
                plan_payload=plan_payload,
                resource_evidence=resource_evidence,
                error=exc,
            )
            self._terminal[request.transfer_id] = handle
            return handle
        handle = CudaWorkerTransferHandle(
            transfer_id=request.transfer_id,
            request=request,
            staging_slot=staging_slot,
            resources=resources,
            runtime=runtime,
            native_handle=native_handle,
            plan_payload=plan_payload,
            target_device=int(target_device),
            resource_evidence=resource_evidence,
            runtime_reused=runtime_reused,
            runtime_cache_key=runtime_cache_key,
        )
        self._inflight[request.transfer_id] = handle
        _trace_cuda_worker_stage(
            "cuda_executor_submit_done",
            transfer_id=request.transfer_id,
        )
        return handle

    def wait(
        self,
        handle: "CudaWorkerTransferHandle",
    ) -> WorkerTransferResult:
        if not isinstance(handle, CudaWorkerTransferHandle):
            raise TypeError("handle must be a CudaWorkerTransferHandle")
        request = handle.request
        staging_slot = handle.staging_slot
        resources = handle.resources
        if handle.state is WorkerTransferState.FAILED:
            self._terminal[handle.transfer_id] = handle
            self._inflight.pop(handle.transfer_id, None)
            return _failed_result(
                request,
                staging_slot,
                handle.error or "CUDA worker transfer failed before submit",
                resources=resources,
            )
        try:
            _trace_cuda_worker_stage(
                "cuda_executor_wait_start",
                transfer_id=request.transfer_id,
            )
            self.backend.wait(handle.runtime, handle.native_handle)
            _trace_cuda_worker_stage(
                "cuda_executor_wait_done",
                transfer_id=request.transfer_id,
            )
            stats = self.backend.stats(handle.runtime, handle.native_handle)
            handle.mark_complete(stats)
            _trace_cuda_worker_stage(
                "cuda_executor_stats_done",
                transfer_id=request.transfer_id,
            )
        except Exception as exc:
            handle.mark_failed(exc)
            self._terminal[handle.transfer_id] = handle
            self._inflight.pop(handle.transfer_id, None)
            return _failed_result(
                request,
                staging_slot,
                str(exc),
                resources=resources,
            )

        plan_payload = handle.plan_payload
        target_device = handle.target_device
        resource_evidence = handle.resource_evidence
        bytes_completed = _stats_int(stats, "bytes", int(plan_payload["total_bytes"]))
        planned_direct_bytes = _assignment_byte_count(plan_payload, "direct")
        planned_relay_bytes = _assignment_byte_count(plan_payload, "relay")
        planned_direct_chunks = _assignment_chunk_count(plan_payload, "direct")
        planned_relay_chunks = _assignment_chunk_count(plan_payload, "relay")
        try:
            _trace_cuda_worker_stage(
                "cuda_executor_verify_start",
                transfer_id=request.transfer_id,
            )
            completion_evidence = _worker_completion_evidence(
                backend=self.backend,
                request=request,
                resources=resources,
                target_device=int(target_device),
                ranges=_plan_transfer_ranges(plan_payload),
                expected_bytes=int(plan_payload["total_bytes"]),
                resource_evidence=resource_evidence,
                direct_bytes=planned_direct_bytes,
                direct_chunks=planned_direct_chunks,
                relay_bytes=planned_relay_bytes,
                relay_chunks=planned_relay_chunks,
            )
            completion_evidence.setdefault("resource_evidence", resource_evidence)
            _trace_cuda_worker_stage(
                "cuda_executor_verify_done",
                transfer_id=request.transfer_id,
            )
        except Exception as exc:
            handle.mark_failed(exc)
            self._terminal[handle.transfer_id] = handle
            self._inflight.pop(handle.transfer_id, None)
            return _failed_result(
                request,
                staging_slot,
                str(exc),
                resources=resources,
            )
        direct_chunks = _stats_int(
            stats,
            "direct_chunks",
            planned_direct_chunks,
        )
        relay_chunks = _stats_int(
            stats,
            "relay_chunks",
            planned_relay_chunks,
        )
        direct_bytes = _stats_int(
            stats,
            "direct_bytes",
            planned_direct_bytes,
        )
        relay_bytes = _stats_int(
            stats,
            "relay_bytes",
            planned_relay_bytes,
        )
        if int(direct_bytes) != int(planned_direct_bytes):
            raise RuntimeError("worker direct bytes do not match daemon plan")
        if int(relay_bytes) != int(planned_relay_bytes):
            raise RuntimeError("worker relay bytes do not match daemon plan")
        if int(direct_chunks) != int(planned_direct_chunks):
            raise RuntimeError("worker direct chunks do not match daemon plan")
        if int(relay_chunks) != int(planned_relay_chunks):
            raise RuntimeError("worker relay chunks do not match daemon plan")
        path_level_evidence = _path_level_execution_evidence(
            stats,
            plan_payload=plan_payload,
            expected_direct_bytes=planned_direct_bytes,
            expected_relay_bytes=planned_relay_bytes,
        )
        canonical_evidence = _canonical_worker_completion_evidence(
            request=request,
            target_device=int(target_device),
            staging_slot=staging_slot,
            resource_evidence=resource_evidence,
            completion_evidence=completion_evidence,
            path_level_evidence=path_level_evidence,
            direct_bytes=direct_bytes,
            direct_chunks=direct_chunks,
            relay_bytes=relay_bytes,
            relay_chunks=relay_chunks,
        )
        result = WorkerTransferResult(
            transfer_id=request.transfer_id,
            state=WorkerTransferState.COMPLETE,
            bytes_completed=bytes_completed,
            metadata={
                **completion_evidence,
                **canonical_evidence,
                **_ticket_binding_metadata(request),
                "async_data_plane": handle.execution_evidence(),
                "worker_runtime_feedback": _worker_runtime_feedback_for_handle(
                    handle,
                    executor=self,
                ),
            },
        )
        self._terminal[handle.transfer_id] = handle
        self._inflight.pop(handle.transfer_id, None)
        return result

    def describe_inflight(self) -> dict[str, dict[str, object]]:
        return {
            transfer_id: handle.as_dict()
            for transfer_id, handle in sorted(self._inflight.items())
        }

    def describe_terminal(self) -> dict[str, dict[str, object]]:
        return {
            transfer_id: handle.as_dict()
            for transfer_id, handle in sorted(self._terminal.items())
        }

    def _runtime_for_request(
        self,
        request: WorkerTransferRequest,
        *,
        target_device: int,
    ) -> tuple[object, bool, tuple[object, ...]]:
        runtime_options = _runtime_options_for_request(self.options, request)
        key = _runtime_cache_key(
            runtime_options,
            target_device=int(target_device),
            relay_gpus=_relay_gpus_for_request(request),
            profile_key=request.data_plane.metadata.get("daemon_profile_key"),
        )
        runtime = self._runtime_cache.get(key)
        if runtime is not None:
            return runtime, True, key
        _trace_cuda_worker_stage(
            "cuda_executor_runtime_create_start",
            transfer_id=request.transfer_id,
        )
        runtime = self.backend.create_runtime(runtime_options)
        _trace_cuda_worker_stage(
            "cuda_executor_runtime_create_done",
            transfer_id=request.transfer_id,
        )
        _trace_cuda_worker_stage(
            "cuda_executor_runtime_init_start",
            transfer_id=request.transfer_id,
            relay_gpus=_relay_gpus_for_request(request),
        )
        self.backend.initialize_runtime(
            runtime,
            int(target_device),
            _relay_gpus_for_request(request),
        )
        _trace_cuda_worker_stage(
            "cuda_executor_runtime_init_done",
            transfer_id=request.transfer_id,
        )
        _install_daemon_profile_if_available(
            backend=self.backend,
            runtime=runtime,
            request=request,
            target_device=int(target_device),
        )
        self._runtime_cache[key] = runtime
        return runtime, False, key


@dataclass
class CudaWorkerTransferHandle:
    transfer_id: str
    request: WorkerTransferRequest
    staging_slot: WorkerStagingSlot
    resources: WorkerDataPlaneResources
    runtime: object | None
    native_handle: object | None
    plan_payload: dict[str, object]
    target_device: int
    resource_evidence: dict[str, object]
    runtime_reused: bool = False
    runtime_cache_key: tuple[object, ...] = ()
    submitted_at: float = 0.0
    completed_at: float | None = None
    state: WorkerTransferState = WorkerTransferState.RUNNING
    error: str | None = None
    stats: object | None = None

    def __post_init__(self) -> None:
        if self.submitted_at == 0.0:
            self.submitted_at = time.time()
        self.transfer_id = str(self.transfer_id)
        self.target_device = int(self.target_device)
        self.resource_evidence = dict(self.resource_evidence)
        self.runtime_reused = bool(self.runtime_reused)
        self.runtime_cache_key = tuple(self.runtime_cache_key)
        self.state = WorkerTransferState(self.state)

    @classmethod
    def failed_before_submit(
        cls,
        *,
        request: WorkerTransferRequest,
        staging_slot: WorkerStagingSlot,
        resources: WorkerDataPlaneResources,
        target_device: int,
        plan_payload: dict[str, object],
        resource_evidence: dict[str, object],
        error: Exception,
    ) -> "CudaWorkerTransferHandle":
        return cls(
            transfer_id=request.transfer_id,
            request=request,
            staging_slot=staging_slot,
            resources=resources,
            runtime=None,
            native_handle=None,
            plan_payload=plan_payload,
            target_device=int(target_device),
            resource_evidence=resource_evidence,
            state=WorkerTransferState.FAILED,
            error=str(error) or error.__class__.__name__,
            completed_at=time.time(),
        )

    def mark_complete(self, stats: object) -> None:
        self.stats = stats
        self.state = WorkerTransferState.COMPLETE
        self.completed_at = time.time()

    def mark_failed(self, error: Exception) -> None:
        self.state = WorkerTransferState.FAILED
        self.error = str(error) or error.__class__.__name__
        self.completed_at = time.time()

    def as_dict(self) -> dict[str, object]:
        evidence = self.execution_evidence()
        evidence["resource_evidence"] = dict(self.resource_evidence)
        return evidence

    def execution_evidence(self) -> dict[str, object]:
        return {
            "transfer_id": self.transfer_id,
            "state": self.state.value,
            "ticket_id": self.request.ticket.ticket_id,
            "plan_generation": int(self.request.ticket.metadata["plan_generation"]),
            "target_device": self.target_device,
            "relay_gpus": _relay_gpus_for_request(self.request),
            "runtime_reused": self.runtime_reused,
            "runtime_cache_key": tuple(self.runtime_cache_key),
            "submitted_at": self.submitted_at,
            "completed_at": self.completed_at,
            "submit_to_complete_ms": (
                None
                if self.completed_at is None
                else (self.completed_at - self.submitted_at) * 1000.0
            ),
            "error": self.error,
            "staging_slot_id": self.staging_slot.slot_id,
        }


def _worker_runtime_feedback_for_handle(
    handle: CudaWorkerTransferHandle,
    *,
    executor: CudaWorkerExecutor,
) -> dict[str, object]:
    return {
        "source": "cuda_worker_executor_runtime_feedback",
        "transfer_id": handle.transfer_id,
        "state": handle.state.value,
        "runtime_reused": bool(handle.runtime_reused),
        "runtime_cache_key": tuple(handle.runtime_cache_key),
        "runtime_cache_size": len(executor._runtime_cache),
        "inflight_count": len(executor._inflight),
        "terminal_count": len(executor._terminal),
        "submit_to_complete_ms": (
            None
            if handle.completed_at is None
            else max(0.0, (handle.completed_at - handle.submitted_at) * 1000.0)
        ),
        "target_device": int(handle.target_device),
        "relay_gpus": _relay_gpus_for_request(handle.request),
    }


def _runtime_options_for_request(
    options: RuntimeOptions,
    request: WorkerTransferRequest,
) -> RuntimeOptions:
    max_chunk_bytes = request.data_plane.staging.max_chunk_bytes
    return replace(
        options,
        chunk_bytes=max(int(options.chunk_bytes), int(max_chunk_bytes)),
    )


def _runtime_cache_key(
    options: RuntimeOptions,
    *,
    target_device: int,
    relay_gpus: list[int],
    profile_key: object | None,
) -> tuple[object, ...]:
    return (
        int(target_device),
        tuple(int(gpu) for gpu in relay_gpus),
        int(options.chunk_bytes),
        int(options.staging_slots),
        bool(options.enable_peer_access),
        int(options.profile_bytes),
        bool(options.profile_on_first_transfer),
        bool(options.profile_cache_enabled),
        int(options.min_chunks_for_relay),
        int(options.min_pool_bytes),
        float(options.relay_min_effective_bw_gbps),
        float(options.relay_min_direct_ratio),
        bool(options.enable_dynamic_weights),
        float(options.dynamic_weight_alpha),
        None if profile_key is None else str(profile_key),
    )


def _target_device_for_request(request: WorkerTransferRequest) -> int | None:
    handle = (
        request.data_plane.dst_handle
        if request.data_plane.direction == "h2d"
        else request.data_plane.src_handle
    )
    return handle.device_index


def _worker_plan_payload(
    request: WorkerTransferRequest,
    target_device: int,
) -> dict[str, object]:
    if not request.data_plane.plan:
        raise ValueError("CUDA worker executor requires a daemon-issued transfer plan")
    _require_ticket_authorizes_current_worker_plan(request)
    return _exact_daemon_plan_payload(request, int(target_device))


def _install_daemon_profile_if_available(
    *,
    backend,
    runtime,
    request: WorkerTransferRequest,
    target_device: int,
) -> None:
    profile_entry = request.data_plane.metadata.get("daemon_profile_entry")
    if not isinstance(profile_entry, dict):
        return
    profile = profile_from_daemon_entry(profile_entry, int(target_device))
    setter = getattr(backend, "set_cached_profile", None)
    if not callable(setter):
        raise RuntimeError("CUDA backend does not support cached profile installation")
    setter(runtime, profile)


def _require_ticket_authorizes_current_worker_plan(
    request: WorkerTransferRequest,
) -> None:
    worker_validation.validate_daemon_issued_ticket(
        request.ticket,
        now=_ticket_validation_time(request),
    )
    worker_validation.validate_ticket_matches_worker_request(
        request.ticket,
        request.authorization,
        request.data_plane,
    )
    if request.ticket.metadata.get("transfer_id") != request.transfer_id:
        raise ValueError("execution ticket transfer_id does not match worker request")


def _exact_daemon_plan_payload(
    request: WorkerTransferRequest,
    target_device: int,
) -> dict[str, object]:
    source_plan = dict(request.data_plane.plan)
    assignments: list[dict[str, object]] = []
    execution_ranges: list[dict[str, int]] = []
    total_bytes = 0
    relay_gpus = set(_relay_gpus_for_request(request))
    for assignment in source_plan.get("assignments", ()) or ():
        if not isinstance(assignment, dict):
            raise ValueError("daemon plan assignment must be a mapping")
        path = assignment.get("path")
        if not isinstance(path, dict):
            raise ValueError("daemon plan assignment path must be a mapping")
        path_kind = str(path.get("kind", "")).lower()
        if path_kind not in {"direct", "relay"}:
            raise ValueError("daemon plan path must be direct or relay")
        if str(path.get("direction", "")).lower() != request.data_plane.direction:
            raise ValueError("daemon plan direction does not match worker request")
        plan_path = dict(path)
        if int(plan_path.get("target_device", target_device)) != int(target_device):
            raise ValueError("daemon plan target does not match worker target")
        if not bool(plan_path.get("enabled", True)):
            raise ValueError("daemon plan path is disabled")
        if path_kind == "relay":
            relay_gpu = int(plan_path.get("relay_device", -1))
            if relay_gpu not in relay_gpus:
                raise ValueError("daemon plan relay is not authorized by worker ticket")
            plan_path["relay_device"] = relay_gpu
        else:
            plan_path["relay_device"] = -1
        plan_path["enabled"] = True
        chunks = []
        for chunk in assignment.get("chunks", ()) or ():
            if not isinstance(chunk, dict):
                raise ValueError("daemon plan chunk must be a mapping")
            chunks.append(
                {
                    "src_offset": int(chunk["src_offset"]),
                    "dst_offset": int(chunk["dst_offset"]),
                    "bytes": int(chunk["bytes"]),
                }
            )
        if not chunks:
            continue
        chunk_bytes = sum(int(chunk["bytes"]) for chunk in chunks)
        total_bytes += chunk_bytes
        execution_ranges.extend(chunks)
        assignments.append(
            {
                "path": plan_path,
                "chunks": chunks,
                "bytes": chunk_bytes,
                "chunk_count": len(chunks),
            }
        )
    if not assignments:
        raise ValueError("daemon plan has no authorized executable chunks")
    if tuple(execution_ranges) != request.data_plane.ranges:
        raise ValueError("authorized ranges do not match daemon plan")
    declared_total_bytes = int(source_plan.get("total_bytes", total_bytes))
    if declared_total_bytes != total_bytes:
        raise ValueError("daemon plan total bytes do not match assigned chunks")
    return {
        "total_bytes": total_bytes,
        "chunk_bytes": int(
            source_plan.get("chunk_bytes", request.data_plane.staging.max_chunk_bytes)
        ),
        "assignments": assignments,
    }


def _assignment_chunk_count(plan_payload: dict[str, object], path_kind: str) -> int:
    total = 0
    for assignment in plan_payload.get("assignments", ()) or ():
        path = assignment.get("path") if isinstance(assignment, dict) else None
        if not isinstance(path, dict):
            continue
        if str(path.get("kind", "")).lower() != path_kind:
            continue
        total += len(assignment.get("chunks", ()) or ())
    return total


def _assignment_byte_count(plan_payload: dict[str, object], path_kind: str) -> int:
    total = 0
    for assignment in plan_payload.get("assignments", ()) or ():
        path = assignment.get("path") if isinstance(assignment, dict) else None
        if not isinstance(path, dict):
            continue
        if str(path.get("kind", "")).lower() != path_kind:
            continue
        for chunk in assignment.get("chunks", ()) or ():
            if isinstance(chunk, dict):
                total += int(chunk.get("bytes", 0))
    return total


def _relay_gpus_for_request(request: WorkerTransferRequest) -> list[int]:
    relays = request.data_plane.metadata.get("relay_gpus")
    if relays is None:
        return [int(request.data_plane.relay_gpu)]
    resolved = sorted({int(relay) for relay in relays})
    if not resolved:
        raise ValueError("worker request has no relay GPUs")
    if int(request.data_plane.relay_gpu) not in resolved:
        raise ValueError("worker request primary relay is not authorized")
    return resolved


def _metadata_path(*, direction: str, direct_chunks: int) -> str:
    prefix = "pool" if direct_chunks > 0 else "relay"
    return f"{prefix}_{direction}"


def _validate_request_and_slot(
    request: WorkerTransferRequest,
    staging_slot: WorkerStagingSlot,
) -> None:
    if not isinstance(request, WorkerTransferRequest):
        raise TypeError("request must be a WorkerTransferRequest")
    if request.ticket is None:
        raise ValueError("CUDA worker executor requires a daemon-issued ExecutionTicket")
    if not isinstance(staging_slot, WorkerStagingSlot):
        raise TypeError("staging_slot must be a WorkerStagingSlot")
    if staging_slot.transfer_id != request.transfer_id:
        raise ValueError("staging slot transfer does not match request")
    if staging_slot.lease_id != request.authorization.lease_id:
        raise ValueError("staging slot lease does not match request")
    if staging_slot.relay_gpu != request.authorization.relay_gpu:
        raise ValueError("staging slot relay does not match request")


def _failed_result(
    request: WorkerTransferRequest,
    staging_slot: WorkerStagingSlot,
    error: str,
    *,
    resources: WorkerDataPlaneResources | None = None,
) -> WorkerTransferResult:
    direct_bytes, direct_chunks, relay_bytes, relay_chunks = (
        _planned_path_split_for_request(request)
    )
    target_device = _target_device_for_request(request)
    return WorkerTransferResult(
        transfer_id=request.transfer_id,
        state=WorkerTransferState.FAILED,
        error=error,
        bytes_completed=0,
        metadata={
            "executor": "cuda_worker",
            "path": _metadata_path(direction=request.data_plane.direction, direct_chunks=0),
            "plan_source": "daemon",
            "relay_gpu": request.authorization.relay_gpu,
            "relay_gpus": _relay_gpus_for_request(request),
            "target_device": target_device,
            "src_buffer_id": request.authorization.src_buffer.buffer_id,
            "dst_buffer_id": request.authorization.dst_buffer.buffer_id,
            "staging_slot_id": staging_slot.slot_id,
            "direct_bytes": direct_bytes,
            "direct_chunks": direct_chunks,
            "relay_bytes": relay_bytes,
            "relay_chunks": relay_chunks,
            "failure_source": "cuda_worker",
            **_resource_evidence_metadata(resources, request),
            **_ticket_binding_metadata(request),
        },
    )


def _planned_path_split_for_request(
    request: WorkerTransferRequest,
) -> tuple[int, int, int, int]:
    direct_bytes = 0
    direct_chunks = 0
    relay_bytes = 0
    relay_chunks = 0
    try:
        assignments = request.data_plane.plan.get("assignments", ()) or ()
    except AttributeError:
        assignments = ()
    for assignment in assignments:
        if not isinstance(assignment, dict):
            continue
        path = assignment.get("path")
        if not isinstance(path, dict):
            continue
        if str(path.get("direction", "")).lower() != request.data_plane.direction:
            continue
        chunk_count = len(assignment.get("chunks", ()) or ())
        byte_count = 0
        for chunk in assignment.get("chunks", ()) or ():
            if isinstance(chunk, dict):
                byte_count += int(chunk.get("bytes", 0) or 0)
        if str(path.get("kind", "")).lower() == "direct":
            direct_bytes += byte_count
            direct_chunks += chunk_count
        elif str(path.get("kind", "")).lower() == "relay":
            relay_bytes += byte_count
            relay_chunks += chunk_count
    if direct_bytes == 0 and relay_bytes == 0:
        relay_bytes = sum(int(item["bytes"]) for item in request.data_plane.ranges)
        relay_chunks = len(request.data_plane.ranges)
    return direct_bytes, direct_chunks, relay_bytes, relay_chunks


def _ticket_validation_time(request: WorkerTransferRequest) -> float | None:
    if "ticket_authorized_at" not in request.data_plane.metadata:
        return None
    return time.time()


def _ticket_binding_metadata(
    request: WorkerTransferRequest,
) -> dict[str, object]:
    metadata: dict[str, object] = {"ticket_id": request.ticket.ticket_id}
    transfer_id = request.ticket.metadata.get("transfer_id")
    if transfer_id is not None:
        metadata["transfer_id"] = str(transfer_id)
    plan_generation = request.ticket.metadata.get("plan_generation")
    if plan_generation is not None:
        metadata["plan_generation"] = int(plan_generation)
    return metadata


def _resource_evidence(
    request: WorkerTransferRequest,
    resources: WorkerDataPlaneResources,
) -> dict[str, object]:
    evidence = resources.as_dict()
    evidence.setdefault("evidence_source", "worker_data_plane_resources")
    evidence.update(_ticket_resource_binding_metadata(request))
    return evidence


def _resource_evidence_metadata(
    resources: WorkerDataPlaneResources | None,
    request: WorkerTransferRequest | None = None,
) -> dict[str, object]:
    if resources is None:
        return {}
    if request is None:
        evidence = resources.as_dict()
        evidence.setdefault("evidence_source", "worker_data_plane_resources")
    else:
        evidence = _resource_evidence(request, resources)
    return {"resource_evidence": evidence}


def _ticket_resource_binding_metadata(
    request: WorkerTransferRequest,
) -> dict[str, object]:
    metadata = _ticket_binding_metadata(request)
    metadata.setdefault("source_buffer_id", request.ticket.source_buffer_id)
    metadata.setdefault("destination_buffer_id", request.ticket.destination_buffer_id)
    metadata.setdefault("ticket_job_id", request.ticket.job_id)
    metadata.setdefault("ticket_session_id", request.ticket.session_id)
    return metadata


def _stats_int(stats: Any, field_name: str, default: int) -> int:
    value = _stats_value(stats, field_name, default)
    return int(value if value is not None else default)


def _stats_bool(stats: Any, field_name: str, default: bool) -> bool:
    value = _stats_value(stats, field_name, default)
    return bool(value if value is not None else default)


def _stats_value(stats: Any, field_name: str, default: Any) -> Any:
    value = getattr(stats, field_name, default)
    if isinstance(stats, dict):
        value = stats.get(field_name, value)
    return value


def _path_level_execution_evidence(
    stats: Any,
    *,
    plan_payload: dict[str, object],
    expected_direct_bytes: int,
    expected_relay_bytes: int,
) -> dict[str, object]:
    native_path_stats = _native_path_stats(stats)
    if not native_path_stats:
        planned_path_stats = _planned_path_stats(plan_payload)
        relay_device_stats = _relay_device_stats_from_path_stats(planned_path_stats)
        return {
            "relay_device_stats": relay_device_stats,
            "path_level_evidence": {
                "source": "daemon_plan_without_native_path_stats",
                "path_stats": planned_path_stats,
                "relay_device_stats": relay_device_stats,
                "direct_bytes": int(expected_direct_bytes),
                "relay_bytes": int(expected_relay_bytes),
                "direct_chunks": sum(
                    int(path["chunks"])
                    for path in planned_path_stats
                    if str(path.get("kind", "")).startswith("direct")
                ),
                "relay_chunks": sum(
                    int(path["chunks"])
                    for path in planned_path_stats
                    if str(path.get("kind", "")).startswith("relay")
                ),
            }
        }
    direct_bytes = sum(
        int(path["bytes"])
        for path in native_path_stats
        if str(path.get("kind", "")).startswith("direct")
    )
    relay_bytes = sum(
        int(path["bytes"])
        for path in native_path_stats
        if str(path.get("kind", "")).startswith("relay")
    )
    if direct_bytes != int(expected_direct_bytes):
        raise RuntimeError("native direct path stats do not match daemon plan")
    if relay_bytes != int(expected_relay_bytes):
        raise RuntimeError("native relay path stats do not match daemon plan")
    return {
        "native_path_stats": native_path_stats,
        "relay_device_stats": _relay_device_stats(stats),
        "path_level_evidence": {
            "source": "native_cuda_transfer_stats",
            "path_stats": native_path_stats,
            "relay_device_stats": _relay_device_stats(stats),
            "direct_bytes": direct_bytes,
            "relay_bytes": relay_bytes,
            "direct_chunks": sum(
                int(path["chunks"])
                for path in native_path_stats
                if str(path.get("kind", "")).startswith("direct")
            ),
            "relay_chunks": sum(
                int(path["chunks"])
                for path in native_path_stats
                if str(path.get("kind", "")).startswith("relay")
            ),
        },
    }


def _relay_device_stats_from_path_stats(
    path_stats: tuple[dict[str, object], ...],
) -> tuple[dict[str, int], ...]:
    by_relay: dict[int, dict[str, int]] = {}
    for path in path_stats:
        if not str(path.get("kind", "")).startswith("relay"):
            continue
        relay_device = int(path.get("relay_device", -1))
        if relay_device < 0:
            continue
        record = by_relay.setdefault(
            relay_device,
            {
                "relay_device": relay_device,
                "bytes": 0,
                "chunks": 0,
            },
        )
        record["bytes"] += int(path.get("bytes", 0) or 0)
        record["chunks"] += int(path.get("chunks", 0) or 0)
    return tuple(by_relay[key] for key in sorted(by_relay))


def _native_path_stats(stats: Any) -> tuple[dict[str, object], ...]:
    raw_path_stats = _stats_value(stats, "path_stats", ()) or ()
    normalized: list[dict[str, object]] = []
    for item in raw_path_stats:
        kind = _path_stat_value(item, "kind", "")
        direction = _path_stat_value(item, "direction", "")
        normalized.append(
            {
                "kind": str(kind),
                "direction": str(direction),
                "target_device": int(_path_stat_value(item, "target_device", -1)),
                "relay_device": int(_path_stat_value(item, "relay_device", -1)),
                "bytes": int(_path_stat_value(item, "bytes", 0)),
                "chunks": int(_path_stat_value(item, "chunks", 0)),
                "cuda_elapsed_ms": float(
                    _path_stat_value(item, "cuda_elapsed_ms", 0.0)
                ),
                "gib_per_second": float(
                    _path_stat_value(item, "gib_per_second", 0.0)
                ),
            }
        )
    return tuple(normalized)


def _relay_device_stats(stats: Any) -> tuple[dict[str, int], ...]:
    relay_devices = tuple(int(item) for item in _stats_value(stats, "relay_devices", ()) or ())
    relay_bytes = tuple(
        int(item) for item in _stats_value(stats, "relay_device_bytes", ()) or ()
    )
    relay_chunks = tuple(
        int(item) for item in _stats_value(stats, "relay_device_chunks", ()) or ()
    )
    records: list[dict[str, int]] = []
    for index, relay_device in enumerate(relay_devices):
        records.append(
            {
                "relay_device": relay_device,
                "bytes": relay_bytes[index] if index < len(relay_bytes) else 0,
                "chunks": relay_chunks[index] if index < len(relay_chunks) else 0,
            }
        )
    return tuple(records)


def _path_stat_value(item: Any, field_name: str, default: Any) -> Any:
    if isinstance(item, dict):
        return item.get(field_name, default)
    return getattr(item, field_name, default)


def _planned_path_stats(
    plan_payload: dict[str, object],
) -> tuple[dict[str, object], ...]:
    records: list[dict[str, object]] = []
    for assignment in plan_payload.get("assignments", ()) or ():
        if not isinstance(assignment, dict):
            continue
        path = assignment.get("path")
        if not isinstance(path, dict):
            continue
        records.append(
            {
                "kind": str(path.get("kind", "")),
                "direction": str(path.get("direction", "")),
                "target_device": int(path.get("target_device", -1)),
                "relay_device": int(path.get("relay_device", -1)),
                "bytes": int(assignment.get("bytes", 0) or 0),
                "chunks": int(assignment.get("chunk_count", 0) or 0),
                "cuda_elapsed_ms": 0.0,
                "gib_per_second": 0.0,
            }
        )
    return tuple(records)


def _canonical_worker_completion_evidence(
    *,
    request: WorkerTransferRequest,
    target_device: int,
    staging_slot: WorkerStagingSlot,
    resource_evidence: dict[str, object],
    completion_evidence: dict[str, object],
    path_level_evidence: dict[str, object],
    direct_bytes: int,
    direct_chunks: int,
    relay_bytes: int,
    relay_chunks: int,
) -> dict[str, object]:
    expected_bytes = int(direct_bytes) + int(relay_bytes)
    verified_bytes = int(completion_evidence.get("verified_bytes", expected_bytes) or 0)
    if verified_bytes != expected_bytes:
        raise RuntimeError("worker completion verified bytes do not match daemon plan")
    canonical = {
        "expected_bytes": expected_bytes,
        "verified_bytes": verified_bytes,
        "content_match": bool(completion_evidence.get("content_match", False)),
        "verification_source": completion_evidence.get(
            "verification_source",
            "cuda_worker",
        ),
        "verification_method": completion_evidence.get(
            "verification_method",
            "worker_backend_verification",
        ),
        "executor": "cuda_worker",
        "path": _metadata_path(
            direction=request.data_plane.direction,
            direct_chunks=int(direct_chunks),
        ),
        "plan_source": "daemon",
        "relay_gpu": request.data_plane.relay_gpu,
        "relay_gpus": _relay_gpus_for_request(request),
        "target_device": int(target_device),
        "src_buffer_id": request.data_plane.src_handle.buffer_id,
        "dst_buffer_id": request.data_plane.dst_handle.buffer_id,
        "staging_slot_id": staging_slot.slot_id,
        "resource_evidence": resource_evidence,
        "direct_bytes": int(direct_bytes),
        "direct_chunks": int(direct_chunks),
        "relay_bytes": int(relay_bytes),
        "relay_chunks": int(relay_chunks),
    }
    if "source_digest" in completion_evidence:
        canonical["source_digest"] = completion_evidence["source_digest"]
    if "destination_digest" in completion_evidence:
        canonical["destination_digest"] = completion_evidence["destination_digest"]
    canonical.update(path_level_evidence)
    return canonical


def _worker_completion_evidence(
    *,
    backend,
    request: WorkerTransferRequest,
    resources: WorkerDataPlaneResources,
    target_device: int,
    ranges: tuple[dict[str, int], ...],
    expected_bytes: int,
    resource_evidence: dict[str, object],
    direct_bytes: int,
    direct_chunks: int,
    relay_bytes: int,
    relay_chunks: int,
) -> dict[str, object]:
    if _request_skips_verification(request):
        return _skipped_verification_evidence(
            expected_bytes=int(expected_bytes),
            resource_evidence=resource_evidence,
            executor="cuda_worker",
            path=_metadata_path(
                direction=request.data_plane.direction,
                direct_chunks=int(direct_chunks),
            ),
            target_device=int(target_device),
            direct_bytes=int(direct_bytes),
            direct_chunks=int(direct_chunks),
            relay_bytes=int(relay_bytes),
            relay_chunks=int(relay_chunks),
        )
    verifier = getattr(backend, "verify_transfer", None)
    if not callable(verifier):
        raise RuntimeError("worker backend must support transfer verification")
    evidence = dict(
        verifier(
            target_device=int(target_device),
            direction=request.data_plane.direction,
            host_ptr=resources.host_ptr,
            host_bytes=resources.host_bytes,
            device_ptr=resources.device_ptr,
            device_bytes=resources.device_bytes,
            ranges=ranges,
        )
    )
    evidence.setdefault("verification_source", "cuda_worker")
    return evidence


def _request_skips_verification(request: WorkerTransferRequest) -> bool:
    return bool(request.ticket.metadata.get("skip_verification", False))


def _skipped_verification_evidence(
    *,
    expected_bytes: int,
    resource_evidence: dict[str, object],
    executor: str,
    path: str,
    target_device: int,
    direct_bytes: int,
    direct_chunks: int,
    relay_bytes: int,
    relay_chunks: int,
) -> dict[str, object]:
    return {
        "expected_bytes": int(expected_bytes),
        "verified_bytes": int(expected_bytes),
        "content_match": True,
        "verification_source": "benchmark_no_verify",
        "verification_method": "verification_skipped",
        "verification_skipped": True,
        "resource_evidence": dict(resource_evidence),
        "executor": str(executor),
        "plan_source": "daemon",
        "path": str(path),
        "target_device": int(target_device),
        "direct_bytes": int(direct_bytes),
        "direct_chunks": int(direct_chunks),
        "relay_bytes": int(relay_bytes),
        "relay_chunks": int(relay_chunks),
    }


def _plan_transfer_ranges(plan_payload: dict[str, object]) -> tuple[dict[str, int], ...]:
    ranges: list[dict[str, int]] = []
    for assignment in plan_payload.get("assignments", ()) or ():
        if not isinstance(assignment, dict):
            continue
        for chunk in assignment.get("chunks", ()) or ():
            if not isinstance(chunk, dict):
                continue
            ranges.append(
                {
                    "src_offset": int(chunk["src_offset"]),
                    "dst_offset": int(chunk["dst_offset"]),
                    "bytes": int(chunk["bytes"]),
                }
            )
    return tuple(ranges)


def _trace_cuda_worker_stage(name: str, **fields) -> None:
    if os.environ.get("TURBOBUS_BENCHMARK_TRACE") != "1":
        return
    details = " ".join(f"{key}={value}" for key, value in sorted(fields.items()))
    print(f"turbobus_cuda_worker_stage name={name} {details}".rstrip(), flush=True)


__all__ = ["CudaWorkerExecutor"]
