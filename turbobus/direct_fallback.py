from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping

from .client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer
from .runtime_engine import RuntimeOptions
from .schema import ExecutionTicket
from .transfer import TransferRequest
from .transfer_execution import require_daemon_transfer_complete, require_ok


def is_direct_only_worker_plan(plan_payload: Mapping[str, object]) -> bool:
    if plan_payload.get("lease_tokens") or plan_payload.get("reservations"):
        return False
    plan = plan_payload.get("plan")
    if not isinstance(plan, Mapping):
        return False
    assignments = plan.get("assignments", ()) or ()
    if not assignments:
        return False
    for assignment in assignments:
        if not isinstance(assignment, Mapping):
            return False
        path = assignment.get("path")
        if not isinstance(path, Mapping):
            return False
        if str(path.get("kind", "")).lower() != "direct":
            return False
    return True


def execute_direct_fallback_transfer(
    *,
    daemon_client,
    backend,
    runtime_options: RuntimeOptions,
    transfer_request: TransferRequest,
    planned_payload: Mapping[str, object],
    session_id: str,
    job_id: str,
    source: SharedPinnedCpuBuffer | CudaIpcDeviceBuffer,
    target: SharedPinnedCpuBuffer | CudaIpcDeviceBuffer,
    result_factory: Callable[..., object],
):
    transfer_id = str(planned_payload["transfer_id"])
    try:
        ticket = _direct_ticket_from_planned_payload(
            planned_payload,
            transfer_request=transfer_request,
            transfer_id=transfer_id,
            job_id=job_id,
            source_buffer_id=source.buffer_id,
            target_buffer_id=target.buffer_id,
        )
        bytes_completed, completion_evidence = _execute_direct_ticket_plan(
            backend=backend,
            runtime_options=runtime_options,
            direction=transfer_request.direction.value,
            ticket=ticket,
            source=source,
            target=target,
        )
        if bytes_completed != ticket.total_bytes:
            raise RuntimeError(
                "direct fallback completed "
                f"{bytes_completed} of {ticket.total_bytes} daemon-ticketed bytes"
            )
    except Exception as exc:
        daemon_client.transfer_status(
            transfer_id,
            state="failed",
            bytes_completed=0,
            error=str(exc) or exc.__class__.__name__,
        )
        raise
    completed = daemon_client.transfer_status(
        transfer_id,
        state="complete",
        bytes_completed=bytes_completed,
        completion_source="backend",
        completion_evidence=_completion_evidence_with_ticket_binding(
            completion_evidence,
            ticket=ticket,
        ),
    )
    if not completed.ok:
        daemon_client.transfer_status(
            transfer_id,
            state="failed",
            bytes_completed=0,
            error=completed.error or "backend verification failed",
        )
    require_ok(completed, "daemon direct transfer completion update failed")
    status = daemon_client.transfer_status(transfer_id)
    require_ok(status, "daemon transfer status query failed")
    final_status = dict(status.payload["status"])
    require_daemon_transfer_complete(
        final_status,
        expected_bytes=ticket.total_bytes,
    )
    return result_factory(
        transfer_id=transfer_id,
        session_id=session_id,
        job_id=job_id,
        source_buffer_id=source.buffer_id,
        target_buffer_id=target.buffer_id,
        plan=planned_payload,
        lease_token=None,
        lease_tokens=(),
        authorization_request=None,
        worker_lifecycle=None,
        worker_completion=None,
        final_status=final_status,
    )


def _execute_direct_ticket_plan(
    *,
    backend,
    runtime_options: RuntimeOptions,
    direction: str,
    ticket: ExecutionTicket,
    source: SharedPinnedCpuBuffer | CudaIpcDeviceBuffer,
    target: SharedPinnedCpuBuffer | CudaIpcDeviceBuffer,
) -> tuple[int, dict[str, object]]:
    if not isinstance(ticket, ExecutionTicket):
        raise TypeError("direct fallback requires a daemon-issued ExecutionTicket")
    plan_payload = dict(ticket.plan)
    if direction == "h2d":
        if not isinstance(source, SharedPinnedCpuBuffer):
            raise TypeError("direct h2d source must be a SharedPinnedCpuBuffer")
        if not isinstance(target, CudaIpcDeviceBuffer):
            raise TypeError("direct h2d target must be a CudaIpcDeviceBuffer")
        _require_device_pointer(target)
        return _run_direct_plan(
            backend=backend,
            runtime_options=runtime_options,
            target_device=target.device_index,
            plan_payload=plan_payload,
            host_buffer=source,
            device_ptr=int(target.device_ptr),
            device_bytes=target.size_bytes,
            direction=direction,
        )
    if not isinstance(source, CudaIpcDeviceBuffer):
        raise TypeError("direct d2h source must be a CudaIpcDeviceBuffer")
    if not isinstance(target, SharedPinnedCpuBuffer):
        raise TypeError("direct d2h target must be a SharedPinnedCpuBuffer")
    _require_device_pointer(source)
    return _run_direct_plan(
        backend=backend,
        runtime_options=runtime_options,
        target_device=source.device_index,
        plan_payload=plan_payload,
        host_buffer=target,
        device_ptr=int(source.device_ptr),
        device_bytes=source.size_bytes,
        direction=direction,
    )


def _run_direct_plan(
    *,
    backend,
    runtime_options: RuntimeOptions,
    target_device: int,
    plan_payload: Mapping[str, object],
    host_buffer: SharedPinnedCpuBuffer,
    device_ptr: int,
    device_bytes: int,
    direction: str,
) -> tuple[int, dict[str, object]]:
    _require_direct_plan_matches_target(
        plan_payload,
        target_device=int(target_device),
        direction=direction,
        host_bytes=host_buffer.size_bytes,
        device_bytes=int(device_bytes),
    )
    _set_cuda_device_for_direct_plan(backend, int(target_device))
    native_plan = backend.make_transfer_plan(plan_payload)
    runtime = backend.create_runtime(runtime_options)
    backend.initialize_runtime(runtime, int(target_device), [])
    host_buffer.register_for_cuda(backend)
    host_ptr = host_buffer.address
    try:
        if direction == "h2d":
            handle = backend.fetch_plan_to_gpu(
                runtime,
                host_ptr,
                host_buffer.size_bytes,
                device_ptr,
                int(device_bytes),
                native_plan,
            )
        else:
            handle = backend.offload_plan_to_cpu(
                runtime,
                device_ptr,
                int(device_bytes),
                host_ptr,
                host_buffer.size_bytes,
                native_plan,
            )
        backend.wait(runtime, handle)
        stats = _direct_plan_stats(backend, runtime, handle)
        bytes_completed = _direct_plan_completed_bytes(
            stats,
            plan_payload=plan_payload,
        )
        return (
            bytes_completed,
            _direct_plan_completion_evidence(
                backend=backend,
                target_device=int(target_device),
                direction=direction,
                host_ptr=host_ptr,
                host_bytes=host_buffer.size_bytes,
                device_ptr=int(device_ptr),
                device_bytes=int(device_bytes),
                ranges=_plan_transfer_ranges(plan_payload),
                expected_bytes=int(plan_payload["total_bytes"]),
            ),
        )
    finally:
        host_buffer.unregister_from_cuda()


def _direct_ticket_from_planned_payload(
    planned_payload: Mapping[str, object],
    *,
    transfer_request: TransferRequest,
    transfer_id: str,
    job_id: str,
    source_buffer_id: str,
    target_buffer_id: str,
) -> ExecutionTicket:
    ticket_payload = planned_payload.get("ticket")
    if not isinstance(ticket_payload, Mapping):
        raise RuntimeError("daemon direct transfer did not include execution ticket")
    ticket = ExecutionTicket(**dict(ticket_payload))
    if ticket.metadata.get("issuer") != "turbobus-daemon":
        raise RuntimeError("daemon direct ticket was not issued by turbobus-daemon")
    plan_generation = planned_payload.get("plan_generation")
    if (
        plan_generation is not None
        and int(ticket.metadata.get("plan_generation", 0)) != int(plan_generation)
    ):
        raise RuntimeError("daemon direct ticket plan generation mismatch")
    if ticket.metadata.get("transfer_id") != str(transfer_id):
        raise RuntimeError("daemon direct ticket transfer mismatch")
    if ticket.job_id != str(job_id):
        raise RuntimeError("daemon direct ticket job mismatch")
    if ticket.source_buffer_id != str(source_buffer_id):
        raise RuntimeError("daemon direct ticket source buffer mismatch")
    if ticket.destination_buffer_id != str(target_buffer_id):
        raise RuntimeError("daemon direct ticket destination buffer mismatch")
    if ticket.direction != transfer_request.direction.value:
        raise RuntimeError("daemon direct ticket direction mismatch")
    if ticket.total_bytes != transfer_request.total_bytes:
        raise RuntimeError("daemon direct ticket byte total mismatch")
    if not _ticket_ranges_cover_transfer_request(ticket, transfer_request):
        raise RuntimeError("daemon direct ticket ranges mismatch")
    if dict(ticket.plan) != dict(planned_payload.get("plan") or {}):
        raise RuntimeError("daemon direct ticket plan mismatch")
    if ticket.lease_ids:
        raise RuntimeError("daemon direct ticket must not include relay leases")
    return ticket


def _ticket_ranges_cover_transfer_request(
    ticket: ExecutionTicket,
    transfer_request: TransferRequest,
) -> bool:
    request_ranges = tuple(item.as_dict() for item in transfer_request.ranges)
    if not request_ranges:
        return False
    ticket_total = 0
    for ticket_range in ticket.ranges:
        ticket_total += int(ticket_range["bytes"])
        if not any(
            _range_contains(request_range, ticket_range)
            for request_range in request_ranges
        ):
            return False
    return ticket_total == transfer_request.total_bytes


def _range_contains(
    outer: Mapping[str, int],
    inner: Mapping[str, int],
) -> bool:
    outer_src = int(outer["src_offset"])
    outer_dst = int(outer["dst_offset"])
    outer_bytes = int(outer["bytes"])
    inner_src = int(inner["src_offset"])
    inner_dst = int(inner["dst_offset"])
    inner_bytes = int(inner["bytes"])
    return (
        outer_src <= inner_src
        and outer_dst <= inner_dst
        and inner_src + inner_bytes <= outer_src + outer_bytes
        and inner_dst + inner_bytes <= outer_dst + outer_bytes
    )


def _set_cuda_device_for_direct_plan(backend, target_device: int) -> None:
    setter = getattr(backend, "set_device", None)
    if callable(setter):
        setter(int(target_device))


def _direct_plan_stats(
    backend,
    runtime,
    handle,
):
    statter = getattr(backend, "stats", None)
    if not callable(statter):
        return {}
    return statter(runtime, handle)


def _direct_plan_completed_bytes(
    stats,
    *,
    plan_payload: Mapping[str, object],
) -> int:
    bytes_value = _stats_value(stats, "bytes")
    if bytes_value is not None:
        return int(bytes_value)
    return int(plan_payload["total_bytes"])


def _direct_plan_completion_evidence(
    *,
    backend,
    target_device: int,
    direction: str,
    host_ptr: int,
    host_bytes: int,
    device_ptr: int,
    device_bytes: int,
    ranges: Iterable[Mapping[str, int]],
    expected_bytes: int,
) -> dict[str, object]:
    verifier = getattr(backend, "verify_transfer", None)
    if not callable(verifier):
        raise RuntimeError("direct backend must support transfer verification")
    evidence = dict(
        verifier(
            target_device=int(target_device),
            direction=str(direction).lower(),
            host_ptr=int(host_ptr),
            host_bytes=int(host_bytes),
            device_ptr=int(device_ptr),
            device_bytes=int(device_bytes),
            ranges=tuple(ranges),
        )
    )
    evidence.setdefault("expected_bytes", int(expected_bytes))
    return evidence


def _completion_evidence_with_ticket_binding(
    evidence: Mapping[str, object],
    *,
    ticket: ExecutionTicket,
) -> dict[str, object]:
    bound = dict(evidence)
    bound.setdefault("ticket_id", ticket.ticket_id)
    transfer_id = ticket.metadata.get("transfer_id")
    if transfer_id is not None:
        bound.setdefault("transfer_id", str(transfer_id))
    plan_generation = ticket.metadata.get("plan_generation")
    if plan_generation is not None:
        bound.setdefault("plan_generation", int(plan_generation))
    return bound


def _plan_transfer_ranges(plan_payload: Mapping[str, object]) -> tuple[dict[str, int], ...]:
    ranges: list[dict[str, int]] = []
    for assignment in plan_payload.get("assignments", ()) or ():
        if not isinstance(assignment, Mapping):
            continue
        for chunk in assignment.get("chunks", ()) or ():
            if not isinstance(chunk, Mapping):
                continue
            ranges.append(
                {
                    "src_offset": int(chunk["src_offset"]),
                    "dst_offset": int(chunk["dst_offset"]),
                    "bytes": int(chunk["bytes"]),
                }
            )
    return tuple(ranges)


def _stats_value(stats, field_name: str):
    value = getattr(stats, field_name, None)
    if value is None and isinstance(stats, Mapping):
        value = stats.get(field_name)
    return value


def _require_direct_plan_matches_target(
    plan_payload: Mapping[str, object],
    *,
    target_device: int,
    direction: str,
    host_bytes: int,
    device_bytes: int,
) -> None:
    assignments = plan_payload.get("assignments", ()) or ()
    if not assignments:
        raise RuntimeError("daemon direct plan has no assignments")
    expected_direction = str(direction).lower()
    if expected_direction == "h2d":
        src_size = int(host_bytes)
        dst_size = int(device_bytes)
    else:
        src_size = int(device_bytes)
        dst_size = int(host_bytes)
    found_chunks = False
    chunk_total = 0
    for assignment in assignments:
        if not isinstance(assignment, Mapping):
            raise RuntimeError("daemon direct plan assignment must be a mapping")
        path = assignment.get("path")
        if not isinstance(path, Mapping):
            raise RuntimeError("daemon direct plan assignment has no path")
        if str(path.get("kind", "")).lower() != "direct":
            raise RuntimeError("daemon direct fallback requires direct paths")
        if str(path.get("direction", "")).lower() != expected_direction:
            raise RuntimeError("daemon direct plan direction does not match request")
        if int(path.get("target_device", target_device)) != int(target_device):
            raise RuntimeError("daemon direct plan target does not match buffer device")
        if not bool(path.get("enabled", True)):
            raise RuntimeError("daemon direct plan path is disabled")
        for chunk in assignment.get("chunks", ()) or ():
            if not isinstance(chunk, Mapping):
                raise RuntimeError("daemon direct plan chunk must be a mapping")
            src_offset = int(chunk["src_offset"])
            dst_offset = int(chunk["dst_offset"])
            bytes_count = int(chunk["bytes"])
            if src_offset < 0 or dst_offset < 0:
                raise RuntimeError(
                    "daemon direct plan chunk offsets must be non-negative"
                )
            if bytes_count <= 0:
                raise RuntimeError("daemon direct plan chunk bytes must be positive")
            if src_offset + bytes_count > src_size:
                raise RuntimeError("daemon direct plan chunk exceeds source buffer")
            if dst_offset + bytes_count > dst_size:
                raise RuntimeError("daemon direct plan chunk exceeds destination buffer")
            chunk_total += bytes_count
            found_chunks = True
    if not found_chunks:
        raise RuntimeError("daemon direct plan has no chunk assignments")
    declared_total = int(plan_payload.get("total_bytes", chunk_total))
    if declared_total != chunk_total:
        raise RuntimeError(
            "daemon direct plan total bytes do not match assigned chunks"
        )


def _require_device_pointer(buffer: CudaIpcDeviceBuffer) -> None:
    if buffer.device_ptr is None or int(buffer.device_ptr) <= 0:
        raise ValueError("direct fallback requires a local CUDA device pointer")


__all__ = [
    "execute_direct_fallback_transfer",
    "is_direct_only_worker_plan",
]
