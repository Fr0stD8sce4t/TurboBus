from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Mapping

from .client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer
from .intent_execution_support import require_daemon_transfer_complete, require_ok
from .profiling.daemon_format import profile_from_daemon_entry
from .runtime_options import RuntimeOptions
from .schema import ExecutionTicket, TransferIntent


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
    intent: TransferIntent,
    planned_payload: Mapping[str, object],
    source: SharedPinnedCpuBuffer | CudaIpcDeviceBuffer,
    target: SharedPinnedCpuBuffer | CudaIpcDeviceBuffer,
    result_factory: Callable[..., object],
):
    transfer_id = str(planned_payload["transfer_id"])
    ticket: ExecutionTicket | None = None
    try:
        ticket = _direct_ticket_from_planned_payload(
            planned_payload,
            intent=intent,
            transfer_id=transfer_id,
            job_id=intent.job_id,
            source_buffer_id=source.buffer_id,
            target_buffer_id=target.buffer_id,
        )
        bytes_completed, completion_evidence = _execute_direct_ticket_plan(
            backend=backend,
            runtime_options=runtime_options,
            direction=intent.direction,
            ticket=ticket,
            planned_payload=planned_payload,
            source=source,
            target=target,
        )
        if bytes_completed != ticket.total_bytes:
            raise RuntimeError(
                "direct fallback completed "
                f"{bytes_completed} of {ticket.total_bytes} daemon-ticketed bytes"
            )
    except Exception as exc:
        failure_payload = {"failure_source": "direct_fallback"}
        failure_resource_evidence = _direct_endpoint_resource_evidence(
            direction=intent.direction,
            source=source,
            target=target,
        )
        if failure_resource_evidence is not None and ticket is not None:
            failure_resource_evidence = _resource_evidence_with_ticket_binding(
                failure_resource_evidence,
                ticket=ticket,
            )
        if failure_resource_evidence is not None:
            failure_payload["resource_evidence"] = failure_resource_evidence
        failure_evidence = (
            None
            if ticket is None
            else _completion_evidence_with_ticket_binding(
                failure_payload,
                ticket=ticket,
            )
        )
        daemon_client.transfer_status(
            transfer_id,
            state="failed",
            bytes_completed=0,
            error=str(exc) or exc.__class__.__name__,
            completion_source=None if ticket is None else "backend",
            completion_evidence=failure_evidence,
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
        completion_failure_resource_evidence = _direct_endpoint_resource_evidence(
            direction=intent.direction,
            source=source,
            target=target,
        )
        if completion_failure_resource_evidence is not None:
            completion_failure_resource_evidence = _resource_evidence_with_ticket_binding(
                completion_failure_resource_evidence,
                ticket=ticket,
            )
        daemon_client.transfer_status(
            transfer_id,
            state="failed",
            bytes_completed=0,
            error=completed.error or "backend verification failed",
            completion_source="backend",
            completion_evidence=_completion_evidence_with_ticket_binding(
                {
                    "failure_source": "direct_fallback",
                    **(
                        {}
                        if completion_failure_resource_evidence is None
                        else {
                            "resource_evidence": completion_failure_resource_evidence
                        }
                    ),
                },
                ticket=ticket,
            ),
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
        session_id=intent.session_id,
        job_id=intent.job_id,
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


def execute_direct_ticket_plan(
    *,
    backend,
    runtime_options: RuntimeOptions,
    intent: TransferIntent,
    planned_payload: Mapping[str, object],
    source: SharedPinnedCpuBuffer | CudaIpcDeviceBuffer,
    target: SharedPinnedCpuBuffer | CudaIpcDeviceBuffer,
) -> tuple[int, dict[str, object]]:
    transfer_id = str(planned_payload["transfer_id"])
    ticket = _direct_ticket_from_planned_payload(
        planned_payload,
        intent=intent,
        transfer_id=transfer_id,
        job_id=intent.job_id,
        source_buffer_id=source.buffer_id,
        target_buffer_id=target.buffer_id,
        allow_relay_leases=True,
    )
    bytes_completed, completion_evidence = _execute_direct_ticket_plan(
        backend=backend,
        runtime_options=runtime_options,
        direction=intent.direction,
        ticket=ticket,
        planned_payload=planned_payload,
        source=source,
        target=target,
    )
    return (
        bytes_completed,
        _completion_evidence_with_ticket_binding(
            completion_evidence,
            ticket=ticket,
        ),
    )


def _execute_direct_ticket_plan(
    *,
    backend,
    runtime_options: RuntimeOptions,
    direction: str,
    ticket: ExecutionTicket,
    planned_payload: Mapping[str, object],
    source: SharedPinnedCpuBuffer | CudaIpcDeviceBuffer,
    target: SharedPinnedCpuBuffer | CudaIpcDeviceBuffer,
) -> tuple[int, dict[str, object]]:
    if not isinstance(ticket, ExecutionTicket):
        raise TypeError("direct fallback requires a daemon-issued ExecutionTicket")
    plan_payload = _direct_scoped_plan_payload(ticket.plan)
    planning = planned_payload.get("planning")
    if isinstance(planning, Mapping):
        plan_payload["planning"] = dict(planning)
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
            ticket=ticket,
            host_buffer=source,
            source=source,
            target=target,
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
        ticket=ticket,
        host_buffer=target,
        source=source,
        target=target,
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
    ticket: ExecutionTicket,
    host_buffer: SharedPinnedCpuBuffer,
    source: SharedPinnedCpuBuffer | CudaIpcDeviceBuffer,
    target: SharedPinnedCpuBuffer | CudaIpcDeviceBuffer,
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
    _install_daemon_profile_if_available(
        backend=backend,
        runtime=runtime,
        plan_payload=plan_payload,
        target_device=int(target_device),
    )
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
                resource_evidence=_direct_resource_evidence(
                    direction=direction,
                    source=source,
                    target=target,
                    host_ptr=host_ptr,
                    host_bytes=host_buffer.size_bytes,
                    device_ptr=int(device_ptr),
                    device_bytes=int(device_bytes),
                    target_device=int(target_device),
                    ticket=ticket,
                ),
            ),
        )
    finally:
        host_buffer.unregister_from_cuda()


def _direct_ticket_from_planned_payload(
    planned_payload: Mapping[str, object],
    *,
    intent: TransferIntent,
    transfer_id: str,
    job_id: str,
    source_buffer_id: str,
    target_buffer_id: str,
    allow_relay_leases: bool = False,
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
    ticket_generation = ticket.metadata.get("plan_generation")
    if ticket_generation is None:
        raise RuntimeError("daemon direct ticket missing plan generation")
    if int(ticket_generation) <= 0:
        raise RuntimeError("daemon direct ticket plan generation must be positive")
    if time.time() > float(ticket.expires_at):
        raise RuntimeError("daemon direct ticket expired")
    if ticket.metadata.get("transfer_id") != str(transfer_id):
        raise RuntimeError("daemon direct ticket transfer mismatch")
    if ticket.job_id != str(job_id):
        raise RuntimeError("daemon direct ticket job mismatch")
    if ticket.source_buffer_id != str(source_buffer_id):
        raise RuntimeError("daemon direct ticket source buffer mismatch")
    if ticket.destination_buffer_id != str(target_buffer_id):
        raise RuntimeError("daemon direct ticket destination buffer mismatch")
    if ticket.direction != intent.direction:
        raise RuntimeError("daemon direct ticket direction mismatch")
    if ticket.total_bytes != intent.total_bytes:
        raise RuntimeError("daemon direct ticket byte total mismatch")
    if not _ticket_ranges_cover_intent(ticket, intent):
        raise RuntimeError("daemon direct ticket ranges mismatch")
    if dict(ticket.plan) != dict(planned_payload.get("plan") or {}):
        raise RuntimeError("daemon direct ticket plan mismatch")
    if ticket.lease_ids and not allow_relay_leases:
        raise RuntimeError("daemon direct ticket must not include relay leases")
    return ticket


def _ticket_ranges_cover_intent(
    ticket: ExecutionTicket,
    intent: TransferIntent,
) -> bool:
    request_ranges = tuple(dict(item) for item in intent.ranges)
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
    return ticket_total == intent.total_bytes


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
    resource_evidence: Mapping[str, object],
) -> dict[str, object]:
    verifier = getattr(backend, "verify_transfer", None)
    if not callable(verifier):
        raise RuntimeError("direct backend must support transfer verification")
    normalized_ranges = tuple(ranges)
    evidence = dict(
        verifier(
            target_device=int(target_device),
            direction=str(direction).lower(),
            host_ptr=int(host_ptr),
            host_bytes=int(host_bytes),
            device_ptr=int(device_ptr),
            device_bytes=int(device_bytes),
            ranges=normalized_ranges,
        )
    )
    evidence.setdefault("expected_bytes", int(expected_bytes))
    evidence.setdefault("resource_evidence", dict(resource_evidence))
    evidence.setdefault("executor", "direct_backend")
    evidence.setdefault("plan_source", "daemon")
    evidence.setdefault("path", f"direct_{str(direction).lower()}")
    evidence.setdefault("target_device", int(target_device))
    evidence.setdefault("direct_bytes", int(expected_bytes))
    evidence.setdefault("direct_chunks", len(normalized_ranges))
    evidence.setdefault("relay_bytes", 0)
    evidence.setdefault("relay_chunks", 0)
    return evidence


def _direct_resource_evidence(
    *,
    direction: str,
    source: SharedPinnedCpuBuffer | CudaIpcDeviceBuffer,
    target: SharedPinnedCpuBuffer | CudaIpcDeviceBuffer,
    host_ptr: int,
    host_bytes: int,
    device_ptr: int,
    device_bytes: int,
    target_device: int,
    ticket: ExecutionTicket,
) -> dict[str, object]:
    endpoint_evidence = _direct_endpoint_resource_evidence(
        direction=direction,
        source=source,
        target=target,
    )
    evidence = {} if endpoint_evidence is None else dict(endpoint_evidence)
    evidence.update(
        {
            "evidence_source": "direct_fallback_resources",
            "host_ptr": int(host_ptr),
            "host_bytes": int(host_bytes),
            "device_ptr": int(device_ptr),
            "device_bytes": int(device_bytes),
            "device_index": int(target_device),
            "cuda_host_registered": True,
        }
    )
    return _resource_evidence_with_ticket_binding(evidence, ticket=ticket)


def _direct_endpoint_resource_evidence(
    *,
    direction: str,
    source: SharedPinnedCpuBuffer | CudaIpcDeviceBuffer,
    target: SharedPinnedCpuBuffer | CudaIpcDeviceBuffer,
) -> dict[str, object] | None:
    normalized_direction = str(direction).lower()
    if normalized_direction == "h2d":
        if not isinstance(source, SharedPinnedCpuBuffer) or not isinstance(
            target,
            CudaIpcDeviceBuffer,
        ):
            return None
        cpu_buffer = source
        device_buffer = target
        cpu_role = "source"
        device_role = "destination"
    elif normalized_direction == "d2h":
        if not isinstance(source, CudaIpcDeviceBuffer) or not isinstance(
            target,
            SharedPinnedCpuBuffer,
        ):
            return None
        cpu_buffer = target
        device_buffer = source
        cpu_role = "destination"
        device_role = "source"
    else:
        return None
    return {
        "direction": normalized_direction,
        "src_buffer_id": source.buffer_id,
        "src_handle_type": _direct_handle_type(source),
        "dst_buffer_id": target.buffer_id,
        "dst_handle_type": _direct_handle_type(target),
        "cpu_buffer_id": cpu_buffer.buffer_id,
        "cpu_handle_type": "shared_pinned_cpu",
        "cpu_buffer_role": cpu_role,
        "device_buffer_id": device_buffer.buffer_id,
        "device_handle_type": "cuda_ipc_device",
        "device_buffer_role": device_role,
        "device_index": int(device_buffer.device_index),
    }


def _direct_handle_type(
    buffer: SharedPinnedCpuBuffer | CudaIpcDeviceBuffer,
) -> str:
    if isinstance(buffer, SharedPinnedCpuBuffer):
        return "shared_pinned_cpu"
    if isinstance(buffer, CudaIpcDeviceBuffer):
        return "cuda_ipc_device"
    return "unknown"


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


def _resource_evidence_with_ticket_binding(
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
    bound.setdefault("source_buffer_id", ticket.source_buffer_id)
    bound.setdefault("destination_buffer_id", ticket.destination_buffer_id)
    bound.setdefault("ticket_job_id", ticket.job_id)
    bound.setdefault("ticket_session_id", ticket.session_id)
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


def _direct_scoped_plan_payload(plan_payload: Mapping[str, object]) -> dict[str, object]:
    source_plan = dict(plan_payload)
    assignments: list[dict[str, object]] = []
    total_bytes = 0
    for assignment in source_plan.get("assignments", ()) or ():
        if not isinstance(assignment, Mapping):
            raise RuntimeError("daemon direct plan assignment must be a mapping")
        path = assignment.get("path")
        if not isinstance(path, Mapping):
            raise RuntimeError("daemon direct plan assignment has no path")
        if str(path.get("kind", "")).lower() != "direct":
            continue
        chunks: list[dict[str, int]] = []
        for chunk in assignment.get("chunks", ()) or ():
            if not isinstance(chunk, Mapping):
                raise RuntimeError("daemon direct plan chunk must be a mapping")
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
        assignments.append(
            {
                "path": dict(path),
                "chunks": chunks,
                "bytes": chunk_bytes,
                "chunk_count": len(chunks),
            }
        )
    if not assignments:
        raise RuntimeError("daemon direct plan has no direct assignments")
    return {
        "total_bytes": total_bytes,
        "chunk_bytes": int(source_plan.get("chunk_bytes", total_bytes)),
        "assignments": assignments,
    }


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
        path_kind = str(path.get("kind", "")).lower()
        if path_kind == "relay":
            continue
        if path_kind != "direct":
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


def _install_daemon_profile_if_available(
    *,
    backend,
    runtime,
    plan_payload: Mapping[str, object],
    target_device: int,
) -> None:
    planning = plan_payload.get("planning")
    if not isinstance(planning, Mapping):
        return
    profile_entry = planning.get("profile_entry")
    if not isinstance(profile_entry, Mapping):
        return
    profile = profile_from_daemon_entry(profile_entry, int(target_device))
    setter = getattr(backend, "set_cached_profile", None)
    if not callable(setter):
        raise RuntimeError("CUDA backend does not support cached profile installation")
    setter(runtime, profile)


__all__ = [
    "execute_direct_ticket_plan",
    "execute_direct_fallback_transfer",
    "is_direct_only_worker_plan",
]
