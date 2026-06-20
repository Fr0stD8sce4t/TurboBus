from __future__ import annotations

from collections.abc import Iterable, Mapping

from ..schema import (
    BufferRegistration,
    ExecutionTicket,
    SchedulingDecision,
    WorkerDataPlaneRequest,
    WorkerTransferAuthorization,
)


def validate_ticket_matches_buffers(
    ticket: ExecutionTicket,
    src_buffer: BufferRegistration,
    dst_buffer: BufferRegistration,
) -> None:
    if not isinstance(ticket, ExecutionTicket):
        raise TypeError("ticket must be an ExecutionTicket")
    if not isinstance(src_buffer, BufferRegistration):
        raise TypeError("src_buffer must be a BufferRegistration")
    if not isinstance(dst_buffer, BufferRegistration):
        raise TypeError("dst_buffer must be a BufferRegistration")
    if src_buffer.buffer_id != ticket.source_buffer_id:
        raise ValueError("ticket source buffer does not match worker buffer")
    if dst_buffer.buffer_id != ticket.destination_buffer_id:
        raise ValueError("ticket destination buffer does not match worker buffer")
    if src_buffer.job_id != ticket.job_id or dst_buffer.job_id != ticket.job_id:
        raise ValueError("ticket job does not match worker buffers")


def validate_ticket_matches_decision(
    ticket: ExecutionTicket,
    decision: SchedulingDecision,
) -> None:
    if ticket.decision_id != decision.decision_id:
        raise ValueError("ticket decision_id does not match scheduling decision")
    if ticket.intent_id != decision.intent_id:
        raise ValueError("ticket intent_id does not match scheduling decision")
    if ticket.topology_snapshot_id != decision.topology_snapshot_id:
        raise ValueError("ticket topology_snapshot_id does not match scheduling decision")
    if ticket.job_id != decision.job_id:
        raise ValueError("ticket job_id does not match scheduling decision")
    if ticket.session_id != decision.session_id:
        raise ValueError("ticket session_id does not match scheduling decision")
    if dict(ticket.plan) != dict(decision.plan):
        raise ValueError("ticket plan does not match scheduling decision")
    ticket_generation = ticket.metadata.get("plan_generation")
    decision_generation = decision.metadata.get("plan_generation")
    if (
        ticket_generation is not None
        and decision_generation is not None
        and int(ticket_generation) != int(decision_generation)
    ):
        raise ValueError("ticket plan_generation does not match scheduling decision")


def validate_daemon_issued_ticket(
    ticket: ExecutionTicket,
    *,
    plan_generation: object | None = None,
    now: float | None = None,
) -> None:
    if not isinstance(ticket, ExecutionTicket):
        raise TypeError("ticket must be an ExecutionTicket")
    issuer = str(ticket.metadata.get("issuer", ""))
    if issuer != "turbobus-daemon":
        raise ValueError("execution ticket must be issued by turbobus-daemon")
    generation = ticket.metadata.get("plan_generation")
    if generation is None:
        raise ValueError("execution ticket missing plan_generation")
    if int(generation) <= 0:
        raise ValueError("execution ticket plan_generation must be positive")
    if plan_generation is not None and int(plan_generation) != int(generation):
        raise ValueError("execution ticket plan_generation is stale")
    if now is not None and float(now) > float(ticket.expires_at):
        raise ValueError("execution ticket expired")
    transfer_id = ticket.metadata.get("transfer_id")
    if transfer_id is None or not str(transfer_id).strip():
        raise ValueError("execution ticket missing transfer_id")


def validate_ticket_matches_worker_request(
    ticket: ExecutionTicket,
    authorization: WorkerTransferAuthorization,
    data_plane: WorkerDataPlaneRequest | None = None,
) -> None:
    if ticket.job_id != authorization.job_id:
        raise ValueError("ticket job does not match worker authorization")
    if ticket.session_id != authorization.session_id:
        raise ValueError("ticket session does not match worker authorization")
    if ticket.source_buffer_id != authorization.src_buffer.buffer_id:
        raise ValueError("ticket source buffer does not match worker authorization")
    if ticket.destination_buffer_id != authorization.dst_buffer.buffer_id:
        raise ValueError("ticket destination buffer does not match worker authorization")
    if ticket.direction != authorization.direction:
        raise ValueError("ticket direction does not match worker authorization")
    if dict(ticket.plan) != authorization.plan:
        raise ValueError("ticket plan does not match worker authorization")
    metadata_relays = None
    if data_plane is not None:
        metadata_relays = data_plane.metadata.get("relay_gpus")
    relay_gpus = relay_gpus_for_ticket(
        ticket,
        relay_gpu=authorization.relay_gpu,
        relay_gpus=metadata_relays,
    )
    if authorization.relay_gpu not in relay_gpus:
        raise ValueError("ticket relay does not match worker authorization")
    authorized_ranges = relay_ranges_from_ticket_plan(
        ticket,
        relay_gpus=relay_gpus,
    )
    if authorized_ranges != authorization.ranges:
        raise ValueError("ticket ranges do not match worker authorization")


def relay_gpus_for_ticket(
    ticket: ExecutionTicket,
    relay_gpu: int | None = None,
    relay_gpus: Iterable[int] | None = None,
) -> tuple[int, ...]:
    relay_devices = []
    for assignment in ticket.plan.get("assignments", ()) or ():
        if not isinstance(assignment, Mapping):
            raise ValueError("ticket plan assignment must be an object")
        path = assignment.get("path")
        if not isinstance(path, Mapping):
            raise ValueError("ticket plan assignment path must be an object")
        if str(path.get("kind", "")).lower() == "relay":
            relay_devices.append(int(path.get("relay_device", -1)))
    planned_relays = tuple(sorted(set(relay_devices)))
    if not planned_relays:
        raise ValueError("worker ticket requires at least one relay path")
    requested_relays: tuple[int, ...] | None = None
    if relay_gpus is not None:
        requested_relays = tuple(sorted({int(gpu) for gpu in relay_gpus}))
        if not requested_relays:
            raise ValueError("worker ticket relay_gpus must not be empty")
    if relay_gpu is not None:
        relay = int(relay_gpu)
        if requested_relays is None:
            requested_relays = (relay,)
        elif relay not in requested_relays:
            raise ValueError("ticket relay does not match requested relay_gpus")
    if requested_relays is None:
        return planned_relays
    unknown_relays = sorted(set(requested_relays) - set(planned_relays))
    if unknown_relays:
        raise ValueError("ticket relay does not match daemon plan")
    return requested_relays


def lease_ids_for_ticket(
    ticket: ExecutionTicket,
    lease_id: str | None = None,
    lease_ids: Iterable[str] | None = None,
) -> tuple[str, ...]:
    requested_ids: tuple[str, ...] | None = None
    if lease_ids is not None:
        requested_ids = tuple(str(item) for item in lease_ids)
        if not requested_ids or any(not item.strip() for item in requested_ids):
            raise ValueError("worker ticket lease_ids must be non-empty")
    if lease_id is not None:
        resolved = str(lease_id)
        if requested_ids is None:
            requested_ids = (resolved,)
        elif resolved not in requested_ids:
            raise ValueError("worker lease_id does not match requested lease_ids")
    ticket_lease_ids = tuple(str(item) for item in ticket.lease_ids)
    if not ticket_lease_ids:
        raise ValueError("worker ticket requires at least one lease id")
    if requested_ids is None:
        return ticket_lease_ids
    unknown_ids = sorted(set(requested_ids) - set(ticket_lease_ids))
    if unknown_ids:
        raise ValueError("worker lease_id does not match ticket")
    return requested_ids


def cleanup_scope_lease_ids_for_ticket(
    ticket: ExecutionTicket,
    lease_id: str | None = None,
    lease_ids: Iterable[str] | None = None,
) -> tuple[str, ...]:
    owner_binding = owner_binding_for_ticket(
        ticket,
        lease_id=lease_id,
        lease_ids=lease_ids,
    )
    cleanup_scope = owner_binding["cleanup_scope"]
    if str(cleanup_scope["target_kind"]).lower() != "reservation":
        return tuple(owner_binding["lease_ids"])
    return tuple(str(item) for item in cleanup_scope["target_ids"])


def owner_binding_for_ticket(
    ticket: ExecutionTicket,
    *,
    transfer_id: str | None = None,
    job_id: str | None = None,
    session_id: str | None = None,
    lease_id: str | None = None,
    lease_ids: Iterable[str] | None = None,
    relay_gpu: int | None = None,
    relay_gpus: Iterable[int] | None = None,
) -> dict[str, object]:
    metadata = dict(ticket.metadata)
    owner_binding = metadata.get("owner_binding")
    if not isinstance(owner_binding, Mapping):
        raise ValueError("execution ticket missing owner_binding")

    resolved_transfer_id = transfer_id_for_ticket(ticket, transfer_id)
    resolved_job_id = str(ticket.job_id)
    if job_id is not None and str(job_id) != resolved_job_id:
        raise ValueError("ticket job does not match worker owner binding request")

    resolved_session_id = str(ticket.session_id)
    if session_id is not None and str(session_id) != resolved_session_id:
        raise ValueError("ticket session does not match worker owner binding request")

    resolved_lease_ids = lease_ids_for_ticket(
        ticket,
        lease_id=lease_id,
        lease_ids=lease_ids,
    )
    resolved_relays = relay_gpus_for_ticket(
        ticket,
        relay_gpu=relay_gpu,
        relay_gpus=relay_gpus,
    )
    return _validated_owner_binding(
        owner_binding,
        transfer_id=resolved_transfer_id,
        job_id=resolved_job_id,
        session_id=resolved_session_id,
        lease_ids=resolved_lease_ids,
        relay_gpus=resolved_relays,
    )


def owner_binding_for_payload(
    payload: Mapping[str, object],
    *,
    transfer_id: str,
    job_id: str,
    session_id: str,
    lease_id: str | None = None,
    lease_ids: Iterable[str] | None = None,
    relay_gpu: int | None = None,
    relay_gpus: Iterable[int] | None = None,
) -> dict[str, object]:
    if not isinstance(payload, Mapping):
        raise TypeError("payload must be a mapping")
    owner_binding = payload.get("owner_binding")
    if not isinstance(owner_binding, Mapping):
        raise ValueError("authorization payload missing owner_binding")
    resolved_lease_ids = _resolved_owner_binding_lease_ids(
        lease_id=lease_id,
        lease_ids=lease_ids,
    )
    resolved_relays = _resolved_owner_binding_relays(
        relay_gpu=relay_gpu,
        relay_gpus=relay_gpus,
    )
    return _validated_owner_binding(
        owner_binding,
        transfer_id=str(transfer_id),
        job_id=str(job_id),
        session_id=str(session_id),
        lease_ids=resolved_lease_ids,
        relay_gpus=resolved_relays,
    )


def transfer_id_for_ticket(ticket: ExecutionTicket, transfer_id: str | None) -> str:
    ticket_transfer_id = ticket.metadata.get("transfer_id")
    if ticket_transfer_id is None:
        return ticket.ticket_id
    resolved = str(ticket_transfer_id)
    if transfer_id is not None and str(transfer_id) != resolved:
        raise ValueError("transfer_id does not match execution ticket")
    return resolved


def _resolved_owner_binding_lease_ids(
    *,
    lease_id: str | None = None,
    lease_ids: Iterable[str] | None = None,
) -> tuple[str, ...]:
    requested_ids: tuple[str, ...] | None = None
    if lease_ids is not None:
        requested_ids = tuple(str(item) for item in lease_ids)
        if not requested_ids or any(not item.strip() for item in requested_ids):
            raise ValueError("worker owner binding lease_ids must be non-empty")
    if lease_id is not None:
        resolved = str(lease_id)
        if requested_ids is None:
            requested_ids = (resolved,)
        elif resolved not in requested_ids:
            raise ValueError("worker lease_id does not match requested lease_ids")
    if requested_ids is None:
        raise ValueError("worker owner binding lease_ids are unavailable")
    return requested_ids


def _resolved_owner_binding_relays(
    *,
    relay_gpu: int | None = None,
    relay_gpus: Iterable[int] | None = None,
) -> tuple[int, ...]:
    requested_relays: tuple[int, ...] | None = None
    if relay_gpus is not None:
        requested_relays = tuple(sorted({int(gpu) for gpu in relay_gpus}))
        if not requested_relays:
            raise ValueError("worker owner binding relay_gpus must not be empty")
    if relay_gpu is not None:
        relay = int(relay_gpu)
        if requested_relays is None:
            requested_relays = (relay,)
        elif relay not in requested_relays:
            raise ValueError("ticket relay does not match requested relay_gpus")
    if requested_relays is None:
        raise ValueError("worker owner binding relay_gpus are unavailable")
    return requested_relays


def _validated_owner_binding(
    owner_binding: Mapping[str, object],
    *,
    transfer_id: str,
    job_id: str,
    session_id: str,
    lease_ids: tuple[str, ...],
    relay_gpus: tuple[int, ...],
) -> dict[str, object]:
    binding_job_id = owner_binding.get("job_id")
    if binding_job_id is None or str(binding_job_id) != str(job_id):
        raise ValueError("worker owner binding job does not match ticket")
    binding_session_id = owner_binding.get("session_id")
    if binding_session_id is None or str(binding_session_id) != str(session_id):
        raise ValueError("worker owner binding session does not match ticket")
    binding_transfer_id = owner_binding.get("transfer_id")
    if binding_transfer_id is None or str(binding_transfer_id) != str(transfer_id):
        raise ValueError("worker owner binding transfer does not match ticket")
    binding_lease_ids = owner_binding.get("lease_ids")
    if not isinstance(binding_lease_ids, Iterable) or isinstance(
        binding_lease_ids,
        (str, bytes),
    ):
        raise ValueError("worker owner binding lease_ids must be iterable")
    normalized_binding_lease_ids = tuple(str(item) for item in binding_lease_ids)
    if (
        not normalized_binding_lease_ids
        or any(not item.strip() for item in normalized_binding_lease_ids)
    ):
        raise ValueError("worker owner binding lease_ids must be non-empty")
    if normalized_binding_lease_ids != tuple(str(item) for item in lease_ids):
        raise ValueError("worker owner binding lease_ids do not match ticket")
    binding_relays = owner_binding.get("relay_gpus")
    if not isinstance(binding_relays, Iterable) or isinstance(binding_relays, (str, bytes)):
        raise ValueError("worker owner binding relay_gpus must be iterable")
    normalized_binding_relays = tuple(sorted({int(item) for item in binding_relays}))
    if not normalized_binding_relays:
        raise ValueError("worker owner binding relay_gpus must not be empty")
    if normalized_binding_relays != tuple(sorted({int(item) for item in relay_gpus})):
        raise ValueError("worker owner binding relay_gpus do not match ticket")
    cleanup_scope = owner_binding.get("cleanup_scope")
    if not isinstance(cleanup_scope, Mapping):
        raise ValueError("worker owner binding missing cleanup_scope")
    cleanup_target_kind = str(cleanup_scope.get("target_kind", "")).lower()
    if cleanup_target_kind != "reservation":
        raise ValueError("worker owner binding cleanup_scope must target reservations")
    cleanup_target_ids = cleanup_scope.get("target_ids")
    if not isinstance(cleanup_target_ids, Iterable) or isinstance(
        cleanup_target_ids,
        (str, bytes),
    ):
        raise ValueError("worker owner binding cleanup_scope target_ids must be iterable")
    normalized_cleanup_target_ids = tuple(str(item) for item in cleanup_target_ids)
    if (
        not normalized_cleanup_target_ids
        or any(not item.strip() for item in normalized_cleanup_target_ids)
    ):
        raise ValueError("worker owner binding cleanup_scope target_ids must be non-empty")
    if normalized_cleanup_target_ids != normalized_binding_lease_ids:
        raise ValueError("worker owner binding cleanup_scope does not match lease_ids")
    result = {
        "job_id": str(job_id),
        "session_id": str(session_id),
        "transfer_id": str(transfer_id),
        "lease_ids": normalized_binding_lease_ids,
        "relay_gpus": normalized_binding_relays,
        "cleanup_scope": {
            "target_kind": cleanup_target_kind,
            "target_ids": normalized_cleanup_target_ids,
        },
    }
    peer_identity = owner_binding.get("peer_identity")
    if isinstance(peer_identity, Mapping):
        result["peer_identity"] = dict(peer_identity)
    return result


def relay_ranges_from_ticket_plan(
    ticket: ExecutionTicket,
    *,
    relay_gpus: Iterable[int],
) -> tuple[dict[str, int], ...]:
    ranges: list[dict[str, int]] = []
    authorized_relays = {int(gpu) for gpu in relay_gpus}
    if not authorized_relays:
        raise ValueError("ticket relay_gpus must not be empty")
    for assignment in ticket.plan.get("assignments", ()) or ():
        if not isinstance(assignment, Mapping):
            raise ValueError("ticket plan assignment must be an object")
        path = assignment.get("path")
        if not isinstance(path, Mapping):
            raise ValueError("ticket plan assignment path must be an object")
        if str(path.get("kind", "")).lower() != "relay":
            continue
        if int(path.get("relay_device", -1)) not in authorized_relays:
            continue
        if str(path.get("direction", "")).lower() != ticket.direction:
            raise ValueError("ticket plan direction does not match ticket")
        for chunk in assignment.get("chunks", ()) or ():
            if not isinstance(chunk, Mapping):
                raise ValueError("ticket plan chunk must be an object")
            ranges.append(
                {
                    "src_offset": int(chunk["src_offset"]),
                    "dst_offset": int(chunk["dst_offset"]),
                    "bytes": int(chunk["bytes"]),
                }
            )
    if not ranges:
        raise ValueError("ticket plan has no authorized relay chunks")
    relay_bytes = sum(item["bytes"] for item in ranges)
    if relay_bytes > ticket.total_bytes:
        raise ValueError("ticket relay ranges exceed ticket total bytes")
    return tuple(ranges)


def execution_ranges_from_ticket_plan(
    ticket: ExecutionTicket,
) -> tuple[dict[str, int], ...]:
    ranges: list[dict[str, int]] = []
    for assignment in ticket.plan.get("assignments", ()) or ():
        if not isinstance(assignment, Mapping):
            raise ValueError("ticket plan assignment must be an object")
        path = assignment.get("path")
        if not isinstance(path, Mapping):
            raise ValueError("ticket plan assignment path must be an object")
        if str(path.get("direction", "")).lower() != ticket.direction:
            raise ValueError("ticket plan direction does not match ticket")
        path_kind = str(path.get("kind", "")).lower()
        if path_kind not in {"direct", "relay"}:
            raise ValueError("ticket plan path must be direct or relay")
        for chunk in assignment.get("chunks", ()) or ():
            if not isinstance(chunk, Mapping):
                raise ValueError("ticket plan chunk must be an object")
            ranges.append(
                {
                    "src_offset": int(chunk["src_offset"]),
                    "dst_offset": int(chunk["dst_offset"]),
                    "bytes": int(chunk["bytes"]),
                }
            )
    if not ranges:
        raise ValueError("ticket plan has no executable chunks")
    range_bytes = sum(item["bytes"] for item in ranges)
    if range_bytes != ticket.total_bytes:
        raise ValueError("ticket plan ranges do not match ticket total bytes")
    if tuple(ranges) != ticket.ranges:
        raise ValueError("ticket ranges do not match daemon plan")
    return tuple(ranges)


def relay_ranges_by_gpu_for_ticket(
    ticket: ExecutionTicket,
    *,
    relay_gpus: Iterable[int],
) -> dict[int, tuple[dict[str, int], ...]]:
    return {
        int(relay): relay_ranges_from_ticket_plan(ticket, relay_gpus=(int(relay),))
        for relay in relay_gpus
    }


def authorized_relay_gpus_for_request(request) -> tuple[int, ...]:
    metadata_relays = request.data_plane.metadata.get("relay_gpus")
    if metadata_relays is None:
        return (int(request.data_plane.relay_gpu),)
    relays = tuple(sorted({int(gpu) for gpu in metadata_relays}))
    if not relays:
        raise ValueError("worker request has no authorized relay GPUs")
    if int(request.data_plane.relay_gpu) not in relays:
        raise ValueError("worker request primary relay is not authorized")
    return relays


__all__ = [
    "authorized_relay_gpus_for_request",
    "cleanup_scope_lease_ids_for_ticket",
    "execution_ranges_from_ticket_plan",
    "lease_ids_for_ticket",
    "owner_binding_for_payload",
    "owner_binding_for_ticket",
    "relay_gpus_for_ticket",
    "relay_ranges_by_gpu_for_ticket",
    "relay_ranges_from_ticket_plan",
    "transfer_id_for_ticket",
    "validate_daemon_issued_ticket",
    "validate_ticket_matches_buffers",
    "validate_ticket_matches_decision",
    "validate_ticket_matches_worker_request",
]
