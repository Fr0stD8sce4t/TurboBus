from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict

from ..schema import (
    ExecutionTicket,
    LeaseToken,
    Session,
    TransferIntent,
    TransferReservation,
    TransferReceipt,
    TransferStatus,
    TransferStatusState,
)
from ..scheduler import (
    SchedulingDecision,
    scheduling_decision_leases,
    scheduling_decision_stats,
)


def decision_direction(decision: SchedulingDecision) -> str:
    for assignment in decision.plan.get("assignments", ()) or ():
        if not isinstance(assignment, dict):
            continue
        path = assignment.get("path")
        if not isinstance(path, dict):
            continue
        direction = str(path.get("direction", "")).lower()
        if direction in {"h2d", "d2h"}:
            return direction
    raise ValueError("scheduling decision plan has no direction")


def ticket_ranges_for_plan(
    plan: dict[str, object],
    *,
    direction: str,
) -> tuple[dict[str, int], ...]:
    if not isinstance(plan, dict):
        raise ValueError("transfer plan is unavailable")
    ranges: list[dict[str, int]] = []
    requested_direction = str(direction).lower()
    for assignment in plan.get("assignments", ()) or ():
        if not isinstance(assignment, dict):
            raise ValueError("transfer plan assignment must be an object")
        path = assignment.get("path")
        if not isinstance(path, dict):
            raise ValueError("transfer plan assignment path must be an object")
        if str(path.get("direction", "")).lower() != requested_direction:
            continue
        for chunk in assignment.get("chunks", ()) or ():
            if not isinstance(chunk, dict):
                raise ValueError("transfer plan chunk must be an object")
            ranges.append(
                {
                    "src_offset": int(chunk["src_offset"]),
                    "dst_offset": int(chunk["dst_offset"]),
                    "bytes": int(chunk["bytes"]),
                }
            )
    if not ranges:
        raise ValueError("daemon plan has no authorized chunks")
    return tuple(ranges)


def execution_ticket_for_plan(
    *,
    transfer_id: str,
    decision: SchedulingDecision,
    source_buffer_id: str,
    destination_buffer_id: str,
    now: float,
    plan_generation: int,
    default_expires_at: float,
    expires_at: float | None = None,
    lease_ids: tuple[str, ...] = (),
) -> ExecutionTicket:
    direction = decision_direction(decision)
    ticket_ranges = ticket_ranges_for_plan(decision.plan, direction=direction)
    resolved_expires_at = (
        float(expires_at) if expires_at is not None else float(default_expires_at)
    )
    if resolved_expires_at <= float(now):
        resolved_expires_at = float(default_expires_at)
    return ExecutionTicket(
        ticket_id=f"ticket-{transfer_id}",
        decision_id=decision.decision_id,
        intent_id=decision.intent_id,
        topology_snapshot_id=decision.topology_snapshot_id,
        job_id=decision.job_id,
        session_id=decision.session_id,
        source_buffer_id=source_buffer_id,
        destination_buffer_id=destination_buffer_id,
        direction=direction,
        total_bytes=sum(item["bytes"] for item in ticket_ranges),
        ranges=ticket_ranges,
        plan=dict(decision.plan),
        issued_at=float(now),
        expires_at=resolved_expires_at,
        lease_ids=lease_ids,
        metadata={
            "issuer": "turbobus-daemon",
            "transfer_id": transfer_id,
            "plan_generation": int(plan_generation),
        },
    )


def planned_transfer_payload(
    *,
    transfer_id: str,
    decision: SchedulingDecision,
    status: TransferStatus,
    session: Session,
    profile_key: str,
    profile_entry: Mapping[str, object] | None,
    relay_eligibility: dict[str, object],
    reservations: list[TransferReservation],
    admission: dict[str, object],
    plan_generation: int,
    plan_expires_at: float | None,
    lease_tokens: dict[str, LeaseToken],
    ticket: ExecutionTicket | None,
) -> dict[str, object]:
    payload = {
        "decision": asdict(decision),
        "decision_id": decision.decision_id,
        "topology_snapshot_id": decision.topology_snapshot_id,
        "plan": dict(decision.plan),
        "path_summary": list(decision.path_summary),
        "stats": scheduling_decision_stats(decision).as_dict(),
        "leases": [lease.as_dict() for lease in scheduling_decision_leases(decision)],
        "admission": dict(admission),
        "plan_generation": int(plan_generation),
        "plan_expires_at": plan_expires_at,
        "transfer_id": str(transfer_id),
        "transfer_status": asdict(status),
        "planning": {
            "target_gpu": session.target_gpu,
            "profile_key": profile_key,
            "profile_entry": (
                None if profile_entry is None else dict(profile_entry)
            ),
            "relay_eligibility": relay_eligibility,
        },
        "reservations": [asdict(reservation) for reservation in reservations],
    }
    payload["lease_tokens"] = [
        asdict(lease_tokens[reservation.reservation_id])
        for reservation in reservations
        if reservation.reservation_id in lease_tokens
    ]
    payload["ticket"] = None if ticket is None else asdict(ticket)
    return payload


def receipt_for_transfer(
    *,
    transfer_id: str,
    intent: TransferIntent,
    status: TransferStatus,
    decision: SchedulingDecision,
    ticket: ExecutionTicket | None,
    admission: dict[str, object],
    plan_generation: int,
    plan_expires_at: float | None,
    admitted_state: str,
    completion_source: str | None = None,
    completion_evidence: dict[str, object] | None = None,
    buffer_snapshots: Mapping[str, object] | None = None,
) -> TransferReceipt:
    error = status.error
    if status.state in {TransferStatusState.FAILED, TransferStatusState.CANCELED}:
        error = error or f"transfer {status.state.value}"
    evidence = dict(completion_evidence or {})
    evidence_ticket_id = evidence.get("ticket_id")
    evidence_transfer_id = evidence.get("transfer_id")
    evidence_plan_generation = _optional_int(evidence.get("plan_generation"))
    evidence_expected_bytes = _optional_int(evidence.get("expected_bytes"))
    resource_evidence = evidence.get("resource_evidence")
    execution_path_evidence = evidence.get("execution_path_evidence")
    direct_completion_evidence = evidence.get("direct_completion_evidence")
    relay_completion_evidence = evidence.get("relay_completion_evidence")
    cleanup_evidence = evidence.get("cleanup")
    buffer_lifetime_evidence = _buffer_lifetime_evidence(
        intent=intent,
        buffer_snapshots=buffer_snapshots,
        resource_evidence=resource_evidence,
        direct_completion_evidence=direct_completion_evidence,
        relay_completion_evidence=relay_completion_evidence,
        cleanup_evidence=cleanup_evidence,
    )
    completion_contract = _completion_contract_view(
        evidence=evidence,
        execution_path_evidence=execution_path_evidence,
        cleanup_evidence=cleanup_evidence,
        direct_completion_evidence=direct_completion_evidence,
        relay_completion_evidence=relay_completion_evidence,
    )
    return TransferReceipt(
        receipt_id=f"receipt-{transfer_id}",
        ticket_id=(
            ticket.ticket_id
            if ticket is not None
            else f"ticket-pending-{transfer_id}"
        ),
        intent_id=intent.intent_id,
        decision_id=decision.decision_id,
        topology_snapshot_id=decision.topology_snapshot_id,
        job_id=intent.job_id,
        session_id=intent.session_id,
        state=status.state,
        bytes_total=status.bytes_total,
        bytes_completed=status.bytes_completed,
        started_at=decision.issued_at,
        path_stats=decision.path_summary,
        error=error,
        metadata={
            "transfer_id": transfer_id,
            "fallback_reason": decision.fallback_reason,
            "admission_state": admission.get("state", admitted_state),
            "admission_reason": admission.get("reason"),
            "plan_generation": int(plan_generation),
            "plan_expires_at": plan_expires_at,
            "completion_source": completion_source,
            "executed": completion_source in {"worker", "backend"},
            "verified": bool(evidence.get("verified", False)),
            "verified_bytes": int(evidence.get("verified_bytes", 0) or 0),
            "evidence_expected_bytes": (
                None if evidence_expected_bytes is None else evidence_expected_bytes
            ),
            "content_match": bool(evidence.get("content_match", False)),
            "verification_source": evidence.get("verification_source"),
            "verification_method": evidence.get("verification_method"),
            "source_digest": evidence.get("source_digest"),
            "destination_digest": evidence.get("destination_digest"),
            "execution_ticket_id": None if ticket is None else ticket.ticket_id,
            "evidence_ticket_id": (
                None if evidence_ticket_id is None else str(evidence_ticket_id)
            ),
            "evidence_transfer_id": (
                None if evidence_transfer_id is None else str(evidence_transfer_id)
            ),
            "evidence_plan_generation": (
                None if evidence_plan_generation is None else evidence_plan_generation
            ),
            "resource_evidence": (
                dict(resource_evidence)
                if isinstance(resource_evidence, Mapping)
                else None
            ),
            "execution_path_evidence": (
                dict(execution_path_evidence)
                if isinstance(execution_path_evidence, Mapping)
                else None
            ),
            "direct_completion_evidence": (
                dict(direct_completion_evidence)
                if isinstance(direct_completion_evidence, Mapping)
                else None
            ),
            "relay_completion_evidence": (
                dict(relay_completion_evidence)
                if isinstance(relay_completion_evidence, Mapping)
                else None
            ),
            "completion_evidence": dict(evidence),
            "cleanup_evidence": (
                dict(cleanup_evidence)
                if isinstance(cleanup_evidence, Mapping)
                else None
            ),
            "completion_contract": completion_contract,
            "buffer_lifetime_evidence": buffer_lifetime_evidence,
        },
    )


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _buffer_lifetime_evidence(
    *,
    intent: TransferIntent,
    buffer_snapshots: Mapping[str, object] | None,
    resource_evidence: object,
    direct_completion_evidence: object,
    relay_completion_evidence: object,
    cleanup_evidence: object,
) -> dict[str, object]:
    snapshots = (
        {
            str(key): dict(value)
            for key, value in buffer_snapshots.items()
            if isinstance(value, Mapping)
        }
        if isinstance(buffer_snapshots, Mapping)
        else {}
    )
    resource_mapping = (
        dict(resource_evidence) if isinstance(resource_evidence, Mapping) else {}
    )
    direct_mapping = (
        dict(direct_completion_evidence)
        if isinstance(direct_completion_evidence, Mapping)
        else {}
    )
    relay_mapping = (
        dict(relay_completion_evidence)
        if isinstance(relay_completion_evidence, Mapping)
        else {}
    )
    cleanup_mapping = dict(cleanup_evidence) if isinstance(cleanup_evidence, Mapping) else {}
    if not cleanup_mapping:
        nested_cleanup = relay_mapping.get("cleanup")
        if isinstance(nested_cleanup, Mapping):
            cleanup_mapping = dict(nested_cleanup)
    if not cleanup_mapping:
        nested_cleanup = direct_mapping.get("cleanup")
        if isinstance(nested_cleanup, Mapping):
            cleanup_mapping = dict(nested_cleanup)
    return {
        "source_buffer": _buffer_lifetime_record(
            expected_buffer_id=intent.source_buffer_id,
            snapshot=snapshots.get("source"),
            resource_evidence=_buffer_resource_evidence_for_buffer(
                buffer_id=intent.source_buffer_id,
                resource_evidence=resource_mapping,
                direct_completion_evidence=direct_mapping,
                relay_completion_evidence=relay_mapping,
            ),
        ),
        "destination_buffer": _buffer_lifetime_record(
            expected_buffer_id=intent.destination_buffer_id,
            snapshot=snapshots.get("destination"),
            resource_evidence=_buffer_resource_evidence_for_buffer(
                buffer_id=intent.destination_buffer_id,
                resource_evidence=resource_mapping,
                direct_completion_evidence=direct_mapping,
                relay_completion_evidence=relay_mapping,
            ),
        ),
        "cleanup": cleanup_mapping or None,
    }


def _completion_contract_view(
    *,
    evidence: Mapping[str, object],
    execution_path_evidence: object,
    cleanup_evidence: object,
    direct_completion_evidence: object,
    relay_completion_evidence: object,
) -> dict[str, object]:
    contract = {
        "ticket_id": evidence.get("ticket_id"),
        "transfer_id": evidence.get("transfer_id"),
        "plan_generation": evidence.get("plan_generation"),
        "verification_source": evidence.get("verification_source"),
        "verification_method": evidence.get("verification_method"),
        "verified_bytes": evidence.get("verified_bytes"),
        "expected_bytes": evidence.get("expected_bytes"),
        "content_match": evidence.get("content_match"),
        "failure_source": evidence.get("failure_source"),
        "execution_path": (
            dict(execution_path_evidence)
            if isinstance(execution_path_evidence, Mapping)
            else _execution_path_view_from_evidence(evidence)
        ),
        "cleanup": (
            dict(cleanup_evidence)
            if isinstance(cleanup_evidence, Mapping)
            else _cleanup_view_from_nested_completion(
                direct_completion_evidence,
                relay_completion_evidence,
            )
        ),
        "direct": (
            dict(direct_completion_evidence)
            if isinstance(direct_completion_evidence, Mapping)
            else None
        ),
        "relay": (
            dict(relay_completion_evidence)
            if isinstance(relay_completion_evidence, Mapping)
            else None
        ),
    }
    return contract


def _execution_path_view_from_evidence(
    evidence: Mapping[str, object],
) -> dict[str, object] | None:
    view: dict[str, object] = {}
    for field_name in (
        "executor",
        "path",
        "plan_source",
        "target_device",
        "direct_bytes",
        "direct_chunks",
        "relay_bytes",
        "relay_chunks",
        "relay_gpu",
        "relay_gpus",
        "src_buffer_id",
        "dst_buffer_id",
        "staging_slot_id",
    ):
        if field_name in evidence and evidence[field_name] is not None:
            view[field_name] = evidence[field_name]
    return view or None


def _cleanup_view_from_nested_completion(
    direct_completion_evidence: object,
    relay_completion_evidence: object,
) -> dict[str, object] | None:
    for candidate in (relay_completion_evidence, direct_completion_evidence):
        if not isinstance(candidate, Mapping):
            continue
        cleanup = candidate.get("cleanup")
        if isinstance(cleanup, Mapping):
            return dict(cleanup)
    return None


def _buffer_lifetime_record(
    *,
    expected_buffer_id: str,
    snapshot: object,
    resource_evidence: Mapping[str, object] | None,
) -> dict[str, object]:
    record = {
        "buffer_id": str(expected_buffer_id),
        "registration": dict(snapshot) if isinstance(snapshot, Mapping) else None,
        "resource_evidence": (
            dict(resource_evidence) if isinstance(resource_evidence, Mapping) else None
        ),
    }
    if isinstance(snapshot, Mapping):
        metadata = snapshot.get("metadata")
        if isinstance(metadata, Mapping):
            record["runtime_session_id"] = metadata.get("runtime_session_id")
            record["runtime_owned"] = bool(metadata.get("runtime_owned", False))
            record["runtime_buffer_kind"] = metadata.get("runtime_buffer_kind")
    return record


def _buffer_resource_evidence_for_buffer(
    *,
    buffer_id: str,
    resource_evidence: Mapping[str, object],
    direct_completion_evidence: Mapping[str, object],
    relay_completion_evidence: Mapping[str, object],
) -> dict[str, object] | None:
    candidates: list[Mapping[str, object]] = []
    if resource_evidence:
        candidates.append(resource_evidence)
    for nested_key in ("resource_evidence",):
        nested = direct_completion_evidence.get(nested_key)
        if isinstance(nested, Mapping):
            candidates.append(nested)
        nested = relay_completion_evidence.get(nested_key)
        if isinstance(nested, Mapping):
            candidates.append(nested)
    nested_resource_roots = resource_evidence.get("direct"), resource_evidence.get("relay")
    for nested in nested_resource_roots:
        if isinstance(nested, Mapping):
            candidates.append(nested)
    merged: dict[str, object] = {}
    for candidate in candidates:
        if str(candidate.get("src_buffer_id", "")) == str(buffer_id):
            merged["role"] = "source"
            merged.update(dict(candidate))
        if str(candidate.get("dst_buffer_id", "")) == str(buffer_id):
            merged["role"] = "destination"
            merged.update(dict(candidate))
        if str(candidate.get("cpu_buffer_id", "")) == str(buffer_id):
            merged["handle_role"] = candidate.get("cpu_buffer_role")
            merged.update(dict(candidate))
        if str(candidate.get("device_buffer_id", "")) == str(buffer_id):
            merged["handle_role"] = candidate.get("device_buffer_role")
            merged.update(dict(candidate))
    return merged or None


__all__ = [
    "decision_direction",
    "execution_ticket_for_plan",
    "planned_transfer_payload",
    "receipt_for_transfer",
    "ticket_ranges_for_plan",
]
