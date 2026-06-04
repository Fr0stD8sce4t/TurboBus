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
        },
    )


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "decision_direction",
    "execution_ticket_for_plan",
    "planned_transfer_payload",
    "receipt_for_transfer",
    "ticket_ranges_for_plan",
]
