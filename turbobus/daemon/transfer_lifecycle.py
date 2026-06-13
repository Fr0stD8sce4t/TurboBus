from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import asdict

from ..schema import (
    ExecutionTicket,
    PeerIdentity,
    TransferIntent,
    TransferReceipt,
    TransferStatus,
    TransferStatusState,
)
from ..scheduler import SchedulingDecision


def terminal_status_payload(
    *,
    status: TransferStatus,
    removed: Mapping[str, object],
    promoted_transfers: Iterable[Mapping[str, object]],
) -> dict[str, object]:
    return {
        "status": asdict(status),
        "removed": dict(removed),
        "promoted_transfers": promoted_transfers,
    }


def status_persistence_actions(
    *,
    status: TransferStatus,
    completion_evidence: Mapping[str, object] | None,
    completion_ticket: ExecutionTicket | None,
) -> dict[str, object]:
    failed_or_canceled = status.state in {
        TransferStatusState.FAILED,
        TransferStatusState.CANCELED,
    }
    completed = status.state is TransferStatusState.COMPLETE
    return {
        "store_completion_ticket": bool(
            completion_ticket is not None and (failed_or_canceled or completed)
        ),
        "mark_admission_terminal": failed_or_canceled,
        "drop_active_ticket": bool(
            failed_or_canceled or (completed and completion_ticket is not None)
        ),
        "store_completion_source": bool(
            completed or completion_evidence is not None
        ),
        "merge_completion_evidence": completion_evidence is not None,
        "record_terminal_feedback": status.state
        in {
            TransferStatusState.COMPLETE,
            TransferStatusState.FAILED,
            TransferStatusState.CANCELED,
        },
    }


def terminal_finalization_plan(status: TransferStatus) -> dict[str, object]:
    if status.state is TransferStatusState.COMPLETE:
        return {
            "event_type": "worker_completion",
            "reason": None,
            "failure_reason": None,
            "retire_reason": "worker_complete",
            "release_reservations": False,
            "refresh_admission": "if_transfer_removed",
            "record_failure_cleanup_contract": False,
        }
    if status.state is TransferStatusState.FAILED:
        reason = status.error or "worker_failed"
        return {
            "event_type": "worker_failure",
            "reason": reason,
            "failure_reason": reason,
            "retire_reason": reason,
            "release_reservations": True,
            "refresh_admission": "always",
            "record_failure_cleanup_contract": True,
        }
    if status.state is TransferStatusState.CANCELED:
        reason = status.error or "transfer_canceled"
        return {
            "event_type": "transfer_canceled",
            "reason": reason,
            "failure_reason": reason,
            "retire_reason": reason,
            "release_reservations": True,
            "refresh_admission": "always",
            "record_failure_cleanup_contract": True,
        }
    return {
        "event_type": None,
        "reason": None,
        "failure_reason": None,
        "retire_reason": None,
        "release_reservations": False,
        "refresh_admission": "never",
        "record_failure_cleanup_contract": False,
    }


def failure_cleanup_contract(
    *,
    transfer_id: str,
    final_state: TransferStatusState,
    error: str,
    removed: Mapping[str, object],
    promoted_transfers: Iterable[Mapping[str, object]],
    recorded_at: float,
    active_ticket_retained: bool,
    active_reservation_count: int,
    active_staging_count: int,
) -> dict[str, object]:
    return {
        "source": "daemon_failure_cleanup_contract",
        "transfer_id": str(transfer_id),
        "final_state": final_state.value,
        "error": str(error),
        "removed": {
            str(key): int(value)
            for key, value in dict(removed).items()
            if value is not None
        },
        "promoted_transfers": [
            dict(item) for item in promoted_transfers if isinstance(item, Mapping)
        ],
        "recorded_at": float(recorded_at),
        "active_ticket_retained": bool(active_ticket_retained),
        "active_reservation_count": int(active_reservation_count),
        "active_staging_count": int(active_staging_count),
    }


def archive_record(
    *,
    transfer_id: str,
    existing: Mapping[str, object],
    request: Mapping[str, object],
    intent: TransferIntent,
    status: TransferStatus,
    decision: SchedulingDecision,
    ticket: ExecutionTicket | None,
    admission: Mapping[str, object],
    plan_generation: int,
    plan_expires_at: float | None,
    completion_source: str | None,
    completion_evidence: Mapping[str, object] | None,
    block_runtime: Iterable[Mapping[str, object]],
    buffer_snapshots: Mapping[str, object],
    queue_record: Mapping[str, object],
    reservations: Iterable[Mapping[str, object]],
    leases: Iterable[Mapping[str, object]],
    peer_identity: PeerIdentity | Mapping[str, object] | None,
) -> dict[str, object]:
    intent_id = request.get("intent_id")
    if intent_id is None:
        intent_id = existing.get("intent_id")
    return {
        "transfer_id": str(transfer_id),
        "intent_id": str(intent_id) if intent_id is not None else None,
        "intent": intent,
        "status": status,
        "decision": decision,
        "ticket": ticket,
        "admission": dict(admission),
        "plan_generation": int(plan_generation),
        "plan_expires_at": plan_expires_at,
        "completion_source": completion_source,
        "completion_evidence": dict(completion_evidence or {}),
        "block_runtime": tuple(
            dict(record) for record in block_runtime if isinstance(record, Mapping)
        ),
        "buffer_snapshots": dict(buffer_snapshots),
        "queue_record": dict(queue_record),
        "reservations": tuple(
            dict(record) for record in reservations if isinstance(record, Mapping)
        ),
        "leases": tuple(
            dict(record) for record in leases if isinstance(record, Mapping)
        ),
        "peer_identity": peer_identity,
    }


def merge_archive_record(
    *,
    existing: Mapping[str, object],
    record: Mapping[str, object],
) -> dict[str, object]:
    updated = dict(existing)
    updated.update(dict(record))
    return updated


def recovery_state(
    *,
    transfer_id: str,
    status: TransferStatus,
    archived: Mapping[str, object],
    admission: Mapping[str, object],
    queue_record: Mapping[str, object],
    block_runtime: Iterable[Mapping[str, object]],
    ticket: ExecutionTicket | None,
    reservations: Iterable[Mapping[str, object]],
    leases: Iterable[Mapping[str, object]],
    buffer_snapshots: Mapping[str, object],
    cleanup_targets: Iterable[Mapping[str, object]],
    receipt: TransferReceipt | None,
    completion_source: str | None,
    completion_evidence: Mapping[str, object] | None,
    recovered_at: float,
    archived_active: bool,
) -> dict[str, object]:
    return {
        "source": "daemon_authoritative_transfer_recovery",
        "transfer_id": str(transfer_id),
        "intent_id": archived.get("intent_id"),
        "state": str(getattr(status.state, "value", status.state)),
        "job_id": status.job_id,
        "session_id": status.session_id,
        "status": asdict(status),
        "receipt": None if receipt is None else asdict(receipt),
        "admission": dict(admission),
        "queue_record": dict(queue_record),
        "block_runtime": tuple(
            dict(record) for record in block_runtime if isinstance(record, Mapping)
        ),
        "ticket": None if ticket is None else asdict(ticket),
        "reservations": tuple(
            dict(record) for record in reservations if isinstance(record, Mapping)
        ),
        "leases": tuple(
            dict(record) for record in leases if isinstance(record, Mapping)
        ),
        "buffer_snapshots": {
            str(key): dict(value)
            for key, value in buffer_snapshots.items()
            if isinstance(value, Mapping)
        },
        "cleanup_targets": tuple(
            dict(target) for target in cleanup_targets if isinstance(target, Mapping)
        ),
        "completion_source": completion_source,
        "completion_evidence": dict(completion_evidence or {}),
        "recovered_at": float(recovered_at),
        "archived": bool(archived_active),
    }


__all__ = [
    "archive_record",
    "failure_cleanup_contract",
    "merge_archive_record",
    "recovery_state",
    "status_persistence_actions",
    "terminal_finalization_plan",
    "terminal_status_payload",
]
