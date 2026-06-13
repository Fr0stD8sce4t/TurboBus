from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, replace

from ..schema import TransferStatusState


@dataclass(frozen=True)
class BlockRuntimeRecord:
    transfer_id: str
    plan_id: str
    block_id: str
    path_id: str
    path_kind: str
    direction: str
    target_device: int
    relay_device: int | None
    state: str
    attempt: int
    src_offset: int
    dst_offset: int
    bytes: int
    allowed_path_ids: tuple[str, ...]
    lease_ids: tuple[str, ...]
    plan_generation: int
    ticket_id: str | None = None
    issued_at: float | None = None
    started_at: float | None = None
    completed_at: float | None = None
    error: str | None = None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def runtime_records_for_block_plan(
    *,
    transfer_id: str,
    plan: Mapping[str, object],
    plan_generation: int,
    lease_ids_by_relay: Mapping[int, Iterable[str]] | None = None,
) -> tuple[BlockRuntimeRecord, ...]:
    block_plan = plan.get("block_plan")
    if not isinstance(block_plan, Mapping):
        return ()
    plan_id = str(block_plan.get("plan_id", f"block-plan-{transfer_id}"))
    paths = {
        str(path["path_id"]): dict(path)
        for path in block_plan.get("paths", ()) or ()
        if isinstance(path, Mapping) and path.get("path_id") is not None
    }
    relay_leases = _normalized_relay_lease_map(lease_ids_by_relay)
    records: list[BlockRuntimeRecord] = []
    for block in block_plan.get("blocks", ()) or ():
        if not isinstance(block, Mapping):
            continue
        path_id = str(block["path_id"])
        path = paths.get(path_id, {})
        relay_device = _optional_int(path.get("relay_device"))
        records.append(
            BlockRuntimeRecord(
                transfer_id=str(transfer_id),
                plan_id=plan_id,
                block_id=str(block["block_id"]),
                path_id=path_id,
                path_kind=str(path.get("kind", "unknown")),
                direction=str(path.get("direction", block_plan.get("direction", "unknown"))),
                target_device=int(path.get("target_device", -1)),
                relay_device=relay_device,
                state="queued",
                attempt=int(block.get("attempt", 0) or 0),
                src_offset=int(block["src_offset"]),
                dst_offset=int(block["dst_offset"]),
                bytes=int(block["bytes"]),
                allowed_path_ids=tuple(
                    str(item) for item in block.get("allowed_path_ids", ()) or ()
                ),
                lease_ids=(
                    tuple(relay_leases.get(int(relay_device), ()))
                    if relay_device is not None
                    else ()
                ),
                plan_generation=int(plan_generation),
            )
        )
    return tuple(records)


def mark_ticket_issued(
    records: Iterable[BlockRuntimeRecord | Mapping[str, object]],
    *,
    ticket_id: str,
    issued_at: float,
) -> tuple[BlockRuntimeRecord, ...]:
    return tuple(
        replace(_record_from_mapping(record), ticket_id=str(ticket_id), issued_at=float(issued_at))
        for record in records
    )


def advance_for_status(
    records: Iterable[BlockRuntimeRecord | Mapping[str, object]],
    *,
    state: TransferStatusState | str,
    bytes_completed: int,
    completion_source: str | None,
    completion_evidence: Mapping[str, object] | None,
    now: float,
) -> tuple[tuple[BlockRuntimeRecord, ...], dict[str, object]]:
    current = tuple(_record_from_mapping(record) for record in records)
    if not current:
        return (), {
            "available": False,
            "source": "daemon_block_runtime",
            "reason": "no block runtime records",
        }
    status_state = TransferStatusState(state)
    if status_state is TransferStatusState.RUNNING:
        updated = tuple(_running_record(record, now=now) for record in current)
    elif status_state is TransferStatusState.COMPLETE:
        updated = tuple(_completed_record(record, now=now) for record in current)
    elif status_state in {TransferStatusState.FAILED, TransferStatusState.CANCELED}:
        updated = _terminal_partial_records(
            current,
            state=status_state.value,
            bytes_completed=int(bytes_completed),
            error=_completion_error(completion_evidence),
            now=now,
        )
    else:
        updated = current
    evidence = runtime_evidence(
        updated,
        completion_source=completion_source,
        completion_evidence=completion_evidence,
    )
    return updated, evidence


def advance_from_worker_progress(
    records: Iterable[BlockRuntimeRecord | Mapping[str, object]],
    *,
    progress: Mapping[str, object],
    completion_source: str | None,
    completion_evidence: Mapping[str, object] | None,
    now: float,
) -> tuple[tuple[BlockRuntimeRecord, ...], dict[str, object]]:
    current = tuple(_record_from_mapping(record) for record in records)
    if not current:
        return (), {
            "available": False,
            "source": "daemon_block_runtime",
            "reason": "no block runtime records",
        }
    progress_by_block = {
        str(item.get("block_id")): dict(item)
        for item in progress.get("records", ()) or ()
        if isinstance(item, Mapping) and item.get("block_id") is not None
    }
    if not progress_by_block:
        return advance_for_status(
            current,
            state=str(progress.get("state", "running")),
            bytes_completed=int(progress.get("bytes_completed", 0) or 0),
            completion_source=completion_source,
            completion_evidence=completion_evidence,
            now=now,
        )
    updated: list[BlockRuntimeRecord] = []
    for record in current:
        progress_record = progress_by_block.get(record.block_id)
        if progress_record is None:
            updated.append(record)
            continue
        state = str(progress_record.get("state", record.state))
        if state == "complete":
            state = "completed"
        if state == "cancelled":
            state = "canceled"
        if state == "completed":
            updated.append(_completed_record(record, now=now))
            continue
        if state in {"failed", "canceled"}:
            updated.append(
                replace(
                    record,
                    state=state,
                    started_at=record.started_at or float(now),
                    completed_at=float(now),
                    error=_completion_error(completion_evidence) or state,
                )
            )
            continue
        if state == "running":
            updated.append(_running_record(record, now=now))
            continue
        updated.append(record)
    evidence = runtime_evidence(
        updated,
        completion_source=completion_source,
        completion_evidence=completion_evidence,
    )
    evidence["worker_block_progress"] = {
        "source": progress.get("source", "worker_block_progress"),
        "transfer_id": progress.get("transfer_id"),
        "ticket_id": progress.get("ticket_id"),
        "plan_generation": progress.get("plan_generation"),
        "state": progress.get("state"),
        "bytes_completed": int(progress.get("bytes_completed", 0) or 0),
        "block_count": len(progress_by_block),
        "records": tuple(progress_by_block.values()),
    }
    return tuple(updated), evidence


def runtime_evidence(
    records: Iterable[BlockRuntimeRecord | Mapping[str, object]],
    *,
    completion_source: str | None = None,
    completion_evidence: Mapping[str, object] | None = None,
) -> dict[str, object]:
    normalized = tuple(_record_from_mapping(record) for record in records)
    summary = runtime_summary(normalized)
    return {
        "source": "daemon_block_runtime",
        "available": bool(normalized),
        "completion_source": completion_source,
        "summary": summary,
        "records": tuple(record.as_dict() for record in normalized),
        "completion_observed": _completion_observed_view(completion_evidence),
        "cleanup": block_cleanup_summary(normalized),
    }


def runtime_summary(
    records: Iterable[BlockRuntimeRecord | Mapping[str, object]],
) -> dict[str, object]:
    normalized = tuple(_record_from_mapping(record) for record in records)
    states: dict[str, int] = {}
    bytes_by_state: dict[str, int] = {}
    bytes_by_path_kind: dict[str, int] = {}
    lease_ids: set[str] = set()
    ticket_ids: set[str] = set()
    for record in normalized:
        states[record.state] = states.get(record.state, 0) + 1
        bytes_by_state[record.state] = bytes_by_state.get(record.state, 0) + record.bytes
        bytes_by_path_kind[record.path_kind] = (
            bytes_by_path_kind.get(record.path_kind, 0) + record.bytes
        )
        lease_ids.update(record.lease_ids)
        if record.ticket_id is not None:
            ticket_ids.add(record.ticket_id)
    return {
        "source": "daemon_block_runtime",
        "block_count": len(normalized),
        "states": states,
        "bytes_by_state": bytes_by_state,
        "bytes_by_path_kind": bytes_by_path_kind,
        "lease_ids": tuple(sorted(lease_ids)),
        "ticket_ids": tuple(sorted(ticket_ids)),
    }


def ticket_metadata_view(
    records: Iterable[BlockRuntimeRecord | Mapping[str, object]],
) -> dict[str, object] | None:
    normalized = tuple(_record_from_mapping(record) for record in records)
    if not normalized:
        return None
    return {
        "source": "daemon_block_runtime",
        "summary": runtime_summary(normalized),
        "records": tuple(record.as_dict() for record in normalized),
    }


def queue_view(
    records: Iterable[BlockRuntimeRecord | Mapping[str, object]],
) -> dict[str, object]:
    normalized = tuple(_record_from_mapping(record) for record in records)
    summary = runtime_summary(normalized)
    return {
        "source": "daemon_block_runtime",
        "available": bool(normalized),
        "block_count": int(summary["block_count"]),
        "states": dict(summary["states"]),
        "bytes_by_state": dict(summary["bytes_by_state"]),
        "records": tuple(
            {
                "block_id": record.block_id,
                "path_id": record.path_id,
                "state": record.state,
                "attempt": record.attempt,
                "bytes": record.bytes,
                "error": record.error,
                "lease_ids": record.lease_ids,
                "ticket_id": record.ticket_id,
            }
            for record in normalized
        ),
    }


def block_cleanup_summary(
    records: Iterable[BlockRuntimeRecord | Mapping[str, object]],
) -> dict[str, object]:
    normalized = tuple(_record_from_mapping(record) for record in records)
    cleanup_targets = []
    for record in normalized:
        for lease_id in record.lease_ids:
            cleanup_targets.append(
                {
                    "block_id": record.block_id,
                    "target_kind": "reservation",
                    "target_id": lease_id,
                    "path_id": record.path_id,
                    "relay_device": record.relay_device,
                }
            )
    return {
        "source": "daemon_block_runtime_cleanup",
        "block_count": len(normalized),
        "cleanup_target_count": len(cleanup_targets),
        "cleanup_targets": tuple(cleanup_targets),
    }


def _running_record(record: BlockRuntimeRecord, *, now: float) -> BlockRuntimeRecord:
    if record.state in {"completed", "failed", "canceled"}:
        return record
    return replace(record, state="running", started_at=record.started_at or float(now))


def _completed_record(record: BlockRuntimeRecord, *, now: float) -> BlockRuntimeRecord:
    return replace(
        record,
        state="completed",
        started_at=record.started_at or float(now),
        completed_at=float(now),
        error=None,
    )


def _terminal_partial_records(
    records: tuple[BlockRuntimeRecord, ...],
    *,
    state: str,
    bytes_completed: int,
    error: str | None,
    now: float,
) -> tuple[BlockRuntimeRecord, ...]:
    remaining = max(0, int(bytes_completed))
    updated: list[BlockRuntimeRecord] = []
    for record in records:
        if remaining >= record.bytes:
            updated.append(_completed_record(record, now=now))
            remaining -= record.bytes
            continue
        updated.append(
            replace(
                record,
                state=state,
                started_at=record.started_at or float(now),
                completed_at=float(now),
                error=error or state,
            )
        )
    return tuple(updated)


def _completion_observed_view(
    completion_evidence: Mapping[str, object] | None,
) -> dict[str, object] | None:
    if not isinstance(completion_evidence, Mapping):
        return None
    observed = {}
    for field_name in (
        "ticket_id",
        "transfer_id",
        "plan_generation",
        "verified_bytes",
        "expected_bytes",
        "direct_bytes",
        "relay_bytes",
        "direct_chunks",
        "relay_chunks",
        "path",
        "executor",
    ):
        if field_name in completion_evidence:
            observed[field_name] = completion_evidence[field_name]
    return observed or None


def _completion_error(completion_evidence: Mapping[str, object] | None) -> str | None:
    if not isinstance(completion_evidence, Mapping):
        return None
    for field_name in ("error", "failure_source"):
        value = completion_evidence.get(field_name)
        if value is not None:
            return str(value)
    return None


def _normalized_relay_lease_map(
    lease_ids_by_relay: Mapping[int, Iterable[str]] | None,
) -> dict[int, tuple[str, ...]]:
    if not isinstance(lease_ids_by_relay, Mapping):
        return {}
    result: dict[int, tuple[str, ...]] = {}
    for relay, lease_ids in lease_ids_by_relay.items():
        result[int(relay)] = tuple(str(item) for item in lease_ids)
    return result


def _record_from_mapping(
    record: BlockRuntimeRecord | Mapping[str, object],
) -> BlockRuntimeRecord:
    if isinstance(record, BlockRuntimeRecord):
        return record
    relay_device = _optional_int(record.get("relay_device"))
    return BlockRuntimeRecord(
        transfer_id=str(record["transfer_id"]),
        plan_id=str(record["plan_id"]),
        block_id=str(record["block_id"]),
        path_id=str(record["path_id"]),
        path_kind=str(record.get("path_kind", "unknown")),
        direction=str(record.get("direction", "unknown")),
        target_device=int(record.get("target_device", -1)),
        relay_device=relay_device,
        state=str(record.get("state", "queued")),
        attempt=int(record.get("attempt", 0) or 0),
        src_offset=int(record.get("src_offset", 0) or 0),
        dst_offset=int(record.get("dst_offset", 0) or 0),
        bytes=int(record.get("bytes", 0) or 0),
        allowed_path_ids=tuple(
            str(item) for item in record.get("allowed_path_ids", ()) or ()
        ),
        lease_ids=tuple(str(item) for item in record.get("lease_ids", ()) or ()),
        plan_generation=int(record.get("plan_generation", 0) or 0),
        ticket_id=(
            None if record.get("ticket_id") is None else str(record.get("ticket_id"))
        ),
        issued_at=_optional_float(record.get("issued_at")),
        started_at=_optional_float(record.get("started_at")),
        completed_at=_optional_float(record.get("completed_at")),
        error=None if record.get("error") is None else str(record.get("error")),
    )


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


__all__ = [
    "BlockRuntimeRecord",
    "advance_from_worker_progress",
    "advance_for_status",
    "block_cleanup_summary",
    "mark_ticket_issued",
    "queue_view",
    "runtime_evidence",
    "runtime_records_for_block_plan",
    "runtime_summary",
    "ticket_metadata_view",
]
