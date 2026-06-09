from __future__ import annotations

from collections.abc import Mapping

from ..schema import TransferStatusState
from ..scheduler import SchedulingDecision


def runtime_active_path_records_for_transfer(
    *,
    record: Mapping[str, object],
    decision: SchedulingDecision,
) -> tuple[dict[str, object], ...]:
    state = str(record.get("state", ""))
    if state != TransferStatusState.RUNNING.value:
        return ()
    assignments = normalized_plan_assignments(decision.plan.get("assignments", ()) or ())
    if not assignments:
        return ()
    direct_total = sum(
        assignment["bytes_total"]
        for assignment in assignments
        if assignment["kind"] == "direct"
    )
    relay_total = sum(
        assignment["bytes_total"]
        for assignment in assignments
        if assignment["kind"] == "relay"
    )
    completion_source = str(record.get("completion_source", "")).lower()
    active_kind = active_path_kind_for_record(
        completion_source=completion_source,
        bytes_completed=int(record.get("bytes_completed", 0) or 0),
        direct_total=direct_total,
        relay_total=relay_total,
    )
    if active_kind is None:
        return ()
    if active_kind == "direct":
        completed_in_kind = min(
            int(record.get("bytes_completed", 0) or 0),
            direct_total,
        )
    else:
        completed_in_kind = max(
            0,
            int(record.get("bytes_completed", 0) or 0) - direct_total,
        )
    remaining_phase_cursor = completed_in_kind
    records: list[dict[str, object]] = []
    for assignment in assignments:
        if assignment["kind"] != active_kind:
            continue
        bytes_total = int(assignment["bytes_total"])
        bytes_remaining, chunk_count = remaining_assignment_load(
            assignment,
            completed_bytes=remaining_phase_cursor,
        )
        remaining_phase_cursor = max(0, remaining_phase_cursor - bytes_total)
        if bytes_remaining <= 0 or chunk_count <= 0:
            continue
        path = assignment["path"]
        records.append(
            {
                "transfer_id": str(record.get("transfer_id")),
                "kind": assignment["kind"],
                "direction": assignment["direction"],
                "target_device": path.get("target_device"),
                "relay_device": path.get("relay_device"),
                "bytes_total": bytes_remaining,
                "chunk_count": chunk_count,
                "completion_source": completion_source,
                "phase": "running",
            }
        )
    return tuple(records)


def normalized_plan_assignments(
    assignments: object,
) -> tuple[dict[str, object], ...]:
    if not isinstance(assignments, list | tuple):
        return ()
    normalized: list[dict[str, object]] = []
    for assignment in assignments:
        if not isinstance(assignment, Mapping):
            continue
        path = assignment.get("path")
        if not isinstance(path, Mapping):
            continue
        chunks = assignment.get("chunks", ()) or ()
        chunk_records = (
            tuple(dict(chunk) for chunk in chunks if isinstance(chunk, Mapping))
            if isinstance(chunks, list | tuple)
            else ()
        )
        bytes_total = int(assignment.get("bytes", 0) or 0)
        if bytes_total <= 0:
            bytes_total = sum(
                int(chunk.get("bytes", 0) or 0)
                for chunk in chunk_records
            )
        normalized.append(
            {
                "kind": str(path.get("kind", "unknown")).lower(),
                "direction": str(path.get("direction", "unknown")).lower(),
                "path": dict(path),
                "chunks": chunk_records,
                "bytes_total": max(0, bytes_total),
            }
        )
    return tuple(normalized)


def active_path_kind_for_record(
    *,
    completion_source: str,
    bytes_completed: int,
    direct_total: int,
    relay_total: int,
) -> str | None:
    if completion_source == "worker":
        if relay_total > 0:
            return "relay"
        if direct_total > 0:
            return "direct"
        return None
    if completion_source == "backend":
        if direct_total <= 0:
            return None
        if relay_total > 0 and bytes_completed >= direct_total:
            return None
        return "direct"
    return None


def remaining_assignment_load(
    assignment: Mapping[str, object],
    *,
    completed_bytes: int,
) -> tuple[int, int]:
    total_bytes = int(assignment.get("bytes_total", 0) or 0)
    remaining_bytes = max(0, total_bytes - max(0, int(completed_bytes)))
    chunks = assignment.get("chunks", ())
    if not isinstance(chunks, tuple):
        chunks = ()
    if not chunks:
        return remaining_bytes, 0 if remaining_bytes <= 0 else 1
    remaining_chunk_count = 0
    completed_cursor = max(0, int(completed_bytes))
    for chunk in chunks:
        chunk_bytes = int(chunk.get("bytes", 0) or 0)
        if chunk_bytes <= 0:
            continue
        if completed_cursor >= chunk_bytes:
            completed_cursor -= chunk_bytes
            continue
        remaining_chunk_count += 1
    return remaining_bytes, remaining_chunk_count


__all__ = [
    "active_path_kind_for_record",
    "normalized_plan_assignments",
    "remaining_assignment_load",
    "runtime_active_path_records_for_transfer",
]
