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
    active_kinds = active_path_kinds_for_record(
        completion_source=completion_source,
        bytes_completed=int(record.get("bytes_completed", 0) or 0),
        direct_total=direct_total,
        relay_total=relay_total,
    )
    if not active_kinds:
        return ()
    completed_by_kind = completed_bytes_by_path_kind(
        completion_source=completion_source,
        bytes_completed=int(record.get("bytes_completed", 0) or 0),
        direct_total=direct_total,
        relay_total=relay_total,
    )
    records: list[dict[str, object]] = []
    for active_kind in active_kinds:
        completed_in_kind = int(completed_by_kind.get(active_kind, 0))
        remaining_phase_cursor = completed_in_kind
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
    active_kinds = active_path_kinds_for_record(
        completion_source=completion_source,
        bytes_completed=bytes_completed,
        direct_total=direct_total,
        relay_total=relay_total,
    )
    if not active_kinds:
        return None
    return active_kinds[-1]


def active_path_kinds_for_record(
    *,
    completion_source: str,
    bytes_completed: int,
    direct_total: int,
    relay_total: int,
) -> tuple[str, ...]:
    if completion_source == "worker":
        active = []
        if direct_total > 0:
            active.append("direct")
        if relay_total > 0:
            active.append("relay")
        return tuple(active)
    if completion_source == "backend":
        if direct_total <= 0:
            return ()
        if relay_total > 0 and bytes_completed >= direct_total:
            return ()
        return ("direct",)
    return ()


def completed_bytes_by_path_kind(
    *,
    completion_source: str,
    bytes_completed: int,
    direct_total: int,
    relay_total: int,
) -> dict[str, int]:
    completed = max(0, int(bytes_completed))
    direct = max(0, int(direct_total))
    relay = max(0, int(relay_total))
    total = direct + relay
    if completed <= 0 or total <= 0:
        return {"direct": 0, "relay": 0}
    if completion_source == "worker" and direct > 0 and relay > 0:
        capped = min(completed, total)
        direct_completed = min(direct, (capped * direct) // total)
        relay_completed = min(relay, capped - direct_completed)
        return {
            "direct": int(direct_completed),
            "relay": int(relay_completed),
        }
    if completion_source == "backend":
        return {
            "direct": min(completed, direct),
            "relay": 0,
        }
    if direct > 0:
        return {
            "direct": min(completed, direct),
            "relay": max(0, min(relay, completed - direct)),
        }
    return {
        "direct": 0,
        "relay": min(completed, relay),
    }


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
    "active_path_kinds_for_record",
    "completed_bytes_by_path_kind",
    "normalized_plan_assignments",
    "remaining_assignment_load",
    "runtime_active_path_records_for_transfer",
]
