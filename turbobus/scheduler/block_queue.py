from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace

from .block_plan import BlockPlan


@dataclass(frozen=True)
class BlockQueueRecord:
    block_id: str
    path_id: str
    state: str
    attempt: int
    bytes: int
    error: str | None = None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def queue_records_for_block_plan(block_plan: BlockPlan) -> tuple[BlockQueueRecord, ...]:
    return tuple(
        BlockQueueRecord(
            block_id=block.block_id,
            path_id=block.path_id,
            state="queued",
            attempt=block.attempt,
            bytes=block.bytes,
        )
        for block in block_plan.blocks
    )


def transition_block_record(
    record: BlockQueueRecord | Mapping[str, object],
    *,
    state: str,
    error: str | None = None,
) -> BlockQueueRecord:
    current = (
        record
        if isinstance(record, BlockQueueRecord)
        else BlockQueueRecord(
            block_id=str(record["block_id"]),
            path_id=str(record["path_id"]),
            state=str(record.get("state", "queued")),
            attempt=int(record.get("attempt", 0) or 0),
            bytes=int(record.get("bytes", 0) or 0),
            error=None if record.get("error") is None else str(record.get("error")),
        )
    )
    return replace(current, state=str(state), error=error)


def queue_summary(records: tuple[BlockQueueRecord, ...]) -> dict[str, object]:
    states: dict[str, int] = {}
    bytes_by_state: dict[str, int] = {}
    for record in records:
        states[record.state] = states.get(record.state, 0) + 1
        bytes_by_state[record.state] = bytes_by_state.get(record.state, 0) + record.bytes
    return {
        "source": "daemon_scheduler_block_queue",
        "block_count": len(records),
        "states": states,
        "bytes_by_state": bytes_by_state,
    }


__all__ = [
    "BlockQueueRecord",
    "queue_records_for_block_plan",
    "queue_summary",
    "transition_block_record",
]
