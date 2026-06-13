from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass

from ..planner_types import PlannerPath, PlannerPathAssignment


@dataclass(frozen=True)
class BlockPath:
    path_id: str
    kind: str
    direction: str
    target_device: int
    relay_device: int | None
    scheduler_weight_gbps: float
    runtime_pressure: float
    metadata: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class TransferBlock:
    block_id: str
    path_id: str
    attempt: int
    src_offset: int
    dst_offset: int
    bytes: int
    allowed_path_ids: tuple[str, ...]
    metadata: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class BlockPlan:
    plan_id: str
    direction: str
    total_bytes: int
    blocks: tuple[TransferBlock, ...]
    paths: tuple[BlockPath, ...]
    metadata: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return {
            "plan_id": self.plan_id,
            "direction": self.direction,
            "total_bytes": self.total_bytes,
            "blocks": tuple(block.as_dict() for block in self.blocks),
            "paths": tuple(path.as_dict() for path in self.paths),
            "metadata": dict(self.metadata),
        }


def block_path_from_planner_path(path: PlannerPath) -> BlockPath:
    path_id = planner_path_id(path)
    return BlockPath(
        path_id=path_id,
        kind=str(path.kind),
        direction=str(path.direction),
        target_device=int(path.target_device),
        relay_device=None if path.kind != "relay" else int(path.relay_device),
        scheduler_weight_gbps=max(
            0.0,
            float(path.scheduler_weight_gbps or path.effective_bw_gbps or 0.0),
        ),
        runtime_pressure=max(0.0, float(path.runtime_pressure)),
        metadata=dict(path.cost_metadata),
    )


def block_plan_from_assignments(
    *,
    plan_id: str,
    total_bytes: int,
    chunk_bytes: int,
    assignments: Sequence[PlannerPathAssignment],
    direction: str,
    metadata: Mapping[str, object] | None = None,
) -> BlockPlan:
    block_paths: dict[str, BlockPath] = {}
    blocks: list[TransferBlock] = []
    allowed_path_ids = tuple(
        planner_path_id(assignment.path)
        for assignment in assignments
        if assignment.chunks
    )
    for assignment_index, assignment in enumerate(assignments):
        path_id = planner_path_id(assignment.path)
        block_paths.setdefault(path_id, block_path_from_planner_path(assignment.path))
        for chunk_index, chunk in enumerate(assignment.chunks):
            for split_index, split in enumerate(_split_chunk(chunk, chunk_bytes)):
                block_id = f"{plan_id}:b{len(blocks)}"
                blocks.append(
                    TransferBlock(
                        block_id=block_id,
                        path_id=path_id,
                        attempt=0,
                        src_offset=int(split.src_offset),
                        dst_offset=int(split.dst_offset),
                        bytes=int(split.bytes),
                        allowed_path_ids=allowed_path_ids,
                        metadata={
                            "source": "daemon_scheduler_block_plan",
                            "assignment_index": assignment_index,
                            "chunk_index": chunk_index,
                            "split_index": split_index,
                        },
                    )
                )
    return BlockPlan(
        plan_id=str(plan_id),
        direction=str(direction).lower(),
        total_bytes=int(total_bytes),
        blocks=tuple(blocks),
        paths=tuple(block_paths[path_id] for path_id in sorted(block_paths)),
        metadata={
            "source": "daemon_scheduler_block_plan",
            "block_count": len(blocks),
            "path_count": len(block_paths),
            "chunk_bytes": int(chunk_bytes),
            **dict(metadata or {}),
        },
    )


def planner_path_id(path: PlannerPath) -> str:
    if str(path.kind).lower() == "relay":
        return (
            f"{path.direction}:relay:{int(path.relay_device)}"
            f"->{int(path.target_device)}"
        )
    return f"{path.direction}:direct:{int(path.target_device)}"


def _split_chunk(chunk, chunk_bytes: int) -> tuple[object, ...]:
    size = max(1, int(chunk_bytes))
    if int(chunk.bytes) <= size:
        return (chunk,)
    split = []
    consumed = 0
    while consumed < int(chunk.bytes):
        current = min(size, int(chunk.bytes) - consumed)
        split.append(
            type(chunk)(
                src_offset=int(chunk.src_offset) + consumed,
                dst_offset=int(chunk.dst_offset) + consumed,
                bytes=current,
                relay_device=getattr(chunk, "relay_device", None),
            )
        )
        consumed += current
    return tuple(split)


def block_plan_from_mapping(value: Mapping[str, object]) -> BlockPlan:
    return BlockPlan(
        plan_id=str(value.get("plan_id", "unknown")),
        direction=str(value.get("direction", "unknown")),
        total_bytes=int(value.get("total_bytes", 0) or 0),
        blocks=tuple(
            _block_from_mapping(item)
            for item in value.get("blocks", ()) or ()
            if isinstance(item, Mapping)
        ),
        paths=tuple(
            _path_from_mapping(item)
            for item in value.get("paths", ()) or ()
            if isinstance(item, Mapping)
        ),
        metadata=dict(value.get("metadata", {}) or {}),
    )


def _block_from_mapping(value: Mapping[str, object]) -> TransferBlock:
    return TransferBlock(
        block_id=str(value["block_id"]),
        path_id=str(value["path_id"]),
        attempt=int(value.get("attempt", 0) or 0),
        src_offset=int(value["src_offset"]),
        dst_offset=int(value["dst_offset"]),
        bytes=int(value["bytes"]),
        allowed_path_ids=tuple(
            str(item) for item in value.get("allowed_path_ids", ()) or ()
        ),
        metadata=dict(value.get("metadata", {}) or {}),
    )


def _path_from_mapping(value: Mapping[str, object]) -> BlockPath:
    relay_device = value.get("relay_device")
    return BlockPath(
        path_id=str(value["path_id"]),
        kind=str(value["kind"]),
        direction=str(value["direction"]),
        target_device=int(value["target_device"]),
        relay_device=None if relay_device is None else int(relay_device),
        scheduler_weight_gbps=float(value.get("scheduler_weight_gbps", 0.0) or 0.0),
        runtime_pressure=float(value.get("runtime_pressure", 0.0) or 0.0),
        metadata=dict(value.get("metadata", {}) or {}),
    )


__all__ = [
    "BlockPath",
    "BlockPlan",
    "TransferBlock",
    "block_path_from_planner_path",
    "block_plan_from_assignments",
    "block_plan_from_mapping",
    "planner_path_id",
]
