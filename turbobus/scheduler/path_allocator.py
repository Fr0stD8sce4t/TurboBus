from __future__ import annotations

from collections.abc import Mapping

from ..planner_types import PlannerTransferPlan
from .block_plan import BlockPlan, block_plan_from_assignments


def block_plan_for_transfer_plan(
    plan: PlannerTransferPlan,
    *,
    decision_seed: str,
    direction: str,
    scheduler_metadata: Mapping[str, object] | None = None,
) -> BlockPlan:
    assignments = tuple(plan.assignments)
    direct_blocks = 0
    relay_blocks = 0
    for assignment in assignments:
        if assignment.path.kind == "relay":
            relay_blocks += len(assignment.chunks)
        else:
            direct_blocks += len(assignment.chunks)
    if direct_blocks > 0 and relay_blocks > 0:
        mode = "mixed"
    elif relay_blocks > 0:
        mode = "relay"
    else:
        mode = "direct"
    return block_plan_from_assignments(
        plan_id=f"block-plan-{decision_seed}",
        total_bytes=plan.total_bytes,
        chunk_bytes=plan.chunk_bytes,
        assignments=assignments,
        direction=direction,
        metadata={
            "allocation_source": "daemon_scheduler_path_allocator",
            "allocation_mode": mode,
            "direct_block_count": direct_blocks,
            "relay_block_count": relay_blocks,
            "planner_cost_metadata": dict(plan.cost_metadata),
            **dict(scheduler_metadata or {}),
        },
    )


def block_plan_metadata(block_plan: BlockPlan) -> dict[str, object]:
    direct_bytes = 0
    relay_bytes = 0
    paths = {path.path_id: path for path in block_plan.paths}
    for block in block_plan.blocks:
        path = paths.get(block.path_id)
        if path is not None and path.kind == "relay":
            relay_bytes += block.bytes
        else:
            direct_bytes += block.bytes
    return {
        "source": "daemon_scheduler_path_allocator",
        "plan_id": block_plan.plan_id,
        "direction": block_plan.direction,
        "block_count": len(block_plan.blocks),
        "path_count": len(block_plan.paths),
        "direct_bytes": direct_bytes,
        "relay_bytes": relay_bytes,
        "allocation_mode": block_plan.metadata.get("allocation_mode"),
    }


__all__ = [
    "block_plan_for_transfer_plan",
    "block_plan_metadata",
]
