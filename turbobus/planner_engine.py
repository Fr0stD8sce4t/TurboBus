from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Sequence

from .planner_types import (
    PlannerChunk,
    PlannerPath,
    PlannerPathAssignment,
    PlannerTransferPlan,
)
from .schema import TransferMode


@dataclass(frozen=True)
class PlannerEngineOptions:
    min_chunks_for_relay: int = 2
    min_pool_bytes: int = 12 * 1024 * 1024
    relay_min_effective_bw_gbps: float = 0.0
    relay_min_direct_ratio: float = 0.0


class PlannerEngine:
    def __init__(self, options: PlannerEngineOptions | None = None) -> None:
        self.options = options or PlannerEngineOptions()

    def plan(
        self,
        total_bytes: int,
        chunk_bytes: int,
        profile,
        mode: TransferMode | str = TransferMode.POOL,
        *,
        direction: str = "h2d",
    ) -> PlannerTransferPlan:
        total_bytes = max(0, int(total_bytes))
        if total_bytes == 0:
            return PlannerTransferPlan()
        chunk_bytes = int(chunk_bytes)
        if chunk_bytes <= 0:
            raise ValueError("chunk_bytes must be greater than zero")

        chunks = _make_chunks(total_bytes, chunk_bytes)
        transfer_mode = TransferMode(mode)
        pool_disabled_reason = None
        if transfer_mode is TransferMode.POOL and len(chunks) < self.options.min_chunks_for_relay:
            transfer_mode = TransferMode.DIRECT
            pool_disabled_reason = "below_min_chunks_for_relay"
        if (
            transfer_mode is TransferMode.POOL
            and int(self.options.min_pool_bytes) > 0
            and total_bytes < int(self.options.min_pool_bytes)
        ):
            transfer_mode = TransferMode.DIRECT
            pool_disabled_reason = "below_min_pool_bytes"

        paths = self._build_paths(profile, transfer_mode, direction)
        if not paths:
            raise RuntimeError("no enabled transfer path is available")
        return self._plan_chunks(
            chunks,
            total_bytes,
            chunk_bytes,
            paths,
            pool_disabled_reason=pool_disabled_reason,
        )

    def plan_ranges(
        self,
        ranges: Iterable,
        chunk_bytes: int,
        profile,
        mode: TransferMode | str = TransferMode.POOL,
        *,
        direction: str = "h2d",
    ) -> PlannerTransferPlan:
        chunk_bytes = int(chunk_bytes)
        if chunk_bytes <= 0:
            raise ValueError("chunk_bytes must be greater than zero")

        chunks: list[PlannerChunk] = []
        total_bytes = 0
        for range_item in ranges:
            src_offset, dst_offset, bytes_ = _range_fields(range_item)
            if bytes_ <= 0:
                continue
            total_bytes += bytes_
            consumed = 0
            while consumed < bytes_:
                chunk_bytes_this = min(chunk_bytes, bytes_ - consumed)
                chunks.append(
                    PlannerChunk(
                        src_offset=src_offset + consumed,
                        dst_offset=dst_offset + consumed,
                        bytes=chunk_bytes_this,
                    )
                )
                consumed += chunk_bytes_this
        if not chunks:
            return PlannerTransferPlan()

        transfer_mode = TransferMode(mode)
        pool_disabled_reason = None
        if transfer_mode is TransferMode.POOL and len(chunks) < self.options.min_chunks_for_relay:
            transfer_mode = TransferMode.DIRECT
            pool_disabled_reason = "below_min_chunks_for_relay"
        if (
            transfer_mode is TransferMode.POOL
            and int(self.options.min_pool_bytes) > 0
            and total_bytes < int(self.options.min_pool_bytes)
        ):
            transfer_mode = TransferMode.DIRECT
            pool_disabled_reason = "below_min_pool_bytes"

        paths = self._build_paths(profile, transfer_mode, direction)
        if not paths:
            raise RuntimeError("no enabled transfer path is available")
        return self._plan_chunks(
            chunks,
            total_bytes,
            chunk_bytes,
            paths,
            pool_disabled_reason=pool_disabled_reason,
        )

    def _build_paths(
        self,
        profile,
        mode: TransferMode,
        direction: str,
    ) -> list[PlannerPath]:
        paths: list[PlannerPath] = []
        direct_bw = _direct_bandwidth(profile, direction)
        if mode is not TransferMode.RELAY and direct_bw > 0.0:
            paths.append(
                PlannerPath(
                    kind="direct",
                    direction=direction,
                    target_device=int(getattr(profile, "target_device", 0)),
                    relay_device=-1,
                    h2d_bw_gbps=float(getattr(profile, "direct_h2d_bw_gbps", 0.0) or 0.0),
                    d2h_bw_gbps=float(
                        getattr(profile, "direct_d2h_bw_gbps", 0.0) or direct_bw
                    ),
                    p2p_bw_gbps=0.0,
                    effective_bw_gbps=float(direct_bw),
                    enabled=True,
                    scheduler_weight_gbps=_direct_scheduler_weight(profile, direction),
                    runtime_pressure=_direct_runtime_pressure(profile, direction),
                    cost_metadata=dict(getattr(profile, "cost_metadata", {}) or {}),
                )
            )

        if mode is TransferMode.DIRECT:
            return paths

        for relay in getattr(profile, "relays", []) or []:
            relay_effective_bw = _relay_effective_bandwidth(relay, direction)
            if not getattr(relay, "p2p_enabled", False) or relay_effective_bw <= 0.0:
                continue
            if relay_effective_bw < self.options.relay_min_effective_bw_gbps:
                continue
            if (
                direct_bw > 0.0
                and self.options.relay_min_direct_ratio > 0.0
                and relay_effective_bw < direct_bw * self.options.relay_min_direct_ratio
            ):
                continue
            paths.append(
                PlannerPath(
                    kind="relay",
                    direction=direction,
                    target_device=int(getattr(relay, "target_device", getattr(profile, "target_device", 0))),
                    relay_device=int(getattr(relay, "relay_device", -1)),
                    h2d_bw_gbps=float(getattr(relay, "h2d_bw_gbps", 0.0) or 0.0),
                    d2h_bw_gbps=float(
                        getattr(relay, "d2h_bw_gbps", 0.0) or getattr(relay, "h2d_bw_gbps", 0.0)
                    ),
                    p2p_bw_gbps=float(getattr(relay, "p2p_bw_gbps", 0.0) or 0.0),
                    effective_bw_gbps=float(relay_effective_bw),
                    enabled=True,
                    scheduler_weight_gbps=_relay_scheduler_weight(relay, direction),
                    runtime_pressure=_relay_runtime_pressure(relay, direction),
                    cost_metadata=dict(getattr(relay, "cost_metadata", {}) or {}),
                )
            )
        return paths

    @staticmethod
    def _plan_chunks(
        chunks: Sequence[PlannerChunk],
        total_bytes: int,
        chunk_bytes: int,
        paths: Sequence[PlannerPath],
        pool_disabled_reason: str | None = None,
    ) -> PlannerTransferPlan:
        path_weights = tuple(_scheduler_weight(path) for path in paths)
        total_bw = sum(path_weights)
        if total_bw <= 0.0:
            raise RuntimeError("enabled paths have zero effective bandwidth")

        assignment_chunks: list[list[PlannerChunk]] = [[] for _ in paths]
        assigned_scores = [0.0 for _ in paths]

        for chunk in chunks:
            selected = 0
            best_score = math.inf
            best_weight = -1.0
            for index, path in enumerate(paths):
                weight = max(path_weights[index], 1e-12)
                score = (assigned_scores[index] + float(chunk.bytes)) / weight
                if score < best_score or (
                    math.isclose(score, best_score) and weight > best_weight
                ):
                    best_score = score
                    best_weight = weight
                    selected = index
            assignment_chunks[selected].append(chunk)
            assigned_scores[selected] += float(chunk.bytes)

        assignments = tuple(
            PlannerPathAssignment(
                path=path,
                chunks=_coalesced_path_chunks(path, assignment_chunks[index]),
            )
            for index, path in enumerate(paths)
            if assignment_chunks[index]
        )
        return PlannerTransferPlan(
            total_bytes=int(total_bytes),
            chunk_bytes=int(chunk_bytes),
            assignments=assignments,
            cost_metadata=_plan_cost_metadata(
                paths,
                assignments,
                path_weights=path_weights,
                pool_disabled_reason=pool_disabled_reason,
            ),
        )


def _direct_bandwidth(profile, direction: str) -> float:
    direct_attr = "direct_h2d_bw_gbps" if direction == "h2d" else "direct_d2h_bw_gbps"
    direct_bw = max(0.0, float(getattr(profile, direct_attr, 0.0) or 0.0))
    if direction != "h2d" and direct_bw <= 0.0:
        direct_bw = max(0.0, float(getattr(profile, "direct_h2d_bw_gbps", 0.0) or 0.0))
    return direct_bw


def _relay_effective_bandwidth(relay, direction: str) -> float:
    relay_attr = "effective_bw_gbps" if direction == "h2d" else "effective_d2h_bw_gbps"
    effective_bw = max(0.0, float(getattr(relay, relay_attr, 0.0) or 0.0))
    if direction != "h2d" and effective_bw <= 0.0:
        effective_bw = max(0.0, float(getattr(relay, "effective_bw_gbps", 0.0) or 0.0))
    return effective_bw


def _direct_scheduler_weight(profile, direction: str) -> float:
    attr = (
        "direct_scheduler_weight_h2d_gbps"
        if direction == "h2d"
        else "direct_scheduler_weight_d2h_gbps"
    )
    weight = float(getattr(profile, attr, 0.0) or 0.0)
    if weight > 0.0:
        return weight
    return 0.0


def _relay_scheduler_weight(relay, direction: str) -> float:
    attr = (
        "scheduler_weight_h2d_gbps"
        if direction == "h2d"
        else "scheduler_weight_d2h_gbps"
    )
    weight = float(getattr(relay, attr, 0.0) or 0.0)
    if weight > 0.0:
        return weight
    return 0.0


def _direct_runtime_pressure(profile, direction: str) -> float:
    attr = (
        "direct_runtime_pressure_h2d"
        if direction == "h2d"
        else "direct_runtime_pressure_d2h"
    )
    return max(0.0, float(getattr(profile, attr, 0.0) or 0.0))


def _relay_runtime_pressure(relay, direction: str) -> float:
    attr = "runtime_pressure_h2d" if direction == "h2d" else "runtime_pressure_d2h"
    return max(0.0, float(getattr(relay, attr, 0.0) or 0.0))


def _scheduler_weight(path: PlannerPath) -> float:
    if path.scheduler_weight_gbps is not None and float(path.scheduler_weight_gbps) > 0.0:
        return max(0.0, float(path.scheduler_weight_gbps))
    pressure = min(max(0.0, float(path.runtime_pressure)), 4.0)
    return max(0.0, float(path.effective_bw_gbps)) / (1.0 + pressure)


def _coalesced_path_chunks(
    path: PlannerPath,
    chunks: Sequence[PlannerChunk],
) -> tuple[PlannerChunk, ...]:
    if path.kind != "direct":
        return tuple(chunks)
    merged: list[PlannerChunk] = []
    for chunk in chunks:
        if not merged or not _chunks_are_contiguous(merged[-1], chunk):
            merged.append(chunk)
            continue
        previous = merged[-1]
        merged[-1] = PlannerChunk(
            src_offset=previous.src_offset,
            dst_offset=previous.dst_offset,
            bytes=previous.bytes + chunk.bytes,
            relay_device=previous.relay_device,
        )
    return tuple(merged)


def _chunks_are_contiguous(left: PlannerChunk, right: PlannerChunk) -> bool:
    return (
        left.relay_device == right.relay_device
        and left.src_offset + left.bytes == right.src_offset
        and left.dst_offset + left.bytes == right.dst_offset
    )


def _plan_cost_metadata(
    paths: Sequence[PlannerPath],
    assignments: Sequence[PlannerPathAssignment],
    *,
    path_weights: Sequence[float],
    pool_disabled_reason: str | None,
) -> dict[str, object]:
    if len(path_weights) != len(paths):
        raise RuntimeError("planner path weight snapshot does not match paths")
    path_records = []
    assigned_by_path = {
        _planner_path_key(assignment.path): assignment
        for assignment in assignments
    }
    for index, path in enumerate(paths):
        assignment = assigned_by_path.get(_planner_path_key(path))
        assigned_bytes = (
            0
            if assignment is None
            else sum(chunk.bytes for chunk in assignment.chunks)
        )
        assigned_chunks = 0 if assignment is None else len(assignment.chunks)
        weight = max(0.0, float(path_weights[index]))
        path_records.append(
            {
                "kind": path.kind,
                "target_device": int(path.target_device),
                "relay_device": None if path.kind != "relay" else int(path.relay_device),
                "effective_bw_gbps": float(path.effective_bw_gbps),
                "scheduler_weight_gbps": weight,
                "scheduler_weight_source": (
                    "explicit"
                    if path.scheduler_weight_gbps is not None
                    and float(path.scheduler_weight_gbps) > 0.0
                    else "runtime_pressure_fallback"
                ),
                "runtime_pressure": float(path.runtime_pressure),
                "assigned_bytes": int(assigned_bytes),
                "assigned_chunks": int(assigned_chunks),
                "estimated_finish_seconds": (
                    0.0
                    if assigned_bytes <= 0
                    else float(assigned_bytes) / (max(weight, 1e-12) * 1_000_000_000.0)
                ),
                "source": dict(path.cost_metadata).get(
                    "source",
                    "planner_path_weight",
                ),
            }
        )
    return {
        "source": "planner_engine_minimax_finish_time_assignment",
        "assignment_policy": "minimize_projected_path_finish_time",
        "weight_snapshot_source": "planner_runtime_pressure_snapshot",
        "pool_disabled_reason": pool_disabled_reason,
        "path_count": len(path_records),
        "paths": tuple(path_records),
    }


def _planner_path_key(path: PlannerPath) -> tuple[object, ...]:
    return (
        str(path.kind),
        str(path.direction),
        int(path.target_device),
        int(path.relay_device),
    )


def _make_chunks(total_bytes: int, chunk_bytes: int) -> list[PlannerChunk]:
    chunks = []
    for offset in range(0, total_bytes, chunk_bytes):
        size = min(chunk_bytes, total_bytes - offset)
        chunks.append(PlannerChunk(src_offset=offset, dst_offset=offset, bytes=size))
    return chunks


def _range_fields(range_item) -> tuple[int, int, int]:
    if isinstance(range_item, dict):
        return (
            int(range_item["src_offset"]),
            int(range_item["dst_offset"]),
            int(range_item["bytes"]),
        )
    if isinstance(range_item, tuple) or isinstance(range_item, list):
        if len(range_item) != 3:
            raise ValueError("range tuples must be (src_offset, dst_offset, bytes)")
        return int(range_item[0]), int(range_item[1]), int(range_item[2])
    return (
        int(getattr(range_item, "src_offset")),
        int(getattr(range_item, "dst_offset")),
        int(getattr(range_item, "bytes")),
    )


__all__ = [
    "PlannerEngine",
    "PlannerEngineOptions",
]
