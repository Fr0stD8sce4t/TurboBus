from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class PcieEdgeLoad:
    edge_id: str
    h2d_used_gbps: float = 0.0
    d2h_used_gbps: float = 0.0
    source: str = "unknown"
    known: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "edge_id", str(self.edge_id))
        object.__setattr__(self, "h2d_used_gbps", max(0.0, float(self.h2d_used_gbps)))
        object.__setattr__(self, "d2h_used_gbps", max(0.0, float(self.d2h_used_gbps)))
        object.__setattr__(self, "source", str(self.source))
        object.__setattr__(self, "known", bool(self.known))

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class PcieEdgeAvailability:
    edge_id: str
    capacity_gbps: float
    h2d_used_gbps: float
    d2h_used_gbps: float
    available_h2d_gbps: float
    available_d2h_gbps: float
    load_known: bool
    source: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def edge_availability(
    *,
    edge_id: str,
    capacity_gbps: float,
    load: PcieEdgeLoad | Mapping[str, object] | None = None,
) -> PcieEdgeAvailability:
    normalized_load = _edge_load(load, edge_id=str(edge_id))
    capacity = max(0.0, float(capacity_gbps))
    h2d_used = normalized_load.h2d_used_gbps if normalized_load.known else 0.0
    d2h_used = normalized_load.d2h_used_gbps if normalized_load.known else 0.0
    return PcieEdgeAvailability(
        edge_id=str(edge_id),
        capacity_gbps=capacity,
        h2d_used_gbps=h2d_used,
        d2h_used_gbps=d2h_used,
        available_h2d_gbps=max(0.0, capacity - h2d_used),
        available_d2h_gbps=max(0.0, capacity - d2h_used),
        load_known=normalized_load.known,
        source=normalized_load.source,
    )


def path_available_bandwidth(
    edge_records: Mapping[str, Mapping[str, object]],
    edge_ids: tuple[str, ...],
    direction: str,
) -> tuple[float, tuple[dict[str, object], ...], str]:
    normalized_direction = str(direction).lower()
    edge_details = tuple(
        dict(edge_records[edge_id])
        for edge_id in edge_ids
        if edge_id in edge_records
    )
    if not edge_details:
        return 0.0, (), "missing_pcie_fabric_path"
    field_name = (
        "available_d2h_gbps"
        if normalized_direction == "d2h"
        else "available_h2d_gbps"
    )
    capacity_values = [
        float(edge.get(field_name, 0.0) or 0.0)
        for edge in edge_details
        if float(edge.get("capacity_gbps", 0.0) or 0.0) > 0.0
    ]
    if not capacity_values:
        return 0.0, edge_details, "missing_pcie_capacity"
    return min(capacity_values), edge_details, "pcie_fabric_bandwidth_pool"


def _edge_load(
    value: PcieEdgeLoad | Mapping[str, object] | None,
    *,
    edge_id: str,
) -> PcieEdgeLoad:
    if isinstance(value, PcieEdgeLoad):
        return value
    if not isinstance(value, Mapping):
        return PcieEdgeLoad(edge_id=edge_id, source="load_unknown", known=False)
    return PcieEdgeLoad(
        edge_id=str(value.get("edge_id", edge_id)),
        h2d_used_gbps=float(value.get("h2d_used_gbps", 0.0) or 0.0),
        d2h_used_gbps=float(value.get("d2h_used_gbps", 0.0) or 0.0),
        source=str(value.get("source", "daemon_pcie_load_sampler")),
        known=bool(value.get("known", False)),
    )


__all__ = [
    "PcieEdgeAvailability",
    "PcieEdgeLoad",
    "edge_availability",
    "path_available_bandwidth",
]
