from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping

from ..topology.bandwidth_model import PcieEdgeLoad


def pcie_load_from_active_paths(
    *,
    active_paths: object,
    path_edge_map: Mapping[int, tuple[str, ...]],
    capacity_by_edge: Mapping[str, float],
) -> dict[str, dict[str, object]]:
    bytes_by_edge: dict[str, dict[str, int]] = defaultdict(
        lambda: {"h2d": 0, "d2h": 0}
    )
    for record in _mapping_records(active_paths):
        direction = str(record.get("direction", "h2d")).lower()
        if direction not in {"h2d", "d2h"}:
            direction = "h2d"
        device = _path_pcie_device(record)
        if device is None:
            continue
        for edge_id in path_edge_map.get(int(device), ()):
            bytes_by_edge[str(edge_id)][direction] += int(
                record.get("bytes_total", 0) or 0
            )
    loads: dict[str, dict[str, object]] = {}
    max_bytes = max(
        [sum(direction_bytes.values()) for direction_bytes in bytes_by_edge.values()]
        or [0],
    )
    for edge_id, direction_bytes in bytes_by_edge.items():
        capacity = max(0.0, float(capacity_by_edge.get(edge_id, 0.0) or 0.0))
        if capacity <= 0.0 or max_bytes <= 0:
            load = PcieEdgeLoad(edge_id=edge_id, source="daemon_active_paths", known=False)
        else:
            scale = min(1.0, sum(direction_bytes.values()) / max(max_bytes, 1))
            h2d_share = direction_bytes["h2d"] / max(sum(direction_bytes.values()), 1)
            d2h_share = direction_bytes["d2h"] / max(sum(direction_bytes.values()), 1)
            load = PcieEdgeLoad(
                edge_id=edge_id,
                h2d_used_gbps=capacity * scale * h2d_share,
                d2h_used_gbps=capacity * scale * d2h_share,
                source="daemon_active_paths",
                known=True,
            )
        loads[edge_id] = load.as_dict()
    return loads


def _path_pcie_device(record: Mapping[str, object]) -> int | None:
    kind = str(record.get("kind", "")).lower()
    if kind == "relay" and record.get("relay_device") is not None:
        return int(record["relay_device"])
    if record.get("target_device") is not None:
        return int(record["target_device"])
    return None


def _mapping_records(value: object) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list | tuple):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


__all__ = ["pcie_load_from_active_paths"]
