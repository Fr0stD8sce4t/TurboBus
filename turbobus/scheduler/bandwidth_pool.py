from __future__ import annotations

from collections.abc import Mapping

from ..topology.bandwidth_model import edge_availability
from ..topology.pcie_fabric import pcie_fabric_from_mapping


def bandwidth_pool_from_runtime_state(
    runtime_state: Mapping[str, object] | None,
) -> dict[str, object]:
    if not isinstance(runtime_state, Mapping):
        return _empty_pool("missing_runtime_state")
    pool = runtime_state.get("pcie_bandwidth_pool")
    if isinstance(pool, Mapping):
        return dict(pool)
    return _empty_pool("missing_pcie_bandwidth_pool")


def build_bandwidth_pool_snapshot(
    *,
    pcie_fabric: Mapping[str, object],
    edge_load: Mapping[str, Mapping[str, object]] | None = None,
) -> dict[str, object]:
    fabric = pcie_fabric_from_mapping(pcie_fabric)
    loads = edge_load or {}
    edge_records = {
        edge.edge_id: edge_availability(
            edge_id=edge.edge_id,
            capacity_gbps=edge.capacity_gbps,
            load=loads.get(edge.edge_id),
        ).as_dict()
        for edge in fabric.edges
    }
    paths = {}
    for path in fabric.paths:
        path_edges = tuple(
            edge_records[edge_id]
            for edge_id in path.edge_ids
            if edge_id in edge_records
        )
        h2d_values = [
            float(edge.get("available_h2d_gbps", 0.0) or 0.0)
            for edge in path_edges
            if float(edge.get("capacity_gbps", 0.0) or 0.0) > 0.0
        ]
        d2h_values = [
            float(edge.get("available_d2h_gbps", 0.0) or 0.0)
            for edge in path_edges
            if float(edge.get("capacity_gbps", 0.0) or 0.0) > 0.0
        ]
        paths[int(path.device_id)] = {
            "device_id": int(path.device_id),
            "edge_ids": path.edge_ids,
            "capacity_gbps": float(path.capacity_gbps),
            "available_h2d_gbps": min(h2d_values) if h2d_values else 0.0,
            "available_d2h_gbps": min(d2h_values) if d2h_values else 0.0,
            "load_known": (
                all(bool(edge.get("load_known", False)) for edge in path_edges)
                if path_edges
                else False
            ),
            "source": "daemon_pcie_bandwidth_pool",
        }
    return {
        "source": "daemon_pcie_bandwidth_pool",
        "topology_snapshot_id": fabric.snapshot_id,
        "topology_version": fabric.version,
        "available": bool(paths),
        "paths": paths,
        "edges": edge_records,
        "metadata": dict(fabric.metadata),
    }


def _empty_pool(reason: str) -> dict[str, object]:
    return {
        "source": "daemon_pcie_bandwidth_pool",
        "available": False,
        "reason": str(reason),
        "paths": {},
        "edges": {},
    }


__all__ = [
    "bandwidth_pool_from_runtime_state",
    "build_bandwidth_pool_snapshot",
]
