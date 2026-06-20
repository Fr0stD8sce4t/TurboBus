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
            "edges": tuple(dict(edge) for edge in path_edges),
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


def build_runtime_edge_load_snapshot(
    *,
    fabric,
    hardware_sample,
    active_path_load,
) -> dict[str, dict[str, object]]:
    fabric_record = fabric.as_dict() if hasattr(fabric, "as_dict") else dict(fabric)
    fabric_model = pcie_fabric_from_mapping(fabric_record)
    hardware_by_device = _hardware_counters_by_device(hardware_sample)
    active_load = active_path_load if isinstance(active_path_load, Mapping) else {}
    loads: dict[str, dict[str, object]] = {}
    for edge in fabric_model.edges:
        edge_id = str(edge.edge_id)
        device_ids = tuple(
            int(path.device_id)
            for path in fabric_model.paths
            if edge_id in tuple(path.edge_ids)
        )
        hardware_counters = [
            hardware_by_device[device_id]
            for device_id in device_ids
            if device_id in hardware_by_device
        ]
        if hardware_counters:
            sampled_at_values = [
                float(getattr(counter, "sampled_at", 0.0) or 0.0)
                for counter in hardware_counters
            ]
            loads[edge_id] = {
                "edge_id": edge_id,
                "source": "hardware",
                "load_source": "hardware",
                "known": True,
                "load_known": True,
                "confidence": 1.0,
                "sample_age_ms": max(
                    float(getattr(counter, "sample_age_ms", 0.0) or 0.0)
                    for counter in hardware_counters
                ),
                "sampled_at": max(sampled_at_values or [0.0]),
                "h2d_used_gbps": sum(
                    float(getattr(counter, "h2d_used_gbps", 0.0) or 0.0)
                    for counter in hardware_counters
                ),
                "d2h_used_gbps": sum(
                    float(getattr(counter, "d2h_used_gbps", 0.0) or 0.0)
                    for counter in hardware_counters
                ),
            }
            continue
        active_record = active_load.get(edge_id)
        if isinstance(active_record, Mapping) and bool(
            active_record.get("known", active_record.get("load_known", False))
        ):
            loads[edge_id] = {
                **dict(active_record),
                "edge_id": edge_id,
                "source": str(active_record.get("source", "daemon_active_paths")),
                "load_source": "active_paths",
                "load_known": True,
                "known": True,
                "confidence": 0.45,
                "sample_age_ms": active_record.get("sample_age_ms"),
                "h2d_used_gbps": float(active_record.get("h2d_used_gbps", 0.0) or 0.0),
                "d2h_used_gbps": float(active_record.get("d2h_used_gbps", 0.0) or 0.0),
            }
            continue
        loads[edge_id] = {
            "edge_id": edge_id,
            "source": "unknown",
            "load_source": "unknown",
            "known": False,
            "load_known": False,
            "confidence": 0.0,
            "sample_age_ms": None,
            "h2d_used_gbps": 0.0,
            "d2h_used_gbps": 0.0,
        }
    return loads


def _empty_pool(reason: str) -> dict[str, object]:
    return {
        "source": "daemon_pcie_bandwidth_pool",
        "available": False,
        "reason": str(reason),
        "paths": {},
        "edges": {},
    }


def _hardware_counters_by_device(hardware_sample) -> dict[int, object]:
    if hardware_sample is None or not bool(getattr(hardware_sample, "known", False)):
        return {}
    by_device = getattr(hardware_sample, "by_device", None)
    if callable(by_device):
        return dict(by_device())
    counters = getattr(hardware_sample, "counters", ()) or ()
    result = {}
    for counter in counters:
        try:
            result[int(getattr(counter, "device_id"))] = counter
        except (TypeError, ValueError):
            continue
    return result


__all__ = [
    "bandwidth_pool_from_runtime_state",
    "build_bandwidth_pool_snapshot",
    "build_runtime_edge_load_snapshot",
]
