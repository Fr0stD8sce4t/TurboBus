from __future__ import annotations

from collections.abc import Mapping

from ..planner_types import PlannerPath
from .bandwidth_pool import bandwidth_pool_from_runtime_state


def path_bandwidth_view(
    *,
    path: PlannerPath,
    runtime_state: Mapping[str, object] | None,
) -> dict[str, object]:
    pool = bandwidth_pool_from_runtime_state(runtime_state)
    path_record = dict(pool.get("paths", {}).get(int(path.target_device), {}) or {})
    if path.kind == "relay" and path.relay_device >= 0:
        relay_record = dict(pool.get("paths", {}).get(int(path.relay_device), {}) or {})
        available_h2d = min(
            float(path_record.get("available_h2d_gbps", 0.0) or 0.0),
            float(relay_record.get("available_h2d_gbps", 0.0) or 0.0),
        )
        available_d2h = min(
            float(path_record.get("available_d2h_gbps", 0.0) or 0.0),
            float(relay_record.get("available_d2h_gbps", 0.0) or 0.0),
        )
        source = "daemon_pcie_bandwidth_pool_relay"
    else:
        available_h2d = float(path_record.get("available_h2d_gbps", 0.0) or 0.0)
        available_d2h = float(path_record.get("available_d2h_gbps", 0.0) or 0.0)
        source = "daemon_pcie_bandwidth_pool_direct"
    return {
        "kind": str(path.kind),
        "target_device": int(path.target_device),
        "relay_device": int(path.relay_device) if path.relay_device >= 0 else None,
        "available_h2d_gbps": available_h2d,
        "available_d2h_gbps": available_d2h,
        "path_available_gbps": (
            available_d2h if str(path.direction).lower() == "d2h" else available_h2d
        ),
        "source": source,
        "load_known": bool(path_record.get("load_known", False)),
        "path_edges": tuple(path_record.get("edge_ids", ()) or ()),
        "bandwidth_pool": pool,
    }


__all__ = ["path_bandwidth_view"]
