from __future__ import annotations

from collections.abc import Iterable, Mapping

from ..schema import (
    BufferRegistration,
    DaemonResponse,
    JobIdentity,
    SchedulingDecision,
)


def topology_unavailable_response(error: str) -> DaemonResponse:
    return DaemonResponse(
        ok=False,
        error=str(error),
    )


def buffer_snapshot_record(buffer: BufferRegistration) -> dict[str, object]:
    return {
        "buffer_id": buffer.buffer_id,
        "job_id": buffer.job_id,
        "kind": str(buffer.kind),
        "size_bytes": int(buffer.size_bytes),
        "device_index": buffer.device_index,
        "address": buffer.address,
        "pinned": bool(buffer.pinned),
        "handle_type": str(buffer.handle_type),
        "metadata": dict(buffer.metadata),
    }


def relay_path_capabilities(
    inventory,
    *,
    relay_gpu: int,
    target_gpu: int | None,
    fabric_links: list[dict[str, object]],
) -> dict[str, object]:
    pcie_paths = [
        path for path in inventory.pcie_paths if path.device_id == int(relay_gpu)
    ]
    pcie_path = pcie_paths[0] if pcie_paths else None
    enabled_fabric_links = [
        link for link in fabric_links if bool(link.get("enabled", False))
    ]
    fabric_bandwidths = [
        float(link.get("bandwidth_gbps", 0.0) or 0.0)
        for link in enabled_fabric_links
    ]
    fabric_bandwidth_sources = sorted(
        {
            str(link.get("bandwidth_source"))
            for link in enabled_fabric_links
            if link.get("bandwidth_source") is not None
        }
    )
    pcie_bandwidth = 0.0 if pcie_path is None else pcie_path.bandwidth_gbps
    fabric_bandwidth = sum(fabric_bandwidths)
    return {
        "relay_gpu": int(relay_gpu),
        "target_gpu": target_gpu,
        "has_pcie_path": pcie_path is not None,
        "pcie_root_complex": None if pcie_path is None else pcie_path.root_complex,
        "pcie_numa_node": None if pcie_path is None else pcie_path.numa_node,
        "pcie_link_generation": (
            None if pcie_path is None else pcie_path.link_generation
        ),
        "pcie_link_width": None if pcie_path is None else pcie_path.link_width,
        "pcie_negotiated_speed_gtps": (
            None if pcie_path is None else pcie_path.negotiated_speed_gtps
        ),
        "pcie_bandwidth_gbps": pcie_bandwidth,
        "pcie_bandwidth_source": (
            None if pcie_path is None else pcie_path.bandwidth_source
        ),
        "pcie_switch_hierarchy": (
            [] if pcie_path is None else list(pcie_path.switch_hierarchy)
        ),
        "fabric_link_count": len(fabric_links),
        "enabled_fabric_link_count": len(enabled_fabric_links),
        "fabric_kinds": sorted(
            {str(link.get("fabric")) for link in enabled_fabric_links}
        ),
        "fabric_capabilities": sorted(
            {
                str(link.get("capability"))
                for link in enabled_fabric_links
                if link.get("capability") is not None
            }
        ),
        "fabric_bandwidth_gbps": fabric_bandwidth,
        "fabric_bandwidth_sources": fabric_bandwidth_sources,
        "p2p_enabled": bool(enabled_fabric_links),
        "pcie_trusted": pcie_path is not None and pcie_bandwidth > 0.0,
        "fabric_trusted": bool(enabled_fabric_links) and fabric_bandwidth > 0.0,
        "topology_trusted": (
            pcie_path is not None
            and pcie_bandwidth > 0.0
            and bool(enabled_fabric_links)
            and fabric_bandwidth > 0.0
        ),
    }


def fabric_capability_summary_with_snapshot(
    summary: object,
    *,
    inventory,
) -> dict[str, object]:
    result = dict(summary) if isinstance(summary, Mapping) else {}
    result.setdefault("source", "daemon_topology_fabric_capability_summary")
    result["topology_snapshot_id"] = inventory.topology_snapshot_id()
    result["topology_version"] = int(inventory.version)
    result["inventory_source"] = inventory.source
    result["inventory_discovered_at"] = float(inventory.discovered_at)
    return result


def relay_ranges_from_plan(
    plan: dict[str, object],
    *,
    relay_gpu: int | Iterable[int],
    direction: str,
) -> tuple[dict[str, int], ...]:
    if not isinstance(plan, dict):
        raise ValueError("transfer plan is unavailable")
    ranges: list[dict[str, int]] = []
    if isinstance(relay_gpu, int):
        relays = {int(relay_gpu)}
    else:
        relays = {int(gpu) for gpu in relay_gpu}
    if not relays:
        raise ValueError("daemon plan has no authorized relay chunks")
    requested_direction = str(direction).lower()
    for assignment in plan.get("assignments", ()) or ():
        if not isinstance(assignment, dict):
            raise ValueError("transfer plan assignment must be an object")
        path = assignment.get("path")
        if not isinstance(path, dict):
            raise ValueError("transfer plan assignment path must be an object")
        if str(path.get("kind", "")).lower() != "relay":
            continue
        if str(path.get("direction", "")).lower() != requested_direction:
            continue
        if int(path.get("relay_device", -1)) not in relays:
            continue
        for chunk in assignment.get("chunks", ()) or ():
            if not isinstance(chunk, dict):
                raise ValueError("transfer plan chunk must be an object")
            ranges.append(
                {
                    "src_offset": int(chunk["src_offset"]),
                    "dst_offset": int(chunk["dst_offset"]),
                    "bytes": int(chunk["bytes"]),
                }
            )
    if not ranges:
        raise ValueError("daemon plan has no authorized relay chunks")
    return tuple(ranges)


def relay_devices_from_plan(
    plan: dict[str, object],
    *,
    direction: str,
) -> set[int]:
    if not isinstance(plan, dict):
        raise ValueError("transfer plan is unavailable")
    relays: set[int] = set()
    requested_direction = str(direction).lower()
    for assignment in plan.get("assignments", ()) or ():
        if not isinstance(assignment, dict):
            raise ValueError("transfer plan assignment must be an object")
        path = assignment.get("path")
        if not isinstance(path, dict):
            raise ValueError("transfer plan assignment path must be an object")
        if str(path.get("kind", "")).lower() != "relay":
            continue
        if str(path.get("direction", "")).lower() != requested_direction:
            continue
        if assignment.get("chunks"):
            relays.add(int(path.get("relay_device", -1)))
    return relays


def decision_is_direct_only(decision: SchedulingDecision) -> bool:
    assignments = decision.plan.get("assignments", ()) or ()
    if not assignments:
        return False
    for assignment in assignments:
        if not isinstance(assignment, dict):
            return False
        path = assignment.get("path")
        if not isinstance(path, dict):
            return False
        if str(path.get("kind", "")).lower() != "direct":
            return False
    return True


def job_identity_conflicts(
    existing: JobIdentity,
    incoming: JobIdentity,
) -> bool:
    return any(
        getattr(existing, field_name) != getattr(incoming, field_name)
        for field_name in (
            "job_id",
            "user_id",
            "session_id",
            "container_id",
            "process_id",
        )
    )


def buffer_registration_conflicts(
    existing: BufferRegistration,
    incoming: BufferRegistration,
) -> bool:
    return any(
        getattr(existing, field_name) != getattr(incoming, field_name)
        for field_name in (
            "buffer_id",
            "job_id",
            "kind",
            "size_bytes",
            "device_index",
            "address",
            "pinned",
            "handle_type",
            "metadata",
        )
    )


__all__ = [
    "buffer_registration_conflicts",
    "buffer_snapshot_record",
    "decision_is_direct_only",
    "fabric_capability_summary_with_snapshot",
    "job_identity_conflicts",
    "relay_devices_from_plan",
    "relay_path_capabilities",
    "relay_ranges_from_plan",
    "topology_unavailable_response",
]
