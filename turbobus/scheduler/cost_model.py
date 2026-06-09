from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from ..planner_types import PlannerTransferPlan
from ..schema import TransferMode
from .load_feedback import RuntimeLoadView


@dataclass(frozen=True)
class RelayProfile:
    relay_device: int
    target_device: int
    h2d_bw_gbps: float
    d2h_bw_gbps: float
    p2p_bw_gbps: float
    effective_bw_gbps: float
    effective_d2h_bw_gbps: float
    p2p_enabled: bool
    scheduler_weight_h2d_gbps: float = 0.0
    scheduler_weight_d2h_gbps: float = 0.0
    runtime_pressure_h2d: float = 0.0
    runtime_pressure_d2h: float = 0.0
    cost_metadata: dict[str, object] | None = None


@dataclass(frozen=True)
class Profile:
    target_device: int
    direct_h2d_bw_gbps: float
    direct_d2h_bw_gbps: float
    relays: tuple[RelayProfile, ...]
    direct_scheduler_weight_h2d_gbps: float = 0.0
    direct_scheduler_weight_d2h_gbps: float = 0.0
    direct_runtime_pressure_h2d: float = 0.0
    direct_runtime_pressure_d2h: float = 0.0
    cost_metadata: dict[str, object] | None = None


def scheduler_cost_model_metadata(
    *,
    plan: PlannerTransferPlan,
    profile: Profile,
    runtime_view: RuntimeLoadView,
    direction: str,
    total_bytes: int,
) -> dict[str, object]:
    normalized_direction = str(direction).lower()
    direct_bw = profile_direct_bandwidth(profile, normalized_direction)
    path_costs: list[dict[str, object]] = []
    total_estimated_seconds = 0.0
    direct_bytes = 0
    relay_bytes = 0
    for assignment in plan.assignments:
        path = assignment.path
        bytes_count = sum(chunk.bytes for chunk in assignment.chunks)
        bandwidth = max(path_scheduler_weight(path), 1e-12)
        estimated_seconds = estimated_transfer_seconds(bytes_count, bandwidth)
        total_estimated_seconds += estimated_seconds
        if path.kind == "relay":
            relay_bytes += bytes_count
            relay_pressure = runtime_view.relay_cost_pressure(
                int(path.relay_device),
                normalized_direction,
            )
            pressure_summary = runtime_view.scheduler_pressure_summary(
                normalized_direction,
            )
        else:
            direct_bytes += bytes_count
            relay_pressure = runtime_view.direct_cost_pressure(normalized_direction)
            pressure_summary = runtime_view.scheduler_pressure_summary(
                normalized_direction,
            )
        path_costs.append(
            {
                "kind": str(path.kind),
                "target_device": int(path.target_device),
                "relay_device": None if path.kind != "relay" else int(path.relay_device),
                "bytes": int(bytes_count),
                "chunk_count": len(assignment.chunks),
                "effective_bw_gbps": float(path.effective_bw_gbps),
                "scheduler_weight_gbps": path_scheduler_weight(path),
                "allocation_ratio": (
                    0.0 if total_bytes <= 0 else float(bytes_count) / float(total_bytes)
                ),
                "estimated_seconds": estimated_seconds,
                "runtime_pressure": relay_pressure,
                "pressure_summary": dict(pressure_summary),
                "cost_metadata": dict(path.cost_metadata),
                "planner_cost_source": dict(path.cost_metadata).get("source"),
                "admission_state": dict(path.cost_metadata).get("admission_state"),
                "admission_reason": dict(path.cost_metadata).get("admission_reason"),
            }
        )
    relay_profiles = {int(relay.relay_device): relay for relay in profile.relays}
    direct_weight = profile_direct_scheduler_weight(profile, normalized_direction)
    candidate_paths: list[dict[str, object]] = [
        {
            "kind": "direct",
            "target_device": int(profile.target_device),
            "relay_device": None,
            "effective_bw_gbps": direct_bw,
            "scheduler_weight_gbps": direct_weight,
            "runtime_pressure": runtime_view.direct_cost_pressure(normalized_direction),
            "estimated_full_transfer_seconds": estimated_transfer_seconds(
                total_bytes,
                max(direct_weight, 1e-12),
            ),
            "topology_binding": profile_topology_binding(profile),
        }
    ]
    for relay_device, relay in sorted(relay_profiles.items()):
        relay_bw = profile_relay_bandwidth(relay, normalized_direction)
        relay_weight = profile_relay_scheduler_weight(relay, normalized_direction)
        candidate_paths.append(
            {
                "kind": "relay",
                "target_device": int(relay.target_device),
                "relay_device": int(relay_device),
                "effective_bw_gbps": relay_bw,
                "scheduler_weight_gbps": relay_weight,
                "runtime_pressure": runtime_view.relay_cost_pressure(
                    relay_device,
                    normalized_direction,
                ),
                "estimated_full_transfer_seconds": estimated_transfer_seconds(
                    total_bytes,
                    max(relay_weight, 1e-12),
                ),
                "relay_load": dict(runtime_view.relay_load.get(relay_device, {})),
                "topology_binding": relay_topology_binding(relay),
            }
        )
    resolved_mode = resolved_mode_for_plan(plan).value
    return {
        "source": "daemon_scheduler_runtime_cost_model",
        "direction": normalized_direction,
        "resolved_mode": resolved_mode,
        "total_bytes": int(total_bytes),
        "direct_bytes": int(direct_bytes),
        "relay_bytes": int(relay_bytes),
        "estimated_seconds": total_estimated_seconds,
        "path_costs": tuple(path_costs),
        "candidate_paths": tuple(candidate_paths),
        "runtime_pressure_summary": runtime_view.scheduler_pressure_summary(
            normalized_direction,
        ),
        "fabric_abstraction": fabric_abstraction_metadata(profile),
        "profile_binding": profile_topology_binding(profile),
        "profile_import": profile_import_metadata(profile),
        "profile_measurements": profile_measurement_metadata(profile),
        "planner_cost_metadata": dict(getattr(plan, "cost_metadata", {}) or {}),
        "cost_inputs": {
            "profile_measurements": profile_measurement_metadata(profile),
            "runtime_pressure": runtime_view.scheduler_pressure_summary(
                normalized_direction,
            ),
            "workload_kind": runtime_view.workload_kind,
            "priority": int(runtime_view.priority),
        },
    }


def estimated_transfer_seconds(bytes_count: int, bandwidth_gbps: float) -> float:
    bandwidth_bytes_per_second = max(float(bandwidth_gbps), 1e-12) * 1_000_000_000.0
    return float(max(0, int(bytes_count))) / bandwidth_bytes_per_second


def path_scheduler_weight(path) -> float:
    weight = getattr(path, "scheduler_weight_gbps", None)
    if weight is not None:
        return max(0.0, float(weight))
    return max(0.0, float(getattr(path, "effective_bw_gbps", 0.0) or 0.0))


def profile_direct_scheduler_weight(profile: Profile, direction: str) -> float:
    if str(direction).lower() == "d2h":
        return max(
            0.0,
            float(
                profile.direct_scheduler_weight_d2h_gbps
                or profile.direct_scheduler_weight_h2d_gbps
                or profile_direct_bandwidth(profile, direction)
            ),
        )
    return max(
        0.0,
        float(
            profile.direct_scheduler_weight_h2d_gbps
            or profile_direct_bandwidth(profile, direction)
        ),
    )


def profile_relay_scheduler_weight(relay: RelayProfile, direction: str) -> float:
    if str(direction).lower() == "d2h":
        return max(
            0.0,
            float(
                relay.scheduler_weight_d2h_gbps
                or relay.scheduler_weight_h2d_gbps
                or profile_relay_bandwidth(relay, direction)
            ),
        )
    return max(
        0.0,
        float(
            relay.scheduler_weight_h2d_gbps
            or profile_relay_bandwidth(relay, direction)
        ),
    )


def profile_topology_binding(profile: Profile) -> dict[str, object]:
    metadata = dict(profile.cost_metadata or {})
    binding = metadata.get("topology_binding")
    return dict(binding) if isinstance(binding, Mapping) else {}


def fabric_abstraction_metadata(profile: Profile) -> dict[str, object]:
    binding = profile_topology_binding(profile)
    relay_topology = tuple(
        item
        for item in binding.get("relay_topology", ()) or ()
        if isinstance(item, Mapping)
    )
    relay_capabilities = []
    for item in relay_topology:
        topology = item.get("topology")
        if not isinstance(topology, Mapping):
            continue
        relay_capabilities.append(
            {
                "relay_gpu": item.get("relay_gpu"),
                "target_gpu": topology.get("target_gpu"),
                "pcie_bandwidth_gbps": float(
                    topology.get("pcie_bandwidth_gbps", 0.0) or 0.0
                ),
                "fabric_bandwidth_gbps": float(
                    topology.get("fabric_bandwidth_gbps", 0.0) or 0.0
                ),
                "pcie_numa_node": topology.get("pcie_numa_node"),
                "pcie_root_complex": topology.get("pcie_root_complex"),
                "fabric_kinds": tuple(topology.get("fabric_kinds", ()) or ()),
                "fabric_capabilities": tuple(
                    topology.get("fabric_capabilities", ()) or ()
                ),
                "pcie_trusted": bool(topology.get("pcie_trusted", False)),
                "fabric_trusted": bool(topology.get("fabric_trusted", False)),
                "topology_trusted": bool(topology.get("topology_trusted", False)),
            }
        )
    return {
        "source": "daemon_scheduler_fabric_abstraction",
        "topology_snapshot_id": binding.get("topology_snapshot_id"),
        "topology_version": binding.get("topology_version"),
        "target_gpu": binding.get("target_gpu"),
        "inventory_source": binding.get("inventory_source"),
        "relay_count": len(relay_capabilities),
        "trusted_relay_count": sum(
            1 for item in relay_capabilities if bool(item.get("topology_trusted", False))
        ),
        "aggregate_relay_pcie_bandwidth_gbps": sum(
            float(item.get("pcie_bandwidth_gbps", 0.0) or 0.0)
            for item in relay_capabilities
        ),
        "aggregate_fabric_bandwidth_gbps": sum(
            float(item.get("fabric_bandwidth_gbps", 0.0) or 0.0)
            for item in relay_capabilities
        ),
        "fabric_kinds": tuple(
            sorted(
                {
                    str(kind)
                    for item in relay_capabilities
                    for kind in item.get("fabric_kinds", ()) or ()
                }
            )
        ),
        "fabric_capabilities": tuple(
            sorted(
                {
                    str(capability)
                    for item in relay_capabilities
                    for capability in item.get("fabric_capabilities", ()) or ()
                }
            )
        ),
        "relay_capabilities": tuple(relay_capabilities),
    }


def profile_import_metadata(profile: Profile) -> dict[str, object]:
    metadata = dict(profile.cost_metadata or {})
    profile_import = metadata.get("profile_import")
    if not isinstance(profile_import, Mapping):
        return {"available": False}
    return {
        "available": True,
        "source": profile_import.get("source"),
        "measurement_source": profile_import.get("measurement_source"),
        "profile_bytes": int(profile_import.get("profile_bytes", 0) or 0),
        "record_count": int(profile_import.get("record_count", 0) or 0),
        "record_types": tuple(profile_import.get("record_types", ()) or ()),
        "relay_devices": tuple(profile_import.get("relay_devices", ()) or ()),
        "production_evidence": bool(
            profile_import.get("production_evidence", False)
        ),
    }


def profile_measurement_metadata(profile: Profile) -> dict[str, object]:
    metadata = dict(profile.cost_metadata or {})
    records = tuple(
        item
        for item in metadata.get("measurement_records", ()) or ()
        if isinstance(item, Mapping)
    )
    if not records:
        return {"available": False}
    by_type: dict[str, int] = {}
    relays: set[int] = set()
    for record in records:
        record_type = str(record.get("record_type", ""))
        by_type[record_type] = by_type.get(record_type, 0) + 1
        relay_device = record.get("relay_device")
        if relay_device is not None:
            relays.add(int(relay_device))
    return {
        "available": True,
        "record_count": len(records),
        "records_by_type": by_type,
        "relay_devices": tuple(sorted(relays)),
        "has_direct_pcie": by_type.get("direct_pcie", 0) >= 2,
        "has_relay_pcie": by_type.get("relay_pcie", 0) > 0,
        "has_gpu_fabric": by_type.get("gpu_fabric", 0) > 0,
    }


def relay_topology_binding(relay: RelayProfile) -> dict[str, object]:
    metadata = dict(relay.cost_metadata or {})
    topology = metadata.get("topology")
    return dict(topology) if isinstance(topology, Mapping) else {}


def profile_direct_bandwidth(profile: Profile, direction: str) -> float:
    if str(direction).lower() == "d2h":
        return max(
            0.0,
            float(profile.direct_d2h_bw_gbps or profile.direct_h2d_bw_gbps or 0.0),
        )
    return max(0.0, float(profile.direct_h2d_bw_gbps or 0.0))


def profile_relay_bandwidth(relay: RelayProfile, direction: str) -> float:
    if str(direction).lower() == "d2h":
        return max(
            0.0,
            float(relay.effective_d2h_bw_gbps or relay.effective_bw_gbps or 0.0),
        )
    return max(0.0, float(relay.effective_bw_gbps or 0.0))


def profile_cost_context(
    profile_entry: Mapping[str, object] | None,
) -> dict[str, object]:
    if not isinstance(profile_entry, Mapping):
        return {}
    binding = profile_entry.get("topology_binding")
    context = {
        "profile_source": "daemon_profile_cache",
        "profile_updated_at": float(profile_entry.get("updated_at", 0.0) or 0.0),
        "profile_bytes": int(profile_entry.get("profile_bytes", 0) or 0),
    }
    profile_import = profile_entry.get("profile_import")
    if isinstance(profile_import, Mapping):
        context["profile_import"] = dict(profile_import)
    profile = profile_entry.get("profile")
    if isinstance(profile, Mapping):
        records = profile.get("measurement_records")
        if isinstance(records, tuple | list):
            context["measurement_records"] = tuple(
                dict(item)
                for item in records
                if isinstance(item, Mapping)
            )
    if isinstance(binding, Mapping):
        context["topology_binding"] = {
            "source": binding.get("source"),
            "topology_snapshot_id": binding.get("topology_snapshot_id"),
            "topology_version": binding.get("topology_version"),
            "inventory_source": binding.get("inventory_source"),
            "target_gpu": binding.get("target_gpu"),
            "relay_gpus": tuple(binding.get("relay_gpus", ()) or ()),
        }
    return context


def relay_cost_context(
    profile_entry: Mapping[str, object] | None,
    relay_device: int,
) -> dict[str, object]:
    context = profile_cost_context(profile_entry)
    if not isinstance(profile_entry, Mapping):
        return context
    binding = profile_entry.get("topology_binding")
    if not isinstance(binding, Mapping):
        return context
    for item in binding.get("relay_topology", ()) or ():
        if not isinstance(item, Mapping):
            continue
        if int(item.get("relay_gpu", -1)) != int(relay_device):
            continue
        topology = item.get("topology")
        if isinstance(topology, Mapping):
            context["topology"] = dict(topology)
        context["topology_reason"] = item.get("reason")
        break
    return context


def direct_fallback_profile(target_gpu: int) -> Profile:
    return Profile(
        target_device=int(target_gpu),
        direct_h2d_bw_gbps=1.0,
        direct_d2h_bw_gbps=1.0,
        relays=(),
        direct_scheduler_weight_h2d_gbps=1.0,
        direct_scheduler_weight_d2h_gbps=1.0,
        cost_metadata={"profile_source": "direct_fallback_profile"},
    )


def direct_scheduler_weights(
    direct_h2d: float,
    direct_d2h: float,
    *,
    runtime_view: RuntimeLoadView,
    admission_state: str,
) -> tuple[float, float, dict[str, object]]:
    h2d_pressure = runtime_view.direct_cost_pressure("h2d")
    d2h_pressure = runtime_view.direct_cost_pressure("d2h")
    h2d_policy = runtime_view.adaptive_policy_for_path(
        path_kind="direct",
        direction="h2d",
        admission_state=admission_state,
    )
    d2h_policy = runtime_view.adaptive_policy_for_path(
        path_kind="direct",
        direction="d2h",
        admission_state=admission_state,
    )
    adjusted_h2d, h2d_score = scheduler_cost_adjusted_bandwidth(
        direct_h2d,
        h2d_pressure,
        runtime_view=runtime_view,
        path_kind="direct",
        admission_state=admission_state,
        adaptive_policy=h2d_policy,
    )
    adjusted_d2h, d2h_score = scheduler_cost_adjusted_bandwidth(
        direct_d2h,
        d2h_pressure,
        runtime_view=runtime_view,
        path_kind="direct",
        admission_state=admission_state,
        adaptive_policy=d2h_policy,
    )
    return (
        adjusted_h2d,
        adjusted_d2h,
        {
            "kind": "direct",
            "relay_device": None,
            "original_h2d_bw_gbps": float(direct_h2d),
            "scheduler_weight_h2d_gbps": adjusted_h2d,
            "original_d2h_bw_gbps": float(direct_d2h),
            "scheduler_weight_d2h_gbps": adjusted_d2h,
            "h2d_pressure": h2d_pressure,
            "d2h_pressure": d2h_pressure,
            "h2d_cost_score": h2d_score,
            "d2h_cost_score": d2h_score,
            "adaptive_policy_h2d": h2d_policy,
            "adaptive_policy_d2h": d2h_policy,
            "h2d_pressure_summary": runtime_view.scheduler_pressure_summary("h2d"),
            "d2h_pressure_summary": runtime_view.scheduler_pressure_summary("d2h"),
            "admission_state": str(admission_state),
            "workload_kind": runtime_view.workload_kind,
            "priority": int(runtime_view.priority),
            "source": "daemon_scheduler_unified_cost_model",
        },
    )


def relay_profile_with_load_feedback(
    relay_profile: RelayProfile,
    *,
    runtime_view: RuntimeLoadView,
    admission_state: str,
    admission_reason: str | None,
) -> tuple[RelayProfile, dict[str, object]]:
    h2d_pressure = runtime_view.relay_cost_pressure(
        relay_profile.relay_device,
        "h2d",
    )
    d2h_pressure = runtime_view.relay_cost_pressure(
        relay_profile.relay_device,
        "d2h",
    )
    directionless_pressure = max(h2d_pressure, d2h_pressure)
    h2d_policy = runtime_view.adaptive_policy_for_path(
        path_kind="relay",
        direction="h2d",
        relay_device=relay_profile.relay_device,
        admission_state=admission_state,
    )
    d2h_policy = runtime_view.adaptive_policy_for_path(
        path_kind="relay",
        direction="d2h",
        relay_device=relay_profile.relay_device,
        admission_state=admission_state,
    )
    adjusted_h2d, h2d_score = scheduler_cost_adjusted_bandwidth(
        relay_profile.effective_bw_gbps,
        h2d_pressure,
        runtime_view=runtime_view,
        path_kind="relay",
        admission_state=admission_state,
        adaptive_policy=h2d_policy,
    )
    adjusted_d2h, d2h_score = scheduler_cost_adjusted_bandwidth(
        relay_profile.effective_d2h_bw_gbps,
        d2h_pressure,
        runtime_view=runtime_view,
        path_kind="relay",
        admission_state=admission_state,
        adaptive_policy=d2h_policy,
    )
    path_cost_model = {
        "source": "daemon_scheduler_unified_cost_model",
        "admission_state": str(admission_state),
        "admission_reason": admission_reason,
        "h2d_cost_score": h2d_score,
        "d2h_cost_score": d2h_score,
        "adaptive_policy_h2d": h2d_policy,
        "adaptive_policy_d2h": d2h_policy,
        "workload_kind": runtime_view.workload_kind,
        "priority": int(runtime_view.priority),
    }
    adjusted = RelayProfile(
        relay_device=relay_profile.relay_device,
        target_device=relay_profile.target_device,
        h2d_bw_gbps=relay_profile.h2d_bw_gbps,
        d2h_bw_gbps=relay_profile.d2h_bw_gbps,
        p2p_bw_gbps=relay_profile.p2p_bw_gbps,
        effective_bw_gbps=relay_profile.effective_bw_gbps,
        effective_d2h_bw_gbps=relay_profile.effective_d2h_bw_gbps,
        p2p_enabled=relay_profile.p2p_enabled,
        scheduler_weight_h2d_gbps=adjusted_h2d,
        scheduler_weight_d2h_gbps=adjusted_d2h,
        runtime_pressure_h2d=h2d_pressure,
        runtime_pressure_d2h=d2h_pressure,
        cost_metadata={
            **dict(relay_profile.cost_metadata or {}),
            **path_cost_model,
            "path_cost_model": path_cost_model,
        },
    )
    relay = int(relay_profile.relay_device)
    return (
        adjusted,
        {
            "kind": "relay",
            "relay_device": relay,
            "original_effective_bw_gbps": float(relay_profile.effective_bw_gbps),
            "scheduler_weight_h2d_gbps": float(adjusted.scheduler_weight_h2d_gbps),
            "original_effective_d2h_bw_gbps": float(
                relay_profile.effective_d2h_bw_gbps
            ),
            "scheduler_weight_d2h_gbps": float(
                adjusted.scheduler_weight_d2h_gbps
            ),
            "pressure": directionless_pressure,
            "h2d_pressure": h2d_pressure,
            "d2h_pressure": d2h_pressure,
            "h2d_cost_score": h2d_score,
            "d2h_cost_score": d2h_score,
            "h2d_pressure_summary": runtime_view.scheduler_pressure_summary("h2d"),
            "d2h_pressure_summary": runtime_view.scheduler_pressure_summary("d2h"),
            "relay_load": dict(runtime_view.relay_load.get(relay, {})),
            "admission_state": str(admission_state),
            "admission_reason": admission_reason,
            "workload_kind": runtime_view.workload_kind,
            "priority": int(runtime_view.priority),
            "source": "daemon_scheduler_unified_cost_model",
        },
    )


def relay_filtered_cost_record(
    relay_profile: RelayProfile,
    *,
    runtime_view: RuntimeLoadView,
    admission_reason: str,
) -> dict[str, object]:
    h2d_pressure = runtime_view.relay_cost_pressure(
        relay_profile.relay_device,
        "h2d",
    )
    d2h_pressure = runtime_view.relay_cost_pressure(
        relay_profile.relay_device,
        "d2h",
    )
    return {
        "kind": "relay",
        "relay_device": int(relay_profile.relay_device),
        "original_effective_bw_gbps": float(relay_profile.effective_bw_gbps),
        "scheduler_weight_h2d_gbps": 0.0,
        "original_effective_d2h_bw_gbps": float(
            relay_profile.effective_d2h_bw_gbps
        ),
        "scheduler_weight_d2h_gbps": 0.0,
        "pressure": max(h2d_pressure, d2h_pressure),
        "h2d_pressure": h2d_pressure,
        "d2h_pressure": d2h_pressure,
        "h2d_cost_score": scheduler_cost_score(
            h2d_pressure,
            runtime_view=runtime_view,
            path_kind="relay",
            admission_state="filtered",
        ),
        "d2h_cost_score": scheduler_cost_score(
            d2h_pressure,
            runtime_view=runtime_view,
            path_kind="relay",
            admission_state="filtered",
        ),
        "relay_load": dict(
            runtime_view.relay_load.get(int(relay_profile.relay_device), {})
        ),
        "admission_state": "filtered",
        "admission_reason": str(admission_reason),
        "workload_kind": runtime_view.workload_kind,
        "priority": int(runtime_view.priority),
        "source": "daemon_scheduler_unified_cost_model",
    }


def scheduler_cost_adjusted_bandwidth(
    bandwidth: float,
    pressure: float,
    *,
    runtime_view: RuntimeLoadView,
    path_kind: str,
    admission_state: str,
    adaptive_policy: Mapping[str, object] | None = None,
) -> tuple[float, float]:
    cost_score = scheduler_cost_score(
        pressure,
        runtime_view=runtime_view,
        path_kind=path_kind,
        admission_state=admission_state,
    )
    normalized_bandwidth = max(0.0, float(bandwidth))
    if normalized_bandwidth <= 0.0:
        return 0.0, cost_score
    multiplier = 1.0
    if isinstance(adaptive_policy, Mapping):
        multiplier = max(0.05, float(adaptive_policy.get("multiplier", 1.0) or 1.0))
    return normalized_bandwidth * multiplier / max(cost_score, 1e-12), cost_score


def scheduler_cost_score(
    pressure: float,
    *,
    runtime_view: RuntimeLoadView,
    path_kind: str,
    admission_state: str,
) -> float:
    admission_penalty = 1.0
    if str(admission_state).lower() == "deferred":
        admission_penalty = 1.18
    elif str(admission_state).lower() not in {"available", "admitted"}:
        admission_penalty = 1.35
    workload_multiplier = runtime_view.workload_path_multiplier(path_kind)
    priority_discount = runtime_view.priority_pressure_discount()
    pressure_component = 1.0 + min(max(0.0, float(pressure)), 4.0)
    return max(
        1e-12,
        pressure_component * admission_penalty * priority_discount / workload_multiplier,
    )


def bandwidth_after_pressure(bandwidth: float, pressure: float) -> float:
    normalized_bandwidth = max(0.0, float(bandwidth))
    if normalized_bandwidth <= 0.0:
        return 0.0
    return normalized_bandwidth / (1.0 + min(max(0.0, float(pressure)), 4.0))


def resolved_mode_for_plan(plan: PlannerTransferPlan) -> TransferMode:
    has_direct = any(assignment.path.kind == "direct" for assignment in plan.assignments)
    has_relay = any(assignment.path.kind == "relay" for assignment in plan.assignments)
    if has_direct and has_relay:
        return TransferMode.POOL
    if has_relay:
        return TransferMode.RELAY
    return TransferMode.DIRECT


__all__ = [
    "Profile",
    "RelayProfile",
    "bandwidth_after_pressure",
    "direct_fallback_profile",
    "direct_scheduler_weights",
    "profile_cost_context",
    "profile_direct_bandwidth",
    "profile_relay_bandwidth",
    "relay_cost_context",
    "relay_filtered_cost_record",
    "relay_profile_with_load_feedback",
    "resolved_mode_for_plan",
    "scheduler_cost_model_metadata",
]
