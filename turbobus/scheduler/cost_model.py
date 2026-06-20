from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import math

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
    pressure_summaries: Mapping[str, Mapping[str, object]] | None = None,
) -> dict[str, object]:
    normalized_direction = str(direction).lower()
    direct_bw = profile_direct_bandwidth(profile, normalized_direction)
    pressure_summary = _pressure_summary_for_direction(
        runtime_view,
        normalized_direction,
        pressure_summaries,
    )
    direct_pressure = runtime_view.direct_cost_pressure(
        normalized_direction,
        pressure_summary=pressure_summary,
    )
    relay_pressures: dict[int, float] = {}
    path_costs: list[dict[str, object]] = []
    serial_estimated_seconds = 0.0
    parallel_estimated_seconds = 0.0
    direct_bytes = 0
    relay_bytes = 0
    for assignment in plan.assignments:
        path = assignment.path
        bytes_count = sum(chunk.bytes for chunk in assignment.chunks)
        bandwidth = max(path_scheduler_weight(path), 1e-12)
        estimated_seconds = estimated_transfer_seconds(bytes_count, bandwidth)
        serial_estimated_seconds += estimated_seconds
        parallel_estimated_seconds = max(parallel_estimated_seconds, estimated_seconds)
        if path.kind == "relay":
            relay_bytes += bytes_count
            relay_device = int(path.relay_device)
            if relay_device not in relay_pressures:
                relay_pressures[relay_device] = runtime_view.relay_cost_pressure(
                    relay_device,
                    normalized_direction,
                    pressure_summary=pressure_summary,
                )
            relay_pressure = relay_pressures[relay_device]
        else:
            direct_bytes += bytes_count
            relay_pressure = direct_pressure
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
            "runtime_pressure": direct_pressure,
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
        if relay_device not in relay_pressures:
            relay_pressures[relay_device] = runtime_view.relay_cost_pressure(
                relay_device,
                normalized_direction,
                pressure_summary=pressure_summary,
            )
        candidate_paths.append(
            {
                "kind": "relay",
                "target_device": int(relay.target_device),
                "relay_device": int(relay_device),
                "effective_bw_gbps": relay_bw,
                "scheduler_weight_gbps": relay_weight,
                "runtime_pressure": relay_pressures[relay_device],
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
        "estimated_seconds": parallel_estimated_seconds,
        "estimated_parallel_makespan_seconds": parallel_estimated_seconds,
        "estimated_serial_path_seconds": serial_estimated_seconds,
        "path_costs": tuple(path_costs),
        "candidate_paths": tuple(candidate_paths),
        "runtime_pressure_summary": pressure_summary,
        "fabric_abstraction": fabric_abstraction_metadata(profile),
        "profile_binding": profile_topology_binding(profile),
        "profile_import": profile_import_metadata(profile),
        "profile_measurements": profile_measurement_metadata(profile),
        "planner_cost_metadata": dict(getattr(plan, "cost_metadata", {}) or {}),
        "cost_inputs": {
            "profile_measurements": profile_measurement_metadata(profile),
            "runtime_pressure": pressure_summary,
            "workload_kind": runtime_view.workload_kind,
            "priority": int(runtime_view.priority),
        },
    }


def estimated_transfer_seconds(bytes_count: int, bandwidth_gbps: float) -> float:
    bandwidth_bytes_per_second = max(float(bandwidth_gbps), 1e-12) * 1_000_000_000.0
    return float(max(0, int(bytes_count))) / bandwidth_bytes_per_second


def path_scheduler_weight(path) -> float:
    weight = getattr(path, "scheduler_weight_gbps", None)
    if weight is not None and float(weight) > 0.0:
        return max(0.0, float(weight))
    pressure = min(max(0.0, float(getattr(path, "runtime_pressure", 0.0) or 0.0)), 4.0)
    return max(0.0, float(getattr(path, "effective_bw_gbps", 0.0) or 0.0)) / (
        1.0 + pressure
    )


def profile_direct_scheduler_weight(profile: Profile, direction: str) -> float:
    if str(direction).lower() == "d2h":
        explicit = profile.direct_scheduler_weight_d2h_gbps or profile.direct_scheduler_weight_h2d_gbps
        if explicit:
            return max(0.0, float(explicit))
        return max(
            0.0,
            bandwidth_after_pressure(
                profile_direct_bandwidth(profile, direction),
                profile.direct_runtime_pressure_d2h or profile.direct_runtime_pressure_h2d,
            ),
        )
    if profile.direct_scheduler_weight_h2d_gbps:
        return max(0.0, float(profile.direct_scheduler_weight_h2d_gbps))
    return max(
        0.0,
        bandwidth_after_pressure(
            profile_direct_bandwidth(profile, direction),
            profile.direct_runtime_pressure_h2d,
        ),
    )


def profile_relay_scheduler_weight(relay: RelayProfile, direction: str) -> float:
    if str(direction).lower() == "d2h":
        explicit = relay.scheduler_weight_d2h_gbps or relay.scheduler_weight_h2d_gbps
        if explicit:
            return max(0.0, float(explicit))
        return max(
            0.0,
            bandwidth_after_pressure(
                profile_relay_bandwidth(relay, direction),
                relay.runtime_pressure_d2h or relay.runtime_pressure_h2d,
            ),
        )
    if relay.scheduler_weight_h2d_gbps:
        return max(0.0, float(relay.scheduler_weight_h2d_gbps))
    return max(
        0.0,
        bandwidth_after_pressure(
            profile_relay_bandwidth(relay, direction),
            relay.runtime_pressure_h2d,
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
    target_device: int | None = None,
    pressure_summaries: Mapping[str, Mapping[str, object]] | None = None,
) -> tuple[float, float, dict[str, object]]:
    h2d_summary = _pressure_summary_for_direction(
        runtime_view,
        "h2d",
        pressure_summaries,
    )
    d2h_summary = _pressure_summary_for_direction(
        runtime_view,
        "d2h",
        pressure_summaries,
    )
    h2d_pressure = runtime_view.direct_cost_pressure(
        "h2d",
        pressure_summary=h2d_summary,
    )
    d2h_pressure = runtime_view.direct_cost_pressure(
        "d2h",
        pressure_summary=d2h_summary,
    )
    h2d_policy = runtime_view.adaptive_policy_for_path(
        path_kind="direct",
        direction="h2d",
        admission_state=admission_state,
        pressure_summary=h2d_summary,
        path_pressure=h2d_pressure,
    )
    d2h_policy = runtime_view.adaptive_policy_for_path(
        path_kind="direct",
        direction="d2h",
        admission_state=admission_state,
        pressure_summary=d2h_summary,
        path_pressure=d2h_pressure,
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
    pcie_pool_adjustment = _pcie_pool_adjustment(
        runtime_view,
        path_kind="direct",
        target_device=target_device,
        relay_device=None,
        h2d_weight=adjusted_h2d,
        d2h_weight=adjusted_d2h,
    )
    adjusted_h2d = float(pcie_pool_adjustment["scheduler_weight_h2d_gbps"])
    adjusted_d2h = float(pcie_pool_adjustment["scheduler_weight_d2h_gbps"])
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
            "pcie_bandwidth_pool": pcie_pool_adjustment,
            "adaptive_policy_h2d": h2d_policy,
            "adaptive_policy_d2h": d2h_policy,
            "h2d_pressure_summary": h2d_summary,
            "d2h_pressure_summary": d2h_summary,
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
    pressure_summaries: Mapping[str, Mapping[str, object]] | None = None,
) -> tuple[RelayProfile, dict[str, object]]:
    h2d_summary = _pressure_summary_for_direction(
        runtime_view,
        "h2d",
        pressure_summaries,
    )
    d2h_summary = _pressure_summary_for_direction(
        runtime_view,
        "d2h",
        pressure_summaries,
    )
    h2d_pressure = runtime_view.relay_cost_pressure(
        relay_profile.relay_device,
        "h2d",
        pressure_summary=h2d_summary,
    )
    d2h_pressure = runtime_view.relay_cost_pressure(
        relay_profile.relay_device,
        "d2h",
        pressure_summary=d2h_summary,
    )
    directionless_pressure = max(h2d_pressure, d2h_pressure)
    h2d_policy = runtime_view.adaptive_policy_for_path(
        path_kind="relay",
        direction="h2d",
        relay_device=relay_profile.relay_device,
        admission_state=admission_state,
        pressure_summary=h2d_summary,
        path_pressure=h2d_pressure,
    )
    d2h_policy = runtime_view.adaptive_policy_for_path(
        path_kind="relay",
        direction="d2h",
        relay_device=relay_profile.relay_device,
        admission_state=admission_state,
        pressure_summary=d2h_summary,
        path_pressure=d2h_pressure,
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
    pcie_pool_adjustment = _pcie_pool_adjustment(
        runtime_view,
        path_kind="relay",
        target_device=relay_profile.target_device,
        relay_device=relay_profile.relay_device,
        h2d_weight=adjusted_h2d,
        d2h_weight=adjusted_d2h,
    )
    adjusted_h2d = float(pcie_pool_adjustment["scheduler_weight_h2d_gbps"])
    adjusted_d2h = float(pcie_pool_adjustment["scheduler_weight_d2h_gbps"])
    pcie_admission_state = str(
        pcie_pool_adjustment.get("admission_state", admission_state)
    )
    pcie_admission_reason = pcie_pool_adjustment.get("admission_reason")
    path_cost_model = {
        "source": "daemon_scheduler_unified_cost_model",
        "admission_state": pcie_admission_state,
        "admission_reason": pcie_admission_reason or admission_reason,
        "h2d_cost_score": h2d_score,
        "d2h_cost_score": d2h_score,
        "pcie_bandwidth_pool": pcie_pool_adjustment,
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
            "pcie_bandwidth_pool": pcie_pool_adjustment,
            "h2d_pressure_summary": h2d_summary,
            "d2h_pressure_summary": d2h_summary,
            "relay_load": dict(runtime_view.relay_load.get(relay, {})),
            "admission_state": pcie_admission_state,
            "admission_reason": pcie_admission_reason or admission_reason,
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
    pressure_summaries: Mapping[str, Mapping[str, object]] | None = None,
) -> dict[str, object]:
    h2d_summary = _pressure_summary_for_direction(
        runtime_view,
        "h2d",
        pressure_summaries,
    )
    d2h_summary = _pressure_summary_for_direction(
        runtime_view,
        "d2h",
        pressure_summaries,
    )
    h2d_pressure = runtime_view.relay_cost_pressure(
        relay_profile.relay_device,
        "h2d",
        pressure_summary=h2d_summary,
    )
    d2h_pressure = runtime_view.relay_cost_pressure(
        relay_profile.relay_device,
        "d2h",
        pressure_summary=d2h_summary,
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


def _pressure_summary_for_direction(
    runtime_view: RuntimeLoadView,
    direction: str,
    pressure_summaries: Mapping[str, Mapping[str, object]] | None,
) -> Mapping[str, object]:
    normalized_direction = str(direction).lower()
    if isinstance(pressure_summaries, Mapping):
        summary = pressure_summaries.get(normalized_direction)
        if (
            isinstance(summary, Mapping)
            and str(summary.get("direction", "")).lower() == normalized_direction
        ):
            return summary
    return runtime_view.scheduler_pressure_summary(normalized_direction)


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
    priority_discount = runtime_view.priority_cost_discount()
    pressure_component = 1.0 + min(max(0.0, float(pressure)), 4.0)
    return max(
        1e-12,
        pressure_component * admission_penalty * priority_discount / workload_multiplier,
    )


def _pcie_pool_adjustment(
    runtime_view: RuntimeLoadView,
    *,
    path_kind: str,
    target_device: int | None,
    relay_device: int | None,
    h2d_weight: float,
    d2h_weight: float,
) -> dict[str, object]:
    pool = dict(getattr(runtime_view, "pcie_bandwidth_pool", {}) or {})
    paths = pool.get("paths", {})
    if not isinstance(paths, Mapping):
        paths = {}
    target_record = _pcie_pool_path_record(paths, target_device)
    relay_record = _pcie_pool_path_record(paths, relay_device)
    h2d_limit = _path_pcie_limit(
        target_record=target_record,
        relay_record=relay_record,
        direction="h2d",
        path_kind=path_kind,
    )
    d2h_limit = _path_pcie_limit(
        target_record=target_record,
        relay_record=relay_record,
        direction="d2h",
        path_kind=path_kind,
    )
    h2d_confidence = _path_pcie_confidence(
        target_record=target_record,
        relay_record=relay_record,
        path_kind=path_kind,
    )
    d2h_confidence = h2d_confidence
    h2d_adjusted = _adjusted_pcie_weight(
        float(h2d_weight),
        h2d_limit,
        h2d_confidence,
    )
    d2h_adjusted = _adjusted_pcie_weight(
        float(d2h_weight),
        d2h_limit,
        d2h_confidence,
    )
    relay_saturated = (
        str(path_kind).lower() == "relay"
        and relay_record
        and (h2d_limit <= 0.0 or d2h_limit <= 0.0)
    )
    return {
        "source": "daemon_pcie_bandwidth_pool_scheduler_weight",
        "available": bool(pool.get("available", False)),
        "path_kind": str(path_kind),
        "target_device": target_device,
        "relay_device": relay_device,
        "topology_snapshot_id": pool.get("topology_snapshot_id"),
        "topology_version": pool.get("topology_version"),
        "input_scheduler_weight_h2d_gbps": float(h2d_weight),
        "input_scheduler_weight_d2h_gbps": float(d2h_weight),
        "scheduler_weight_h2d_gbps": h2d_adjusted,
        "scheduler_weight_d2h_gbps": d2h_adjusted,
        "available_h2d_gbps": h2d_limit,
        "available_d2h_gbps": d2h_limit,
        "confidence": min(h2d_confidence, d2h_confidence),
        "admission_state": "filtered" if relay_saturated else "available",
        "admission_reason": "pcie_edge_saturated" if relay_saturated else None,
        "target_path": target_record,
        "relay_path": relay_record,
    }


def _path_pcie_limit(
    *,
    target_record: Mapping[str, object],
    relay_record: Mapping[str, object],
    direction: str,
    path_kind: str,
) -> float:
    field_name = (
        "available_d2h_gbps"
        if str(direction).lower() == "d2h"
        else "available_h2d_gbps"
    )
    candidates = []
    if target_record:
        candidates.append(_pcie_available_value(target_record, field_name))
    if str(path_kind).lower() == "relay" and relay_record:
        candidates.append(_pcie_available_value(relay_record, field_name))
    finite_candidates = [
        max(0.0, value)
        for value in candidates
        if math.isfinite(float(value))
    ]
    return min(finite_candidates) if finite_candidates else 0.0


def _pcie_pool_path_record(
    paths: Mapping[object, object],
    device_id: int | None,
) -> dict[str, object]:
    if device_id is None:
        return {}
    for key in (int(device_id), str(int(device_id))):
        record = paths.get(key)
        if isinstance(record, Mapping):
            return dict(record)
    return {}


def _path_pcie_confidence(
    *,
    target_record: Mapping[str, object],
    relay_record: Mapping[str, object],
    path_kind: str,
) -> float:
    values = []
    for record in (target_record, relay_record if str(path_kind).lower() == "relay" else {}):
        if record:
            edges = record.get("edge_ids", ()) or ()
            edge_confidences = []
            for edge in record.get("edges", ()) or ():
                if isinstance(edge, Mapping):
                    edge_confidences.append(float(edge.get("confidence", 0.0) or 0.0))
            if edge_confidences:
                values.append(min(edge_confidences))
            elif edges:
                values.append(0.0)
    return min(values) if values else 0.0


def _pcie_available_value(
    record: Mapping[str, object],
    field_name: str,
) -> float:
    if not record:
        return math.inf
    if not bool(record.get("load_known", False)):
        return float(record.get(field_name, 0.0) or 0.0)
    return float(record.get(field_name, 0.0) or 0.0)


def _adjusted_pcie_weight(
    weight: float,
    limit: float,
    confidence: float,
) -> float:
    adjusted = float(weight)
    if float(limit) <= 0.0:
        adjusted *= 0.05
    else:
        adjusted = min(adjusted, float(limit))
    if float(confidence) < 0.5:
        adjusted *= 0.85
    return max(1e-12, adjusted)


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
