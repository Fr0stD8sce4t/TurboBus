from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Callable, Mapping

from ..planner_engine import PlannerEngine
from ..planner_types import PlannerLease, PlannerStats, PlannerTransferPlan
from .load_feedback import (
    RuntimeLoadView,
    fairness_fallback_for_plan,
    relay_admission_blocked_reason,
    runtime_state_metadata,
    runtime_view,
)
from ..schema import (
    RelayQuota,
    SchedulingDecision,
    SchedulingDecisionState,
    Session,
    TransferMode,
    WorkloadKind,
)


@dataclass(frozen=True)
class _RelayProfile:
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
class _Profile:
    target_device: int
    direct_h2d_bw_gbps: float
    direct_d2h_bw_gbps: float
    relays: tuple[_RelayProfile, ...]
    direct_scheduler_weight_h2d_gbps: float = 0.0
    direct_scheduler_weight_d2h_gbps: float = 0.0
    direct_runtime_pressure_h2d: float = 0.0
    direct_runtime_pressure_d2h: float = 0.0
    cost_metadata: dict[str, object] | None = None


@dataclass(frozen=True)
class _RelayPolicy:
    available_relays: tuple[int, ...]
    deferred_relays: tuple[dict[str, object], ...]
    filtered_relays: tuple[dict[str, object], ...]
    load_adjustments: tuple[dict[str, object], ...]
    defer_relay_admission: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "available_relays": self.available_relays,
            "deferred_relays": self.deferred_relays,
            "filtered_relays": self.filtered_relays,
            "load_adjustments": self.load_adjustments,
            "defer_relay_admission": self.defer_relay_admission,
        }


class DaemonScheduler:
    def __init__(
        self,
        planner: PlannerEngine | None = None,
        lease_id_factory: Callable[[], str] | None = None,
        decision_id_factory: Callable[[], str] | None = None,
        lease_seconds: float = 30.0,
    ) -> None:
        self._planner = planner or PlannerEngine()
        self._lease_id_factory = lease_id_factory or (lambda: str(uuid.uuid4()))
        self._decision_id_factory = decision_id_factory or (lambda: str(uuid.uuid4()))
        self._lease_seconds = max(0.0, float(lease_seconds))

    def plan_transfer(
        self,
        *,
        session: Session,
        profile_entry: Mapping[str, object] | None,
        relay_quotas: Mapping[int, RelayQuota],
        total_bytes: int,
        chunk_bytes: int,
        ranges: tuple[Mapping[str, int], ...] | None = None,
        mode: TransferMode | str = TransferMode.POOL,
        direction: str = "h2d",
        runtime_state: Mapping[str, object] | None = None,
        workload_kind: WorkloadKind | str = WorkloadKind.GENERIC,
        priority: int = 0,
        now: float = 0.0,
        job_id: str | None = None,
        intent_id: str | None = None,
        topology_snapshot_id: str | None = None,
        relay_eligibility: Mapping[str, object] | None = None,
        defer_relay_admission: bool = False,
    ) -> SchedulingDecision:
        total_bytes = int(total_bytes)
        chunk_bytes = int(chunk_bytes)
        normalized_ranges = _normalize_ranges(ranges)
        direction = str(direction).lower()
        if total_bytes < 0:
            raise ValueError("total_bytes must be non-negative")
        if chunk_bytes <= 0:
            raise ValueError("chunk_bytes must be positive")
        if normalized_ranges is not None:
            range_bytes = sum(item["bytes"] for item in normalized_ranges)
            if range_bytes != total_bytes:
                raise ValueError("range bytes must match total_bytes")
        if direction not in {"h2d", "d2h"}:
            raise ValueError("direction must be h2d or d2h")
        if not session.active:
            raise ValueError("session is closed")

        requested_mode = _parse_transfer_mode(mode)
        planning_mode = TransferMode.POOL if requested_mode is TransferMode.AUTO else requested_mode
        runtime_load = runtime_view(
            runtime_state=runtime_state,
            job_id=job_id,
            total_bytes=total_bytes,
            workload_kind=workload_kind,
            priority=priority,
        )
        profile, fallback_reason, relay_policy = self._profile_for_planning(
            profile_entry=profile_entry,
            session=session,
            relay_quotas=relay_quotas,
            direction=direction,
            runtime_view=runtime_load,
            defer_relay_admission=defer_relay_admission,
        )
        if (
            fallback_reason is None
            and planning_mode is not TransferMode.DIRECT
            and session.worker_relay_capable
            and session.relay_gpus
            and not profile.relays
        ):
            fallback_reason = "no daemon-approved relay path"

        plan = self._plan_or_direct(
            total_bytes=total_bytes,
            chunk_bytes=chunk_bytes,
            ranges=normalized_ranges,
            profile=profile,
            mode=planning_mode,
            direction=direction,
        )
        leases, lease_error = self._leases_for_plan(
            plan=plan,
            session=session,
            relay_quotas=relay_quotas,
            direction=direction,
            now=now,
            job_id=job_id,
            defer_relay_admission=defer_relay_admission,
        )
        fairness_fallback = fairness_fallback_for_plan(
            plan=plan,
            runtime_view=runtime_load,
        )
        if fairness_fallback is not None:
            lease_error = fairness_fallback
        if lease_error is not None:
            fallback_reason = lease_error
            plan = self._direct_plan(
                total_bytes=total_bytes,
                chunk_bytes=chunk_bytes,
                ranges=normalized_ranges,
                profile=profile,
                direction=direction,
            )
            leases = ()

        stats = _stats_for_plan(
            plan,
            requested_mode=requested_mode,
            fallback_reason=fallback_reason,
        )
        return SchedulingDecision(
            decision_id=self._decision_id_factory(),
            intent_id=_contract_id(
                intent_id,
                prefix="intent",
                fallback=session.session_id,
            ),
            topology_snapshot_id=_contract_id(
                topology_snapshot_id,
                prefix="topology",
                fallback=session.session_id,
            ),
            job_id=str(job_id or session.session_id),
            session_id=session.session_id,
            state=(
                SchedulingDecisionState.FALLBACK
                if fallback_reason is not None
                else SchedulingDecisionState.PLANNED
            ),
            plan=plan.as_dict(),
            path_summary=_path_summary_for_plan(plan),
            fallback_reason=fallback_reason,
            issued_at=float(now),
            metadata={
                "leases": [lease.as_dict() for lease in leases],
                "stats": stats.as_dict(),
                "cost_model": _scheduler_cost_model_metadata(
                    plan=plan,
                    profile=profile,
                    runtime_view=runtime_load,
                    direction=direction,
                    total_bytes=total_bytes,
                ),
                "runtime_state": runtime_state_metadata(runtime_state),
                "topology": _topology_metadata(
                    topology_snapshot_id=topology_snapshot_id,
                    relay_eligibility=relay_eligibility,
                ),
                "policy": runtime_load.policy_metadata(),
                "relay_policy": relay_policy.as_dict(),
            },
        )

    def _profile_for_planning(
        self,
        *,
        profile_entry: Mapping[str, object] | None,
        session: Session,
        relay_quotas: Mapping[int, RelayQuota],
        direction: str,
        runtime_view: RuntimeLoadView,
        defer_relay_admission: bool,
    ) -> tuple[_Profile, str | None, _RelayPolicy]:
        empty_policy = _RelayPolicy(
            available_relays=(),
            deferred_relays=(),
            filtered_relays=(),
            load_adjustments=(),
            defer_relay_admission=bool(defer_relay_admission),
        )
        payload = _profile_payload(profile_entry)
        if payload is None:
            return (
                _direct_fallback_profile(session.target_gpu),
                "daemon profile miss",
                empty_policy,
            )
        if not bool(session.worker_relay_capable):
            return (
                _Profile(
                    target_device=int(payload.get("target_device", session.target_gpu)),
                    direct_h2d_bw_gbps=float(payload.get("direct_h2d_bw_gbps", 0.0) or 0.0),
                    direct_d2h_bw_gbps=float(
                        payload.get("direct_d2h_bw_gbps", payload.get("direct_h2d_bw_gbps", 0.0))
                        or 0.0
                    ),
                    relays=(),
                    cost_metadata=_profile_cost_context(profile_entry),
                ),
                "session is not worker relay capable",
                _RelayPolicy(
                    available_relays=(),
                    deferred_relays=(),
                    filtered_relays=tuple(
                        {
                            "relay_device": int(gpu),
                            "reason": "session is not worker relay capable",
                        }
                        for gpu in session.relay_gpus
                    ),
                    load_adjustments=(),
                    defer_relay_admission=bool(defer_relay_admission),
                ),
            )

        available_relays = []
        deferred_relays = []
        deferred_relay_profiles = []
        filtered_relays: list[dict[str, object]] = []
        load_adjustments: list[dict[str, object]] = []
        allowed_relays = set(int(gpu) for gpu in session.relay_gpus)
        for relay in payload.get("relays", []) or []:
            if not isinstance(relay, Mapping):
                continue
            relay_device = int(relay["relay_device"])
            if relay_device not in allowed_relays:
                filtered_relays.append(
                    {
                        "relay_device": relay_device,
                        "reason": "relay is not assigned to session",
                    }
                )
                continue
            if not bool(relay.get("p2p_enabled", False)):
                filtered_relays.append(
                    {"relay_device": relay_device, "reason": "relay p2p is disabled"}
                )
                continue
            if float(relay.get("p2p_bw_gbps", 0.0) or 0.0) <= 0.0:
                filtered_relays.append(
                    {
                        "relay_device": relay_device,
                        "reason": "relay p2p bandwidth is unavailable",
                    }
                )
                continue
            relay_profile = _RelayProfile(
                relay_device=relay_device,
                target_device=int(relay.get("target_device", session.target_gpu)),
                h2d_bw_gbps=float(relay.get("h2d_bw_gbps", 0.0) or 0.0),
                d2h_bw_gbps=float(relay.get("d2h_bw_gbps", 0.0) or 0.0),
                p2p_bw_gbps=float(relay.get("p2p_bw_gbps", 0.0) or 0.0),
                effective_bw_gbps=float(relay.get("effective_bw_gbps", 0.0) or 0.0),
                effective_d2h_bw_gbps=float(
                    relay.get("effective_d2h_bw_gbps", 0.0) or 0.0
                ),
                p2p_enabled=bool(relay.get("p2p_enabled", False)),
                cost_metadata=_relay_cost_context(profile_entry, relay_device),
            )
            relay_profile, relay_adjustment = _relay_profile_with_load_feedback(
                relay_profile,
                runtime_view=runtime_view,
            )
            load_adjustments.append(relay_adjustment)
            unavailable_reason = _relay_unavailable_reason(
                session=session,
                quota=relay_quotas.get(relay_device),
                relay_device=relay_device,
                runtime_view=runtime_view,
            )
            if unavailable_reason is None:
                available_relays.append(relay_profile)
            elif defer_relay_admission:
                deferred_relay_profiles.append(relay_profile)
                deferred_relays.append(
                    {
                        "relay_device": relay_device,
                        "reason": unavailable_reason,
                    }
                )
            else:
                filtered_relays.append(
                    {
                        "relay_device": relay_device,
                        "reason": unavailable_reason,
                    }
                )

        direct_h2d = float(payload.get("direct_h2d_bw_gbps", 0.0) or 0.0)
        direct_d2h = float(payload.get("direct_d2h_bw_gbps", 0.0) or direct_h2d)
        if direction == "h2d" and direct_h2d <= 0.0:
            return (
                _direct_fallback_profile(session.target_gpu),
                "daemon direct profile invalid",
                empty_policy,
            )
        if direction == "d2h" and direct_d2h <= 0.0:
            direct_d2h = direct_h2d
        (
            direct_weight_h2d,
            direct_weight_d2h,
            direct_adjustment,
        ) = _direct_scheduler_weights(
            direct_h2d,
            direct_d2h,
            runtime_view=runtime_view,
        )

        selected_relays = tuple(available_relays)
        if not selected_relays and defer_relay_admission:
            selected_relays = tuple(deferred_relay_profiles)
        load_adjustments.insert(0, direct_adjustment)
        relay_policy = _RelayPolicy(
            available_relays=tuple(relay.relay_device for relay in available_relays),
            deferred_relays=tuple(deferred_relays),
            filtered_relays=tuple(filtered_relays),
            load_adjustments=tuple(load_adjustments),
            defer_relay_admission=bool(defer_relay_admission),
        )
        return (
            _Profile(
                target_device=int(payload.get("target_device", session.target_gpu)),
                direct_h2d_bw_gbps=direct_h2d,
                direct_d2h_bw_gbps=direct_d2h,
                relays=selected_relays,
                direct_scheduler_weight_h2d_gbps=direct_weight_h2d,
                direct_scheduler_weight_d2h_gbps=direct_weight_d2h,
                direct_runtime_pressure_h2d=runtime_view.direct_cost_pressure("h2d"),
                direct_runtime_pressure_d2h=runtime_view.direct_cost_pressure("d2h"),
                cost_metadata=_profile_cost_context(profile_entry),
            ),
            None,
            relay_policy,
        )

    def _plan_or_direct(
        self,
        *,
        total_bytes: int,
        chunk_bytes: int,
        ranges: tuple[Mapping[str, int], ...] | None,
        profile: _Profile,
        mode: TransferMode,
        direction: str,
    ) -> PlannerTransferPlan:
        try:
            if ranges is not None:
                return self._planner.plan_ranges(
                    ranges,
                    chunk_bytes,
                    profile,
                    mode,
                    direction=direction,
                )
            return self._planner.plan(
                total_bytes=total_bytes,
                chunk_bytes=chunk_bytes,
                profile=profile,
                mode=mode,
                direction=direction,
            )
        except RuntimeError:
            return self._direct_plan(
                total_bytes=total_bytes,
                chunk_bytes=chunk_bytes,
                ranges=ranges,
                profile=profile,
                direction=direction,
            )

    def _direct_plan(
        self,
        *,
        total_bytes: int,
        chunk_bytes: int,
        ranges: tuple[Mapping[str, int], ...] | None = None,
        profile: _Profile,
        direction: str,
    ) -> PlannerTransferPlan:
        direct_profile = _Profile(
            target_device=profile.target_device,
            direct_h2d_bw_gbps=profile.direct_h2d_bw_gbps or 1.0,
            direct_d2h_bw_gbps=(
                profile.direct_d2h_bw_gbps or profile.direct_h2d_bw_gbps or 1.0
            ),
            relays=(),
        )
        if ranges is not None:
            return self._planner.plan_ranges(
                ranges,
                chunk_bytes,
                direct_profile,
                mode=TransferMode.DIRECT,
                direction=direction,
            )
        return self._planner.plan(
            total_bytes=total_bytes,
            chunk_bytes=chunk_bytes,
            profile=direct_profile,
            mode=TransferMode.DIRECT,
            direction=direction,
        )

    def _leases_for_plan(
        self,
        *,
        plan: PlannerTransferPlan,
        session: Session,
        relay_quotas: Mapping[int, RelayQuota],
        direction: str,
        now: float,
        job_id: str | None,
        defer_relay_admission: bool,
    ) -> tuple[tuple[PlannerLease, ...], str | None]:
        lease_specs: list[tuple[int, int, int]] = []
        for assignment in plan.assignments:
            if assignment.path.kind != "relay":
                continue
            relay_device = int(assignment.path.relay_device)
            chunks = len(assignment.chunks)
            bytes_limit = sum(chunk.bytes for chunk in assignment.chunks)
            if chunks <= 0:
                continue
            quota = relay_quotas.get(relay_device)
            lease_chunks = _relay_reservation_chunks(
                chunks,
                session=session,
                quota=quota,
            )
            if lease_chunks <= 0:
                return (), "relay chunk quota is unavailable"
            lease_specs.append((relay_device, lease_chunks, bytes_limit))

        if not lease_specs:
            return (), None

        total_chunks = sum(chunks for _, chunks, _ in lease_specs)
        if (
            not defer_relay_admission
            and session.active_chunks + total_chunks > session.max_inflight_chunks
        ):
            return (), "session chunk quota is unavailable"

        leases: list[PlannerLease] = []
        for relay_device, chunks, bytes_limit in lease_specs:
            if relay_device not in session.relay_gpus:
                return (), "relay GPU is not assigned to this session"
            quota = relay_quotas.get(relay_device)
            if quota is None:
                return (), "relay chunk quota is unavailable"
            if not defer_relay_admission and not quota.can_reserve(chunks):
                return (), "relay chunk quota is unavailable"
            leases.append(
                PlannerLease(
                    lease_id=self._lease_id_factory(),
                    session_id=session.session_id,
                    relay_device=relay_device,
                    chunk_limit=chunks,
                    bytes_limit=bytes_limit,
                    direction=direction,
                    granted_at=float(now),
                    expires_at=float(now) + self._lease_seconds,
                    job_id=job_id,
                )
            )
        return tuple(leases), None


def _stats_for_plan(
    plan: PlannerTransferPlan,
    *,
    requested_mode: TransferMode,
    fallback_reason: str | None,
) -> PlannerStats:
    direct_bytes = 0
    relay_bytes = 0
    direct_chunks = 0
    relay_chunks = 0
    relay_path_count = 0
    for assignment in plan.assignments:
        assignment_bytes = sum(chunk.bytes for chunk in assignment.chunks)
        if assignment.path.kind == "relay":
            relay_bytes += assignment_bytes
            relay_chunks += len(assignment.chunks)
            relay_path_count += 1
        else:
            direct_bytes += assignment_bytes
            direct_chunks += len(assignment.chunks)
    return PlannerStats(
        bytes=int(plan.total_bytes),
        direct_bytes=direct_bytes,
        relay_bytes=relay_bytes,
        direct_chunks=direct_chunks,
        relay_chunks=relay_chunks,
        path_count=len(plan.assignments),
        relay_path_count=relay_path_count,
        fallback_reason=fallback_reason,
        requested_mode=requested_mode,
        resolved_mode=_resolved_mode_for_plan(plan),
    )


def _scheduler_cost_model_metadata(
    *,
    plan: PlannerTransferPlan,
    profile: _Profile,
    runtime_view: RuntimeLoadView,
    direction: str,
    total_bytes: int,
) -> dict[str, object]:
    normalized_direction = str(direction).lower()
    direct_bw = _profile_direct_bandwidth(profile, normalized_direction)
    path_costs: list[dict[str, object]] = []
    total_estimated_seconds = 0.0
    direct_bytes = 0
    relay_bytes = 0
    for assignment in plan.assignments:
        path = assignment.path
        bytes_count = sum(chunk.bytes for chunk in assignment.chunks)
        bandwidth = max(_path_scheduler_weight(path), 1e-12)
        estimated_seconds = _estimated_transfer_seconds(bytes_count, bandwidth)
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
                "scheduler_weight_gbps": _path_scheduler_weight(path),
                "allocation_ratio": (
                    0.0 if total_bytes <= 0 else float(bytes_count) / float(total_bytes)
                ),
                "estimated_seconds": estimated_seconds,
                "runtime_pressure": relay_pressure,
                "pressure_summary": dict(pressure_summary),
                "cost_metadata": dict(path.cost_metadata),
            }
        )
    relay_profiles = {
        int(relay.relay_device): relay
        for relay in profile.relays
    }
    direct_weight = _profile_direct_scheduler_weight(profile, normalized_direction)
    candidate_paths: list[dict[str, object]] = [
        {
            "kind": "direct",
            "target_device": int(profile.target_device),
            "relay_device": None,
            "effective_bw_gbps": direct_bw,
            "scheduler_weight_gbps": direct_weight,
            "runtime_pressure": runtime_view.direct_cost_pressure(normalized_direction),
            "estimated_full_transfer_seconds": _estimated_transfer_seconds(
                total_bytes,
                max(direct_weight, 1e-12),
            ),
            "topology_binding": _profile_topology_binding(profile),
        }
    ]
    for relay_device, relay in sorted(relay_profiles.items()):
        relay_bw = _profile_relay_bandwidth(relay, normalized_direction)
        relay_weight = _profile_relay_scheduler_weight(relay, normalized_direction)
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
                "estimated_full_transfer_seconds": _estimated_transfer_seconds(
                    total_bytes,
                    max(relay_weight, 1e-12),
                ),
                "relay_load": dict(runtime_view.relay_load.get(relay_device, {})),
                "topology_binding": _relay_topology_binding(relay),
            }
        )
    resolved_mode = _resolved_mode_for_plan(plan).value
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
        "profile_binding": _profile_topology_binding(profile),
    }


def _estimated_transfer_seconds(bytes_count: int, bandwidth_gbps: float) -> float:
    bandwidth_bytes_per_second = max(float(bandwidth_gbps), 1e-12) * 1_000_000_000.0
    return float(max(0, int(bytes_count))) / bandwidth_bytes_per_second


def _path_scheduler_weight(path) -> float:
    weight = getattr(path, "scheduler_weight_gbps", None)
    if weight is not None:
        return max(0.0, float(weight))
    return max(0.0, float(getattr(path, "effective_bw_gbps", 0.0) or 0.0))


def _profile_direct_scheduler_weight(profile: _Profile, direction: str) -> float:
    if str(direction).lower() == "d2h":
        return max(
            0.0,
            float(
                profile.direct_scheduler_weight_d2h_gbps
                or profile.direct_scheduler_weight_h2d_gbps
                or _profile_direct_bandwidth(profile, direction)
            ),
        )
    return max(
        0.0,
        float(
            profile.direct_scheduler_weight_h2d_gbps
            or _profile_direct_bandwidth(profile, direction)
        ),
    )


def _profile_relay_scheduler_weight(relay: _RelayProfile, direction: str) -> float:
    if str(direction).lower() == "d2h":
        return max(
            0.0,
            float(
                relay.scheduler_weight_d2h_gbps
                or relay.scheduler_weight_h2d_gbps
                or _profile_relay_bandwidth(relay, direction)
            ),
        )
    return max(
        0.0,
        float(
            relay.scheduler_weight_h2d_gbps
            or _profile_relay_bandwidth(relay, direction)
        ),
    )


def _profile_topology_binding(profile: _Profile) -> dict[str, object]:
    metadata = dict(profile.cost_metadata or {})
    binding = metadata.get("topology_binding")
    return dict(binding) if isinstance(binding, Mapping) else {}


def _relay_topology_binding(relay: _RelayProfile) -> dict[str, object]:
    metadata = dict(relay.cost_metadata or {})
    topology = metadata.get("topology")
    return dict(topology) if isinstance(topology, Mapping) else {}


def _profile_direct_bandwidth(profile: _Profile, direction: str) -> float:
    if str(direction).lower() == "d2h":
        return max(
            0.0,
            float(profile.direct_d2h_bw_gbps or profile.direct_h2d_bw_gbps or 0.0),
        )
    return max(0.0, float(profile.direct_h2d_bw_gbps or 0.0))


def _profile_relay_bandwidth(relay: _RelayProfile, direction: str) -> float:
    if str(direction).lower() == "d2h":
        return max(
            0.0,
            float(relay.effective_d2h_bw_gbps or relay.effective_bw_gbps or 0.0),
        )
    return max(0.0, float(relay.effective_bw_gbps or 0.0))


def scheduling_decision_leases(
    decision: SchedulingDecision,
) -> tuple[PlannerLease, ...]:
    if not isinstance(decision, SchedulingDecision):
        raise TypeError("decision must be a SchedulingDecision")
    leases = decision.metadata.get("leases", ())
    if not isinstance(leases, tuple | list):
        raise ValueError("scheduling decision metadata leases must be a sequence")
    return tuple(_planner_lease_from_payload(item) for item in leases)


def scheduling_decision_stats(decision: SchedulingDecision) -> PlannerStats:
    if not isinstance(decision, SchedulingDecision):
        raise TypeError("decision must be a SchedulingDecision")
    payload = decision.metadata.get("stats", {})
    if not isinstance(payload, Mapping):
        raise ValueError("scheduling decision metadata stats must be a mapping")
    return PlannerStats(
        bytes=int(payload.get("bytes", 0)),
        direct_bytes=int(payload.get("direct_bytes", 0)),
        relay_bytes=int(payload.get("relay_bytes", 0)),
        direct_chunks=int(payload.get("direct_chunks", 0)),
        relay_chunks=int(payload.get("relay_chunks", 0)),
        path_count=int(payload.get("path_count", 0)),
        relay_path_count=int(payload.get("relay_path_count", 0)),
        fallback_reason=payload.get("fallback_reason"),
        requested_mode=payload.get("requested_mode", TransferMode.POOL),
        resolved_mode=payload.get("resolved_mode", TransferMode.POOL),
    )


def _planner_lease_from_payload(payload: object) -> PlannerLease:
    if not isinstance(payload, Mapping):
        raise ValueError("scheduling decision lease must be a mapping")
    return PlannerLease(
        lease_id=str(payload["lease_id"]),
        session_id=str(payload["session_id"]),
        relay_device=int(payload["relay_device"]),
        chunk_limit=int(payload["chunk_limit"]),
        bytes_limit=int(payload.get("bytes_limit", 0)),
        direction=str(payload.get("direction", "unknown")),
        granted_at=float(payload.get("granted_at", 0.0)),
        expires_at=float(payload.get("expires_at", 0.0)),
        active=bool(payload.get("active", True)),
        job_id=payload.get("job_id"),
        reason=payload.get("reason"),
    )


def _path_summary_for_plan(
    plan: PlannerTransferPlan,
) -> tuple[dict[str, object], ...]:
    summary: list[dict[str, object]] = []
    for assignment in plan.assignments:
        path = assignment.path
        bytes_count = sum(chunk.bytes for chunk in assignment.chunks)
        summary.append(
            {
                "kind": path.kind,
                "direction": path.direction,
                "target_device": path.target_device,
                "relay_device": path.relay_device,
                "bytes": bytes_count,
                "chunk_count": len(assignment.chunks),
            }
        )
    return tuple(summary)


def _topology_metadata(
    *,
    topology_snapshot_id: str | None,
    relay_eligibility: Mapping[str, object] | None,
) -> dict[str, object]:
    if not isinstance(relay_eligibility, Mapping):
        return {
            "topology_snapshot_id": topology_snapshot_id,
            "requested_relays": (),
            "eligible_relays": (),
            "filtered_relays": (),
        }
    return {
        "topology_snapshot_id": (
            relay_eligibility.get("topology_snapshot_id") or topology_snapshot_id
        ),
        "topology_version": relay_eligibility.get("topology_version"),
        "inventory_source": relay_eligibility.get("inventory_source"),
        "inventory_discovered_at": relay_eligibility.get("inventory_discovered_at"),
        "requested_relays": tuple(relay_eligibility.get("requested_relays", ()) or ()),
        "eligible_relays": tuple(
            dict(item)
            for item in relay_eligibility.get("eligible_relays", ()) or ()
            if isinstance(item, Mapping)
        ),
        "filtered_relays": tuple(
            dict(item)
            for item in relay_eligibility.get("filtered_relays", ()) or ()
            if isinstance(item, Mapping)
        ),
    }


def _contract_id(value: str | None, *, prefix: str, fallback: str) -> str:
    if value is not None and str(value).strip():
        return str(value)
    return f"{prefix}-{fallback}"


def _resolved_mode_for_plan(plan: PlannerTransferPlan) -> TransferMode:
    has_direct = any(assignment.path.kind == "direct" for assignment in plan.assignments)
    has_relay = any(assignment.path.kind == "relay" for assignment in plan.assignments)
    if has_direct and has_relay:
        return TransferMode.POOL
    if has_relay:
        return TransferMode.RELAY
    return TransferMode.DIRECT


def _profile_payload(profile_entry: Mapping[str, object] | None) -> Mapping[str, object] | None:
    if not profile_entry:
        return None
    profile = profile_entry.get("profile")
    if isinstance(profile, Mapping):
        return profile
    return profile_entry


def _profile_cost_context(
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


def _relay_cost_context(
    profile_entry: Mapping[str, object] | None,
    relay_device: int,
) -> dict[str, object]:
    context = _profile_cost_context(profile_entry)
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


def _direct_fallback_profile(target_gpu: int) -> _Profile:
    return _Profile(
        target_device=int(target_gpu),
        direct_h2d_bw_gbps=1.0,
        direct_d2h_bw_gbps=1.0,
        relays=(),
        direct_scheduler_weight_h2d_gbps=1.0,
        direct_scheduler_weight_d2h_gbps=1.0,
        cost_metadata={"profile_source": "direct_fallback_profile"},
    )


def _relay_unavailable_reason(
    *,
    session: Session,
    quota: RelayQuota | None,
    relay_device: int,
    runtime_view: RuntimeLoadView,
) -> str | None:
    if relay_device not in session.relay_gpus:
        return "relay GPU is not assigned to this session"
    if quota is None:
        return "relay chunk quota is unavailable"
    if session.active_chunks >= session.max_inflight_chunks:
        return "session chunk quota is unavailable"
    if quota.active_chunks >= quota.max_inflight_chunks:
        return "relay chunk quota is unavailable"
    runtime_blocked = relay_admission_blocked_reason(
        runtime_view,
        int(relay_device),
    )
    if runtime_blocked is not None:
        return runtime_blocked
    return None


def _relay_reservation_chunks(
    requested_chunks: int,
    *,
    session: Session,
    quota: RelayQuota | None,
) -> int:
    if quota is None:
        return 0
    requested = max(0, int(requested_chunks))
    if requested <= 0:
        return 0
    session_available = max(
        0,
        int(session.max_inflight_chunks) - int(session.active_chunks),
    )
    relay_available = max(
        0,
        int(quota.max_inflight_chunks) - int(quota.active_chunks),
    )
    return min(requested, session_available, relay_available)


def _direct_scheduler_weights(
    direct_h2d: float,
    direct_d2h: float,
    *,
    runtime_view: RuntimeLoadView,
) -> tuple[float, float, dict[str, object]]:
    h2d_pressure = runtime_view.direct_cost_pressure("h2d")
    d2h_pressure = runtime_view.direct_cost_pressure("d2h")
    adjusted_h2d = _bandwidth_after_pressure(direct_h2d, h2d_pressure)
    adjusted_d2h = _bandwidth_after_pressure(direct_d2h, d2h_pressure)
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
            "h2d_pressure_summary": runtime_view.scheduler_pressure_summary("h2d"),
            "d2h_pressure_summary": runtime_view.scheduler_pressure_summary("d2h"),
            "source": "runtime_cost_model",
        },
    )


def _relay_profile_with_load_feedback(
    relay_profile: _RelayProfile,
    *,
    runtime_view: RuntimeLoadView,
) -> tuple[_RelayProfile, dict[str, object]]:
    h2d_pressure = runtime_view.relay_cost_pressure(
        relay_profile.relay_device,
        "h2d",
    )
    d2h_pressure = runtime_view.relay_cost_pressure(
        relay_profile.relay_device,
        "d2h",
    )
    directionless_pressure = max(h2d_pressure, d2h_pressure)
    adjusted = _RelayProfile(
        relay_device=relay_profile.relay_device,
        target_device=relay_profile.target_device,
        h2d_bw_gbps=relay_profile.h2d_bw_gbps,
        d2h_bw_gbps=relay_profile.d2h_bw_gbps,
        p2p_bw_gbps=relay_profile.p2p_bw_gbps,
        effective_bw_gbps=relay_profile.effective_bw_gbps,
        effective_d2h_bw_gbps=relay_profile.effective_d2h_bw_gbps,
        p2p_enabled=relay_profile.p2p_enabled,
        scheduler_weight_h2d_gbps=_bandwidth_after_pressure(
            relay_profile.effective_bw_gbps,
            h2d_pressure,
        ),
        scheduler_weight_d2h_gbps=_bandwidth_after_pressure(
            relay_profile.effective_d2h_bw_gbps,
            d2h_pressure,
        ),
        runtime_pressure_h2d=h2d_pressure,
        runtime_pressure_d2h=d2h_pressure,
        cost_metadata=dict(relay_profile.cost_metadata or {}),
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
            "h2d_pressure_summary": runtime_view.scheduler_pressure_summary("h2d"),
            "d2h_pressure_summary": runtime_view.scheduler_pressure_summary("d2h"),
            "relay_load": dict(runtime_view.relay_load.get(relay, {})),
            "source": "runtime_cost_model",
        },
    )


def _bandwidth_after_pressure(bandwidth: float, pressure: float) -> float:
    normalized_bandwidth = max(0.0, float(bandwidth))
    if normalized_bandwidth <= 0.0:
        return 0.0
    return normalized_bandwidth / (1.0 + min(max(0.0, float(pressure)), 4.0))


def _parse_transfer_mode(mode: TransferMode | str) -> TransferMode:
    if isinstance(mode, TransferMode):
        return mode
    value = str(mode)
    try:
        return TransferMode(value)
    except ValueError:
        return TransferMode[value.upper()]


def _normalize_ranges(
    ranges: tuple[Mapping[str, int], ...] | None,
) -> tuple[Mapping[str, int], ...] | None:
    if ranges is None:
        return None
    normalized = []
    for item in ranges:
        if not isinstance(item, Mapping):
            raise ValueError("ranges must contain mappings")
        src_offset = int(item["src_offset"])
        dst_offset = int(item["dst_offset"])
        bytes_count = int(item["bytes"])
        if src_offset < 0 or dst_offset < 0:
            raise ValueError("range offsets must be non-negative")
        if bytes_count <= 0:
            raise ValueError("range bytes must be positive")
        normalized.append(
            {
                "src_offset": src_offset,
                "dst_offset": dst_offset,
                "bytes": bytes_count,
            }
        )
    return tuple(normalized)


__all__ = [
    "DaemonScheduler",
    "SchedulingDecision",
    "scheduling_decision_leases",
    "scheduling_decision_stats",
]
