from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Callable, Mapping

from ..planner_engine import PlannerEngine, PlannerEngineOptions
from ..planner_types import PlannerLease, PlannerStats, PlannerTransferPlan
from .cost_model import (
    Profile as _Profile,
    RelayProfile as _RelayProfile,
    direct_fallback_profile as _direct_fallback_profile,
    direct_scheduler_weights as _direct_scheduler_weights,
    profile_cost_context as _profile_cost_context,
    relay_cost_context as _relay_cost_context,
    relay_filtered_cost_record as _relay_filtered_cost_record,
    relay_profile_with_load_feedback as _relay_profile_with_load_feedback,
    resolved_mode_for_plan as _resolved_mode_for_plan,
    scheduler_cost_model_metadata as _scheduler_cost_model_metadata,
)
from .load_feedback import (
    RuntimeLoadView,
    fairness_fallback_for_plan,
    relay_admission_blocked_reason,
    runtime_view,
)
from .path_allocator import (
    block_plan_for_transfer_plan as _block_plan_for_transfer_plan,
    block_plan_metadata as _block_plan_metadata,
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


@dataclass(frozen=True)
class _RelayCandidateSet:
    available_profiles: tuple[_RelayProfile, ...]
    deferred_profiles: tuple[_RelayProfile, ...]
    deferred_relays: tuple[dict[str, object], ...]
    filtered_relays: tuple[dict[str, object], ...]
    load_adjustments: tuple[dict[str, object], ...]


class DaemonScheduler:
    def __init__(
        self,
        planner: PlannerEngine | None = None,
        planner_options: PlannerEngineOptions | None = None,
        lease_id_factory: Callable[[], str] | None = None,
        decision_id_factory: Callable[[], str] | None = None,
        lease_seconds: float = 30.0,
    ) -> None:
        self._planner = planner or PlannerEngine(planner_options)
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
        total_bytes, chunk_bytes, normalized_ranges, direction = (
            _validated_plan_transfer_inputs(
                session=session,
                total_bytes=total_bytes,
                chunk_bytes=chunk_bytes,
                ranges=ranges,
                direction=direction,
            )
        )
        requested_mode = _parse_transfer_mode(mode)
        planning_mode = TransferMode.POOL if requested_mode is TransferMode.AUTO else requested_mode
        profile, runtime_load, relay_policy, fallback_reason, pressure_summaries = (
            self._planning_profile_context(
                session=session,
                profile_entry=profile_entry,
                relay_quotas=relay_quotas,
                runtime_state=runtime_state,
                job_id=job_id,
                total_bytes=total_bytes,
                workload_kind=workload_kind,
                priority=priority,
                direction=direction,
                planning_mode=planning_mode,
                defer_relay_admission=defer_relay_admission,
            )
        )
        plan, leases, fallback_reason = self._plan_with_admission(
            total_bytes=total_bytes,
            chunk_bytes=chunk_bytes,
            ranges=normalized_ranges,
            profile=profile,
            planning_mode=planning_mode,
            direction=direction,
            session=session,
            relay_quotas=relay_quotas,
            runtime_load=runtime_load,
            now=now,
            job_id=job_id,
            defer_relay_admission=defer_relay_admission,
            fallback_reason=fallback_reason,
        )
        plan = self._plan_with_block_metadata(
            plan=plan,
            direction=direction,
            decision_seed=(
                intent_id
                or job_id
                or session.session_id
                or str(total_bytes)
            ),
            runtime_load=runtime_load,
            fallback_reason=fallback_reason,
        )
        stats = _stats_for_plan(
            plan,
            requested_mode=requested_mode,
            fallback_reason=fallback_reason,
        )
        return self._scheduling_decision(
            session=session,
            plan=plan,
            leases=leases,
            stats=stats,
            profile=profile,
            runtime_load=runtime_load,
            relay_policy=relay_policy,
            fallback_reason=fallback_reason,
            now=now,
            job_id=job_id,
            intent_id=intent_id,
            topology_snapshot_id=topology_snapshot_id,
            relay_eligibility=relay_eligibility,
            runtime_state=runtime_state,
            direction=direction,
            total_bytes=total_bytes,
            pressure_summaries=pressure_summaries,
        )

    def _planning_profile_context(
        self,
        *,
        session: Session,
        profile_entry: Mapping[str, object] | None,
        relay_quotas: Mapping[int, RelayQuota],
        runtime_state: Mapping[str, object] | None,
        job_id: str | None,
        total_bytes: int,
        workload_kind: WorkloadKind | str,
        priority: int,
        direction: str,
        planning_mode: TransferMode,
        defer_relay_admission: bool,
    ) -> tuple[
        _Profile,
        RuntimeLoadView,
        _RelayPolicy,
        str | None,
        dict[str, Mapping[str, object]],
    ]:
        runtime_load = runtime_view(
            runtime_state=runtime_state,
            job_id=job_id,
            total_bytes=total_bytes,
            workload_kind=workload_kind,
            priority=priority,
        )
        pressure_summaries = _runtime_pressure_summaries(runtime_load)
        profile, fallback_reason, relay_policy = self._profile_for_planning(
            profile_entry=profile_entry,
            session=session,
            relay_quotas=relay_quotas,
            direction=direction,
            runtime_view=runtime_load,
            defer_relay_admission=defer_relay_admission,
            pressure_summaries=pressure_summaries,
        )
        fallback_reason = _relay_profile_fallback_reason(
            fallback_reason=fallback_reason,
            planning_mode=planning_mode,
            session=session,
            profile=profile,
        )
        return profile, runtime_load, relay_policy, fallback_reason, pressure_summaries

    def _plan_with_admission(
        self,
        *,
        total_bytes: int,
        chunk_bytes: int,
        ranges: tuple[Mapping[str, int], ...] | None,
        profile: _Profile,
        planning_mode: TransferMode,
        direction: str,
        session: Session,
        relay_quotas: Mapping[int, RelayQuota],
        runtime_load: RuntimeLoadView,
        now: float,
        job_id: str | None,
        defer_relay_admission: bool,
        fallback_reason: str | None,
    ) -> tuple[PlannerTransferPlan, tuple[PlannerLease, ...], str | None]:
        plan = self._plan_or_direct(
            total_bytes=total_bytes,
            chunk_bytes=chunk_bytes,
            ranges=ranges,
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
        if lease_error is None:
            return plan, leases, fallback_reason
        fallback_reason = lease_error
        plan = self._direct_plan(
            total_bytes=total_bytes,
            chunk_bytes=chunk_bytes,
            ranges=ranges,
            profile=profile,
            direction=direction,
        )
        return plan, (), fallback_reason

    def _scheduling_decision(
        self,
        *,
        session: Session,
        plan: PlannerTransferPlan,
        leases: tuple[PlannerLease, ...],
        stats: PlannerStats,
        profile: _Profile,
        runtime_load: RuntimeLoadView,
        relay_policy: _RelayPolicy,
        fallback_reason: str | None,
        now: float,
        job_id: str | None,
        intent_id: str | None,
        topology_snapshot_id: str | None,
        relay_eligibility: Mapping[str, object] | None,
        runtime_state: Mapping[str, object] | None,
        direction: str,
        total_bytes: int,
        pressure_summaries: Mapping[str, Mapping[str, object]],
    ) -> SchedulingDecision:
        cost_model = _scheduler_cost_model_metadata(
            plan=plan,
            profile=profile,
            runtime_view=runtime_load,
            direction=direction,
            total_bytes=total_bytes,
            pressure_summaries=pressure_summaries,
        )
        adaptive_policy = _adaptive_policy_metadata_from_cost_model(
            runtime_load=runtime_load,
            direction=direction,
            cost_model=cost_model,
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
            fallback_reason=None if fallback_reason is None else str(fallback_reason),
            issued_at=float(now),
            metadata={
                "leases": [lease.as_dict() for lease in leases],
                "stats": stats.as_dict(),
                "cost_model": cost_model,
                "runtime_state": dict(runtime_load.runtime_state),
                "topology": _topology_metadata(
                    topology_snapshot_id=topology_snapshot_id,
                    relay_eligibility=relay_eligibility,
                ),
                "policy": runtime_load.policy_metadata(),
                "adaptive_policy": adaptive_policy,
                "relay_policy": relay_policy.as_dict(),
                "block_plan": (
                    dict(plan.block_plan)
                    if isinstance(plan.block_plan, Mapping)
                    else {}
                ),
            },
        )

    def _plan_with_block_metadata(
        self,
        *,
        plan: PlannerTransferPlan,
        direction: str,
        decision_seed: str,
        runtime_load: RuntimeLoadView,
        fallback_reason: str | None,
    ) -> PlannerTransferPlan:
        block_plan = _block_plan_for_transfer_plan(
            plan,
            decision_seed=str(decision_seed),
            direction=direction,
            scheduler_metadata={
                "runtime_state_version": int(
                    runtime_load.runtime_state.get("version", 0) or 0
                ),
                "fallback_reason": fallback_reason,
            },
        )
        metadata = {
            **dict(plan.cost_metadata),
            "block_plan": _block_plan_metadata(block_plan),
        }
        return PlannerTransferPlan(
            total_bytes=plan.total_bytes,
            chunk_bytes=plan.chunk_bytes,
            assignments=plan.assignments,
            cost_metadata=metadata,
            block_plan=block_plan.as_dict(),
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
        pressure_summaries: Mapping[str, Mapping[str, object]],
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
            return _profile_miss_result(session, empty_policy)
        if not bool(session.worker_relay_capable):
            return _session_not_relay_capable_result(
                profile_entry=profile_entry,
                payload=payload,
                session=session,
                defer_relay_admission=defer_relay_admission,
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
        relay_candidates = _relay_candidates_for_profile(
            profile_entry=profile_entry,
            payload=payload,
            session=session,
            relay_quotas=relay_quotas,
            runtime_view=runtime_view,
            direction=direction,
            defer_relay_admission=defer_relay_admission,
            pressure_summaries=pressure_summaries,
        )

        direct_profile = _direct_profile_with_runtime_feedback(
            profile_entry=profile_entry,
            payload=payload,
            session=session,
            direct_h2d=direct_h2d,
            direct_d2h=direct_d2h,
            runtime_view=runtime_view,
            pressure_summaries=pressure_summaries,
        )
        direct_adjustment = dict(direct_profile.cost_metadata["path_cost_model"])
        selected_relays = relay_candidates.available_profiles
        if not selected_relays and defer_relay_admission:
            selected_relays = relay_candidates.deferred_profiles
        load_adjustments = (direct_adjustment, *relay_candidates.load_adjustments)
        relay_policy = _RelayPolicy(
            available_relays=tuple(
                relay.relay_device for relay in relay_candidates.available_profiles
            ),
            deferred_relays=relay_candidates.deferred_relays,
            filtered_relays=relay_candidates.filtered_relays,
            load_adjustments=load_adjustments,
            defer_relay_admission=bool(defer_relay_admission),
        )
        return (
            _profile_with_selected_relays(direct_profile, selected_relays),
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
            direct_scheduler_weight_h2d_gbps=(
                profile.direct_scheduler_weight_h2d_gbps
            ),
            direct_scheduler_weight_d2h_gbps=(
                profile.direct_scheduler_weight_d2h_gbps
            ),
            direct_runtime_pressure_h2d=profile.direct_runtime_pressure_h2d,
            direct_runtime_pressure_d2h=profile.direct_runtime_pressure_d2h,
            cost_metadata=dict(profile.cost_metadata or {}),
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


def _adaptive_policy_metadata_from_cost_model(
    *,
    runtime_load: RuntimeLoadView,
    direction: str,
    cost_model: Mapping[str, object],
) -> dict[str, object]:
    direct_path_pressure: float | None = None
    relay_path_pressures: dict[int, float] = {}
    for candidate in cost_model.get("candidate_paths", ()) or ():
        if not isinstance(candidate, Mapping):
            continue
        path_pressure = float(candidate.get("runtime_pressure", 0.0) or 0.0)
        if str(candidate.get("kind", "")).lower() == "direct":
            direct_path_pressure = path_pressure
            continue
        if str(candidate.get("kind", "")).lower() != "relay":
            continue
        relay_device = candidate.get("relay_device")
        if relay_device is None:
            continue
        relay_path_pressures[int(relay_device)] = path_pressure
    pressure_summary = cost_model.get("runtime_pressure_summary")
    return runtime_load.adaptive_policy_metadata(
        direction,
        pressure_summary=(
            pressure_summary if isinstance(pressure_summary, Mapping) else None
        ),
        direct_path_pressure=direct_path_pressure,
        relay_path_pressures=relay_path_pressures,
    )


def _relay_candidates_for_profile(
    *,
    profile_entry: Mapping[str, object] | None,
    payload: Mapping[str, object],
    session: Session,
    relay_quotas: Mapping[int, RelayQuota],
    runtime_view: RuntimeLoadView,
    direction: str,
    defer_relay_admission: bool,
    pressure_summaries: Mapping[str, Mapping[str, object]],
) -> _RelayCandidateSet:
    available_relays: list[_RelayProfile] = []
    deferred_relays: list[dict[str, object]] = []
    deferred_relay_profiles: list[_RelayProfile] = []
    filtered_relays: list[dict[str, object]] = []
    load_adjustments: list[dict[str, object]] = []
    allowed_relays = set(int(gpu) for gpu in session.relay_gpus)
    for relay in payload.get("relays", []) or []:
        if not isinstance(relay, Mapping):
            continue
        relay_device = int(relay["relay_device"])
        static_filter_reason = _static_relay_filter_reason(
            relay,
            relay_device=relay_device,
            allowed_relays=allowed_relays,
        )
        if static_filter_reason is not None:
            filtered_relays.append(
                {"relay_device": relay_device, "reason": static_filter_reason}
            )
            continue
        relay_profile = _relay_profile_from_payload(
            profile_entry,
            relay,
            relay_device=relay_device,
            target_gpu=session.target_gpu,
        )
        unavailable_reason = _relay_unavailable_reason(
            session=session,
            quota=relay_quotas.get(relay_device),
            relay_device=relay_device,
            runtime_view=runtime_view,
            direction=direction,
        )
        if unavailable_reason is None:
            adjusted_profile, relay_adjustment = _relay_profile_with_load_feedback(
                relay_profile,
                runtime_view=runtime_view,
                admission_state="available",
                admission_reason=None,
                pressure_summaries=pressure_summaries,
            )
            load_adjustments.append(relay_adjustment)
            available_relays.append(adjusted_profile)
            continue
        if defer_relay_admission:
            adjusted_profile, relay_adjustment = _relay_profile_with_load_feedback(
                relay_profile,
                runtime_view=runtime_view,
                admission_state="deferred",
                admission_reason=unavailable_reason,
                pressure_summaries=pressure_summaries,
            )
            load_adjustments.append(relay_adjustment)
            deferred_relay_profiles.append(adjusted_profile)
            deferred_relays.append(
                {"relay_device": relay_device, "reason": unavailable_reason}
            )
            continue
        load_adjustments.append(
            _relay_filtered_cost_record(
                relay_profile,
                runtime_view=runtime_view,
                admission_reason=unavailable_reason,
                pressure_summaries=pressure_summaries,
            )
        )
        filtered_relays.append(
            {"relay_device": relay_device, "reason": unavailable_reason}
        )
    return _RelayCandidateSet(
        available_profiles=tuple(available_relays),
        deferred_profiles=tuple(deferred_relay_profiles),
        deferred_relays=tuple(deferred_relays),
        filtered_relays=tuple(filtered_relays),
        load_adjustments=tuple(load_adjustments),
    )


def _profile_miss_result(
    session: Session,
    relay_policy: _RelayPolicy,
) -> tuple[_Profile, str | None, _RelayPolicy]:
    return (
        _direct_fallback_profile(session.target_gpu),
        "daemon profile miss",
        relay_policy,
    )


def _session_not_relay_capable_result(
    *,
    profile_entry: Mapping[str, object] | None,
    payload: Mapping[str, object],
    session: Session,
    defer_relay_admission: bool,
) -> tuple[_Profile, str | None, _RelayPolicy]:
    direct_h2d = float(payload.get("direct_h2d_bw_gbps", 0.0) or 0.0)
    return (
        _Profile(
            target_device=int(payload.get("target_device", session.target_gpu)),
            direct_h2d_bw_gbps=direct_h2d,
            direct_d2h_bw_gbps=float(
                payload.get("direct_d2h_bw_gbps", direct_h2d) or 0.0
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


def _direct_profile_with_runtime_feedback(
    *,
    profile_entry: Mapping[str, object] | None,
    payload: Mapping[str, object],
    session: Session,
    direct_h2d: float,
    direct_d2h: float,
    runtime_view: RuntimeLoadView,
    pressure_summaries: Mapping[str, Mapping[str, object]],
) -> _Profile:
    direct_weight_h2d, direct_weight_d2h, direct_adjustment = (
        _direct_scheduler_weights(
            direct_h2d,
            direct_d2h,
            runtime_view=runtime_view,
            admission_state="available",
            target_device=session.target_gpu,
            pressure_summaries=pressure_summaries,
        )
    )
    return _Profile(
        target_device=int(payload.get("target_device", session.target_gpu)),
        direct_h2d_bw_gbps=direct_h2d,
        direct_d2h_bw_gbps=direct_d2h,
        relays=(),
        direct_scheduler_weight_h2d_gbps=direct_weight_h2d,
        direct_scheduler_weight_d2h_gbps=direct_weight_d2h,
        direct_runtime_pressure_h2d=float(direct_adjustment["h2d_pressure"]),
        direct_runtime_pressure_d2h=float(direct_adjustment["d2h_pressure"]),
        cost_metadata={
            **_profile_cost_context(profile_entry),
            "source": "daemon_scheduler_unified_cost_model",
            "path_cost_model": direct_adjustment,
            "admission_state": direct_adjustment.get("admission_state"),
        },
    )


def _profile_with_selected_relays(
    profile: _Profile,
    selected_relays: tuple[_RelayProfile, ...],
) -> _Profile:
    return _Profile(
        target_device=profile.target_device,
        direct_h2d_bw_gbps=profile.direct_h2d_bw_gbps,
        direct_d2h_bw_gbps=profile.direct_d2h_bw_gbps,
        relays=selected_relays,
        direct_scheduler_weight_h2d_gbps=profile.direct_scheduler_weight_h2d_gbps,
        direct_scheduler_weight_d2h_gbps=profile.direct_scheduler_weight_d2h_gbps,
        direct_runtime_pressure_h2d=profile.direct_runtime_pressure_h2d,
        direct_runtime_pressure_d2h=profile.direct_runtime_pressure_d2h,
        cost_metadata=dict(profile.cost_metadata),
    )


def _runtime_pressure_summaries(
    runtime_view: RuntimeLoadView,
) -> dict[str, Mapping[str, object]]:
    return {
        "h2d": runtime_view.scheduler_pressure_summary("h2d"),
        "d2h": runtime_view.scheduler_pressure_summary("d2h"),
    }


def _static_relay_filter_reason(
    relay: Mapping[str, object],
    *,
    relay_device: int,
    allowed_relays: set[int],
) -> str | None:
    if relay_device not in allowed_relays:
        return "relay is not assigned to session"
    if not bool(relay.get("p2p_enabled", False)):
        return "relay p2p is disabled"
    if float(relay.get("p2p_bw_gbps", 0.0) or 0.0) <= 0.0:
        return "relay p2p bandwidth is unavailable"
    return None


def _relay_profile_from_payload(
    profile_entry: Mapping[str, object] | None,
    relay: Mapping[str, object],
    *,
    relay_device: int,
    target_gpu: int,
) -> _RelayProfile:
    return _RelayProfile(
        relay_device=relay_device,
        target_device=int(relay.get("target_device", target_gpu)),
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


def _validated_plan_transfer_inputs(
    *,
    session: Session,
    total_bytes: int,
    chunk_bytes: int,
    ranges: tuple[Mapping[str, int], ...] | None,
    direction: str,
) -> tuple[int, int, tuple[Mapping[str, int], ...] | None, str]:
    normalized_total_bytes = int(total_bytes)
    normalized_chunk_bytes = int(chunk_bytes)
    normalized_ranges = _normalize_ranges(ranges)
    normalized_direction = str(direction).lower()
    if normalized_total_bytes < 0:
        raise ValueError("total_bytes must be non-negative")
    if normalized_chunk_bytes <= 0:
        raise ValueError("chunk_bytes must be positive")
    if normalized_ranges is not None:
        range_bytes = sum(item["bytes"] for item in normalized_ranges)
        if range_bytes != normalized_total_bytes:
            raise ValueError("range bytes must match total_bytes")
    if normalized_direction not in {"h2d", "d2h"}:
        raise ValueError("direction must be h2d or d2h")
    if not session.active:
        raise ValueError("session is closed")
    return (
        normalized_total_bytes,
        normalized_chunk_bytes,
        normalized_ranges,
        normalized_direction,
    )


def _relay_profile_fallback_reason(
    *,
    fallback_reason: str | None,
    planning_mode: TransferMode,
    session: Session,
    profile: _Profile,
) -> str | None:
    if fallback_reason is not None:
        return fallback_reason
    if planning_mode is TransferMode.DIRECT:
        return None
    if not session.worker_relay_capable:
        return None
    if not session.relay_gpus:
        return None
    if profile.relays:
        return None
    return "no daemon-approved relay path"


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
            "fabric_capability_summary": {},
        }
    fabric_summary = relay_eligibility.get("fabric_capability_summary")
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
        "fabric_capability_summary": (
            dict(fabric_summary) if isinstance(fabric_summary, Mapping) else {}
        ),
    }


def _contract_id(value: str | None, *, prefix: str, fallback: str) -> str:
    if value is not None and str(value).strip():
        return str(value)
    return f"{prefix}-{fallback}"


def _profile_payload(profile_entry: Mapping[str, object] | None) -> Mapping[str, object] | None:
    if not profile_entry:
        return None
    profile = profile_entry.get("profile")
    if isinstance(profile, Mapping):
        return profile
    return profile_entry


def _relay_unavailable_reason(
    *,
    session: Session,
    quota: RelayQuota | None,
    relay_device: int,
    runtime_view: RuntimeLoadView,
    direction: str,
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
        direction=str(direction),
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
