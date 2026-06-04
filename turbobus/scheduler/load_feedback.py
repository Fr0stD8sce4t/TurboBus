from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Iterable as TypingIterable

from ..planner_types import PlannerTransferPlan
from ..schema import WorkloadKind


@dataclass(frozen=True)
class RuntimeLoadView:
    job_id: str | None
    workload_kind: str
    priority: int
    busy_relays: frozenset[int]
    job_weight: float
    total_weight: float
    current_job_active_bytes: int
    total_active_bytes: int
    request_charge_bytes: float
    average_weighted_active_bytes: float
    current_weighted_active_bytes: float
    projected_weighted_active_bytes: float
    fairness_threshold_bytes: float
    active_transfer_count: int
    running_transfer_count: int
    queued_transfer_count: int
    delayed_transfer_count: int

    def policy_metadata(self) -> dict[str, object]:
        return {
            "job_id": self.job_id,
            "job_weight": self.job_weight,
            "total_weight": self.total_weight,
            "workload_kind": self.workload_kind,
            "priority": self.priority,
            "request_charge_bytes": self.request_charge_bytes,
            "current_job_active_bytes": self.current_job_active_bytes,
            "total_active_bytes": self.total_active_bytes,
            "current_weighted_active_bytes": self.current_weighted_active_bytes,
            "projected_weighted_active_bytes": self.projected_weighted_active_bytes,
            "average_weighted_active_bytes": self.average_weighted_active_bytes,
            "fairness_threshold_bytes": self.fairness_threshold_bytes,
            "busy_relays": tuple(sorted(self.busy_relays)),
            "active_transfer_count": self.active_transfer_count,
            "running_transfer_count": self.running_transfer_count,
            "queued_transfer_count": self.queued_transfer_count,
            "delayed_transfer_count": self.delayed_transfer_count,
        }


def runtime_state_metadata(
    runtime_state: Mapping[str, object] | None,
) -> dict[str, object]:
    if not isinstance(runtime_state, Mapping):
        return {
            "version": 0,
            "queued_transfer_count": 0,
            "delayed_transfer_count": 0,
            "running_transfer_count": 0,
            "active_transfer_count": 0,
            "active_reservation_count": 0,
            "active_lease_count": 0,
            "relay_staging_count": 0,
            "relay_path_count": 0,
            "relay_path_bytes_total": 0,
            "completion_source_counts": {},
            "terminal_completion_source_counts": {},
            "busy_relays": (),
            "active_bytes_by_direction": {},
            "queued_bytes_by_direction": {},
            "active_resource_usage": {},
        }
    summary = runtime_state.get("summary", {})
    if not isinstance(summary, Mapping):
        summary = {}
    completion_source_counts = summary.get("completion_source_counts", {})
    if not isinstance(completion_source_counts, Mapping):
        completion_source_counts = {}
    terminal_completion_source_counts = summary.get(
        "terminal_completion_source_counts",
        {},
    )
    if not isinstance(terminal_completion_source_counts, Mapping):
        terminal_completion_source_counts = {}
    return {
        "version": int(runtime_state.get("version", 0) or 0),
        "queued_transfer_count": int(summary.get("queued_transfer_count", 0) or 0),
        "delayed_transfer_count": int(summary.get("delayed_transfer_count", 0) or 0),
        "running_transfer_count": int(summary.get("running_transfer_count", 0) or 0),
        "active_transfer_count": int(summary.get("active_transfer_count", 0) or 0),
        "active_reservation_count": int(summary.get("active_reservation_count", 0) or 0),
        "active_lease_count": int(summary.get("active_lease_count", 0) or 0),
        "relay_staging_count": int(summary.get("relay_staging_count", 0) or 0),
        "relay_path_count": int(summary.get("relay_path_count", 0) or 0),
        "relay_path_bytes_total": int(summary.get("relay_path_bytes_total", 0) or 0),
        "completion_source_counts": {
            str(key): int(value)
            for key, value in completion_source_counts.items()
        },
        "terminal_completion_source_counts": {
            str(key): int(value)
            for key, value in terminal_completion_source_counts.items()
        },
        "busy_relays": tuple(int(item) for item in summary.get("busy_relays", ()) or ()),
        "active_bytes_by_direction": dict(
            summary.get("active_bytes_by_direction", {}) or {}
        ),
        "queued_bytes_by_direction": dict(
            summary.get("queued_bytes_by_direction", {}) or {}
        ),
        "active_resource_usage": dict(summary.get("active_resource_usage", {}) or {}),
    }


def runtime_view(
    *,
    runtime_state: Mapping[str, object] | None,
    job_id: str | None,
    total_bytes: int,
    workload_kind: WorkloadKind | str,
    priority: int,
) -> RuntimeLoadView:
    normalized_job_id = None if job_id is None else str(job_id)
    workload = WorkloadKind(workload_kind).value
    active_transfer_count = 0
    running_transfer_count = 0
    queued_transfer_count = 0
    delayed_transfer_count = 0
    job_runtime_state: Mapping[str, object] = {}
    if isinstance(runtime_state, Mapping):
        summary = runtime_state.get("summary", {})
        if isinstance(summary, Mapping):
            active_transfer_count = int(summary.get("active_transfer_count", 0) or 0)
            running_transfer_count = int(summary.get("running_transfer_count", 0) or 0)
            queued_transfer_count = int(summary.get("queued_transfer_count", 0) or 0)
            delayed_transfer_count = int(summary.get("delayed_transfer_count", 0) or 0)
            nested_jobs = summary.get("job_runtime_state", {})
            if isinstance(nested_jobs, Mapping):
                job_runtime_state = nested_jobs
        jobs = runtime_state.get("job_runtime_state", {})
        if isinstance(jobs, Mapping):
            job_runtime_state = jobs
    busy_relays = busy_relays_from_runtime_state(runtime_state)

    total_weight = 0.0
    total_active_bytes = 0
    current_job_active_bytes = 0
    job_weight = 1.0
    for key, value in job_runtime_state.items():
        if not isinstance(value, Mapping):
            continue
        weight = max(0.0, float(value.get("weight", 1.0) or 1.0))
        total_weight += weight
        active_bytes = int(value.get("active_bytes_remaining", 0) or 0)
        total_active_bytes += active_bytes
        if normalized_job_id is not None and str(key) == normalized_job_id:
            job_weight = weight or 1.0
            current_job_active_bytes = active_bytes
    if total_weight <= 0.0:
        total_weight = max(1.0, job_weight)
    if normalized_job_id is not None and normalized_job_id not in job_runtime_state:
        total_weight += job_weight
    request_charge = float(total_bytes) * workload_charge_multiplier(workload)
    if int(priority) > 0:
        request_charge = request_charge / (1.0 + min(int(priority), 9) * 0.1)
    current_weighted = current_job_active_bytes / max(job_weight, 1e-12)
    projected_weighted = (current_job_active_bytes + request_charge) / max(
        job_weight,
        1e-12,
    )
    average_weighted = (
        (total_active_bytes + request_charge) / max(total_weight, 1e-12)
    )
    return RuntimeLoadView(
        job_id=normalized_job_id,
        workload_kind=workload,
        priority=int(priority),
        busy_relays=frozenset(busy_relays),
        job_weight=job_weight,
        total_weight=total_weight,
        current_job_active_bytes=current_job_active_bytes,
        total_active_bytes=total_active_bytes,
        request_charge_bytes=request_charge,
        average_weighted_active_bytes=average_weighted,
        current_weighted_active_bytes=current_weighted,
        projected_weighted_active_bytes=projected_weighted,
        fairness_threshold_bytes=average_weighted * 1.25,
        active_transfer_count=active_transfer_count,
        running_transfer_count=running_transfer_count,
        queued_transfer_count=queued_transfer_count,
        delayed_transfer_count=delayed_transfer_count,
    )


def workload_charge_multiplier(workload_kind: str) -> float:
    if workload_kind == WorkloadKind.KV_CACHE.value:
        return 0.75
    if workload_kind == WorkloadKind.TRAINING_STATE.value:
        return 1.25
    if workload_kind == WorkloadKind.OPTIMIZER_STATE.value:
        return 1.25
    return 1.0


def fairness_fallback_for_plan(
    *,
    plan: PlannerTransferPlan,
    runtime_view: RuntimeLoadView,
) -> str | None:
    has_relay = any(assignment.path.kind == "relay" for assignment in plan.assignments)
    if not has_relay:
        return None
    if runtime_view.total_active_bytes <= 0:
        return None
    running_pressure = min(runtime_view.running_transfer_count, 8) * 0.05
    effective_threshold = runtime_view.fairness_threshold_bytes / (1.0 + running_pressure)
    if runtime_view.projected_weighted_active_bytes <= effective_threshold:
        return None
    return "weighted fairness limit prefers direct fallback"


def busy_relays_from_runtime_state(
    runtime_state: Mapping[str, object] | None,
) -> set[int]:
    busy: set[int] = set()
    if not isinstance(runtime_state, Mapping):
        return busy
    for record in _runtime_records(runtime_state.get("active_paths", ())):
        if not isinstance(record, Mapping):
            continue
        if str(record.get("kind", "")).lower() != "relay":
            continue
        relay = record.get("relay_device")
        if relay is not None:
            busy.add(int(relay))
    for key in ("active_leases", "active_reservations", "relay_staging"):
        for record in _runtime_records(runtime_state.get(key, ())):
            if not isinstance(record, Mapping):
                continue
            relay = record.get("relay_gpu")
            if relay is not None:
                busy.add(int(relay))
    return busy


def _runtime_records(value: object) -> TypingIterable[object]:
    if isinstance(value, list | tuple):
        return value
    return ()


__all__ = [
    "RuntimeLoadView",
    "busy_relays_from_runtime_state",
    "fairness_fallback_for_plan",
    "runtime_state_metadata",
    "runtime_view",
    "workload_charge_multiplier",
]
