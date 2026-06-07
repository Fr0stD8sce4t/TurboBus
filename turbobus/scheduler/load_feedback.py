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
    runtime_state: dict[str, object]
    resource_pressure: float
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
    relay_load: dict[int, dict[str, object]]
    completion_source_pressure: dict[str, float]
    active_execution_evidence: dict[str, int]
    active_execution_evidence_by_source: dict[str, dict[str, int]]
    terminal_execution_evidence: dict[str, int]
    terminal_execution_evidence_by_source: dict[str, dict[str, int]]

    def policy_metadata(self) -> dict[str, object]:
        runtime_state = dict(self.runtime_state)
        active_resource_usage = dict(runtime_state.get("active_resource_usage", {}) or {})
        return {
            "job_id": self.job_id,
            "job_weight": self.job_weight,
            "total_weight": self.total_weight,
            "workload_kind": self.workload_kind,
            "priority": self.priority,
            "request_charge_bytes": self.request_charge_bytes,
            "resource_pressure": self.resource_pressure,
            "current_job_active_bytes": self.current_job_active_bytes,
            "total_active_bytes": self.total_active_bytes,
            "current_weighted_active_bytes": self.current_weighted_active_bytes,
            "projected_weighted_active_bytes": self.projected_weighted_active_bytes,
            "average_weighted_active_bytes": self.average_weighted_active_bytes,
            "fairness_threshold_bytes": self.fairness_threshold_bytes,
            "busy_relays": tuple(sorted(self.busy_relays)),
            "runtime_state_version": int(runtime_state.get("version", 0) or 0),
            "active_reservation_count": int(
                runtime_state.get("active_reservation_count", 0) or 0
            ),
            "active_lease_count": int(runtime_state.get("active_lease_count", 0) or 0),
            "relay_staging_count": int(runtime_state.get("relay_staging_count", 0) or 0),
            "relay_path_count": int(runtime_state.get("relay_path_count", 0) or 0),
            "relay_path_bytes_total": int(
                runtime_state.get("relay_path_bytes_total", 0) or 0
            ),
            "active_transfer_count": self.active_transfer_count,
            "running_transfer_count": self.running_transfer_count,
            "queued_transfer_count": self.queued_transfer_count,
            "delayed_transfer_count": self.delayed_transfer_count,
            "completion_source_counts": {
                str(key): int(value)
                for key, value in dict(
                    runtime_state.get("completion_source_counts", {}) or {}
                ).items()
            },
            "terminal_completion_source_counts": {
                str(key): int(value)
                for key, value in dict(
                    runtime_state.get("terminal_completion_source_counts", {}) or {}
                ).items()
            },
            "completion_source_pressure": dict(self.completion_source_pressure),
            "active_execution_evidence": dict(self.active_execution_evidence),
            "active_execution_evidence_by_source": {
                str(key): dict(value)
                for key, value in self.active_execution_evidence_by_source.items()
            },
            "terminal_execution_evidence": dict(self.terminal_execution_evidence),
            "terminal_execution_evidence_by_source": {
                str(key): dict(value)
                for key, value in self.terminal_execution_evidence_by_source.items()
            },
            "relay_load": {
                int(relay): dict(record)
                for relay, record in sorted(self.relay_load.items())
            },
            "active_bytes_by_direction": dict(
                runtime_state.get("active_bytes_by_direction", {}) or {}
            ),
            "queued_bytes_by_direction": dict(
                runtime_state.get("queued_bytes_by_direction", {}) or {}
            ),
            "active_resource_usage": active_resource_usage,
        }

    def relay_pressure(self, relay_device: int) -> float:
        record = self.relay_load.get(int(relay_device), {})
        pressure = float(record.get("pressure", 0.0) or 0.0)
        pressure += self.completion_source_pressure.get("worker", 0.0) * 0.10
        relay_bytes = int(self.active_execution_evidence.get("relay_bytes", 0) or 0)
        active_bytes = max(self.total_active_bytes, 1)
        if relay_bytes > 0:
            pressure += min(relay_bytes / active_bytes, 1.0) * 0.14
        pressure += min(self.queued_transfer_count, 8) * 0.015
        pressure += min(self.delayed_transfer_count, 8) * 0.025
        return max(0.0, pressure)

    def direct_pressure(self, direction: str) -> float:
        runtime_state = dict(self.runtime_state)
        active_resource_usage = runtime_state.get("active_resource_usage", {})
        direction_usage = {}
        if isinstance(active_resource_usage, Mapping):
            direction_usage = active_resource_usage.get(str(direction).lower(), {})
        active_bytes = 0
        if isinstance(direction_usage, Mapping):
            active_bytes = int(direction_usage.get("bytes_remaining", 0) or 0)
        pressure = self.completion_source_pressure.get("backend", 0.0) * 0.08
        direct_bytes = int(self.active_execution_evidence.get("direct_bytes", 0) or 0)
        pressure += min(self.running_transfer_count, 8) * 0.02
        pressure += min(self.queued_transfer_count, 8) * 0.01
        if self.total_active_bytes > 0:
            pressure += min(active_bytes / max(self.total_active_bytes, 1), 1.0) * 0.12
            if direct_bytes > 0:
                pressure += min(direct_bytes / max(self.total_active_bytes, 1), 1.0) * 0.10
        return max(0.0, pressure)


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
            "active_execution_evidence": {},
            "active_execution_evidence_by_source": {},
            "terminal_execution_evidence": {},
            "terminal_execution_evidence_by_source": {},
            "busy_relays": (),
            "active_bytes_by_direction": {},
            "queued_bytes_by_direction": {},
            "active_resource_usage": {},
            "relay_load": {},
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
        "active_execution_evidence": dict(summary.get("active_execution_evidence", {}) or {}),
        "active_execution_evidence_by_source": {
            str(key): dict(value)
            for key, value in dict(
                summary.get("active_execution_evidence_by_source", {}) or {}
            ).items()
        },
        "terminal_execution_evidence": dict(
            summary.get("terminal_execution_evidence", {}) or {}
        ),
        "terminal_execution_evidence_by_source": {
            str(key): dict(value)
            for key, value in dict(
                summary.get("terminal_execution_evidence_by_source", {}) or {}
            ).items()
        },
        "busy_relays": tuple(int(item) for item in summary.get("busy_relays", ()) or ()),
        "active_bytes_by_direction": dict(
            summary.get("active_bytes_by_direction", {}) or {}
        ),
        "queued_bytes_by_direction": dict(
            summary.get("queued_bytes_by_direction", {}) or {}
        ),
        "active_resource_usage": dict(summary.get("active_resource_usage", {}) or {}),
        "relay_load": relay_load_from_runtime_state(runtime_state),
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
    runtime_state_snapshot = runtime_state_metadata(runtime_state)
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
    relay_load = relay_load_from_runtime_state(runtime_state)
    completion_source_pressure = completion_source_pressure_from_runtime_state(
        runtime_state_snapshot
    )
    active_execution_evidence = dict(
        runtime_state_snapshot.get("active_execution_evidence", {}) or {}
    )
    active_execution_evidence_by_source = {
        str(key): dict(value)
        for key, value in dict(
            runtime_state_snapshot.get("active_execution_evidence_by_source", {}) or {}
        ).items()
    }
    terminal_execution_evidence = dict(
        runtime_state_snapshot.get("terminal_execution_evidence", {}) or {}
    )
    terminal_execution_evidence_by_source = {
        str(key): dict(value)
        for key, value in dict(
            runtime_state_snapshot.get("terminal_execution_evidence_by_source", {}) or {}
        ).items()
    }

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
    active_reservation_count = int(
        runtime_state_snapshot.get("active_reservation_count", 0) or 0
    )
    active_lease_count = int(runtime_state_snapshot.get("active_lease_count", 0) or 0)
    relay_staging_count = int(runtime_state_snapshot.get("relay_staging_count", 0) or 0)
    relay_path_bytes_total = int(
        runtime_state_snapshot.get("relay_path_bytes_total", 0) or 0
    )
    p2p_bytes_total = 0
    active_resource_usage = runtime_state_snapshot.get("active_resource_usage", {})
    if isinstance(active_resource_usage, Mapping):
        p2p_usage = active_resource_usage.get("p2p", {})
        if isinstance(p2p_usage, Mapping):
            p2p_bytes_total = int(p2p_usage.get("bytes_total", 0) or 0)
    resource_pressure = 0.0
    resource_pressure += min(int(active_transfer_count), 8) * 0.02
    resource_pressure += min(int(running_transfer_count), 8) * 0.05
    resource_pressure += min(active_reservation_count, 8) * 0.03
    resource_pressure += min(active_lease_count, 8) * 0.03
    resource_pressure += min(relay_staging_count, 8) * 0.04
    resource_pressure += min(len(busy_relays), 8) * 0.02
    if total_active_bytes > 0:
        resource_pressure += min(
            (relay_path_bytes_total + p2p_bytes_total) / max(total_active_bytes, 1),
            4.0,
        ) * 0.03
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
        runtime_state=runtime_state_snapshot,
        resource_pressure=resource_pressure,
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
        relay_load=relay_load,
        completion_source_pressure=completion_source_pressure,
        active_execution_evidence=active_execution_evidence,
        active_execution_evidence_by_source=active_execution_evidence_by_source,
        terminal_execution_evidence=terminal_execution_evidence,
        terminal_execution_evidence_by_source=terminal_execution_evidence_by_source,
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
    effective_threshold = runtime_view.fairness_threshold_bytes / (
        1.0 + runtime_view.resource_pressure
    )
    if runtime_view.projected_weighted_active_bytes <= effective_threshold:
        return None
    return "weighted fairness limit prefers direct fallback"


def relay_admission_blocked_reason(
    runtime_view: RuntimeLoadView,
    relay_device: int,
) -> str | None:
    relay = int(relay_device)
    if relay in runtime_view.busy_relays:
        return "relay has active path"
    record = runtime_view.relay_load.get(relay, {})
    pressure = runtime_view.relay_pressure(relay)
    active_path_count = int(record.get("active_path_count", 0) or 0)
    active_chunk_count = int(record.get("active_chunk_count", 0) or 0)
    active_reservation_count = int(record.get("active_reservation_count", 0) or 0)
    active_lease_count = int(record.get("active_lease_count", 0) or 0)
    if active_path_count > 0 and pressure >= 0.30:
        return "relay runtime load still has active path pressure"
    if pressure >= 0.95:
        return "relay runtime load is saturated"
    if runtime_view.delayed_transfer_count > 0 and pressure >= 0.65:
        return "relay runtime load is delayed by queued backlog"
    if (
        runtime_view.running_transfer_count >= 3
        and (active_chunk_count > 0 or active_lease_count > 0)
        and pressure >= 0.50
    ):
        return "relay runtime load is delayed by active worker pressure"
    if active_reservation_count >= 2 and pressure >= 0.45:
        return "relay runtime load is delayed by reservation pressure"
    return None


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


def relay_load_from_runtime_state(
    runtime_state: Mapping[str, object] | None,
) -> dict[int, dict[str, object]]:
    if not isinstance(runtime_state, Mapping):
        return {}
    records: dict[int, dict[str, object]] = {}
    for record in _runtime_records(runtime_state.get("active_paths", ())):
        if not isinstance(record, Mapping):
            continue
        if str(record.get("kind", "")).lower() != "relay":
            continue
        relay = record.get("relay_device")
        if relay is None:
            continue
        bucket = _relay_load_bucket(records, int(relay))
        bucket["active_path_count"] = int(bucket["active_path_count"]) + 1
        bucket["active_chunk_count"] = int(bucket["active_chunk_count"]) + int(
            record.get("chunk_count", 0) or 0
        )
        bucket["active_bytes"] = int(bucket["active_bytes"]) + int(
            record.get("bytes_total", 0) or 0
        )
        transfer_id = record.get("transfer_id")
        if transfer_id is not None:
            bucket["transfer_ids"].add(str(transfer_id))
    for key, count_name in (
        ("active_reservations", "active_reservation_count"),
        ("active_leases", "active_lease_count"),
        ("relay_staging", "staging_record_count"),
    ):
        for record in _runtime_records(runtime_state.get(key, ())):
            if not isinstance(record, Mapping):
                continue
            relay = record.get("relay_gpu")
            if relay is None:
                continue
            bucket = _relay_load_bucket(records, int(relay))
            bucket[count_name] = int(bucket[count_name]) + 1
            transfer_id = record.get("transfer_id")
            if transfer_id is not None:
                bucket["transfer_ids"].add(str(transfer_id))
            job_id = record.get("job_id")
            if job_id is not None:
                bucket["job_ids"].add(str(job_id))
    normalized: dict[int, dict[str, object]] = {}
    for relay, record in records.items():
        active_path_count = int(record["active_path_count"])
        active_chunk_count = int(record["active_chunk_count"])
        active_reservation_count = int(record["active_reservation_count"])
        active_lease_count = int(record["active_lease_count"])
        staging_record_count = int(record["staging_record_count"])
        pressure = 0.0
        pressure += min(active_path_count, 8) * 0.20
        pressure += min(active_chunk_count, 32) * 0.015
        pressure += min(active_reservation_count, 8) * 0.06
        pressure += min(active_lease_count, 8) * 0.05
        pressure += min(staging_record_count, 8) * 0.08
        normalized[int(relay)] = {
            "relay_gpu": int(relay),
            "active_path_count": active_path_count,
            "active_chunk_count": active_chunk_count,
            "active_bytes": int(record["active_bytes"]),
            "active_reservation_count": active_reservation_count,
            "active_lease_count": active_lease_count,
            "staging_record_count": staging_record_count,
            "transfer_ids": tuple(sorted(record["transfer_ids"])),
            "job_ids": tuple(sorted(record["job_ids"])),
            "pressure": pressure,
        }
    return normalized


def completion_source_pressure_from_runtime_state(
    runtime_state: Mapping[str, object] | None,
) -> dict[str, float]:
    if not isinstance(runtime_state, Mapping):
        return {"worker": 0.0, "backend": 0.0}
    source_counts = runtime_state.get("completion_source_counts", {})
    if not isinstance(source_counts, Mapping):
        source_counts = {}
    terminal_source_counts = runtime_state.get("terminal_completion_source_counts", {})
    if not isinstance(terminal_source_counts, Mapping):
        terminal_source_counts = {}
    worker_count = int(source_counts.get("worker", 0) or 0)
    backend_count = int(source_counts.get("backend", 0) or 0)
    worker_recent = int(terminal_source_counts.get("worker", 0) or 0)
    backend_recent = int(terminal_source_counts.get("backend", 0) or 0)
    active_by_source = runtime_state.get("active_execution_evidence_by_source", {})
    if not isinstance(active_by_source, Mapping):
        active_by_source = {}
    terminal_by_source = runtime_state.get("terminal_execution_evidence_by_source", {})
    if not isinstance(terminal_by_source, Mapping):
        terminal_by_source = {}
    worker_active_bytes = _execution_evidence_total_bytes(active_by_source.get("worker"))
    backend_active_bytes = _execution_evidence_total_bytes(active_by_source.get("backend"))
    worker_recent_bytes = _execution_evidence_total_bytes(terminal_by_source.get("worker"))
    backend_recent_bytes = _execution_evidence_total_bytes(terminal_by_source.get("backend"))
    weighted_worker = worker_count + min(worker_recent, 8) * 0.5
    weighted_backend = backend_count + min(backend_recent, 8) * 0.5
    weighted_worker += min(worker_active_bytes / (64.0 * 1024 * 1024), 8.0) * 0.20
    weighted_backend += min(backend_active_bytes / (64.0 * 1024 * 1024), 8.0) * 0.20
    weighted_worker += min(worker_recent_bytes / (128.0 * 1024 * 1024), 8.0) * 0.10
    weighted_backend += min(backend_recent_bytes / (128.0 * 1024 * 1024), 8.0) * 0.10
    total = max(1.0, weighted_worker + weighted_backend)
    return {
        "worker": min(weighted_worker / total, 1.0),
        "backend": min(weighted_backend / total, 1.0),
    }


def _execution_evidence_total_bytes(value: object) -> int:
    if not isinstance(value, Mapping):
        return 0
    return int(value.get("direct_bytes", 0) or 0) + int(value.get("relay_bytes", 0) or 0)


def _relay_load_bucket(
    records: dict[int, dict[str, object]],
    relay: int,
) -> dict[str, object]:
    return records.setdefault(
        int(relay),
        {
            "active_path_count": 0,
            "active_chunk_count": 0,
            "active_bytes": 0,
            "active_reservation_count": 0,
            "active_lease_count": 0,
            "staging_record_count": 0,
            "transfer_ids": set(),
            "job_ids": set(),
        },
    )


def _runtime_records(value: object) -> TypingIterable[object]:
    if isinstance(value, list | tuple):
        return value
    return ()


__all__ = [
    "RuntimeLoadView",
    "busy_relays_from_runtime_state",
    "completion_source_pressure_from_runtime_state",
    "fairness_fallback_for_plan",
    "relay_admission_blocked_reason",
    "relay_load_from_runtime_state",
    "runtime_state_metadata",
    "runtime_view",
    "workload_charge_multiplier",
]
