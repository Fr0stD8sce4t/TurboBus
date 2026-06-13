from __future__ import annotations

from collections.abc import Mapping

_ADMISSION_DELAYED = "delayed"


def ordered_delayed_admission_records(
    *,
    transfer_admissions: Mapping[str, Mapping[str, object]],
    transfer_queue_records: Mapping[str, Mapping[str, object]],
    runtime_state: Mapping[str, object],
    now: float,
) -> tuple[dict[str, object], ...]:
    records = [
        admission_priority_record(
            transfer_id=str(transfer_id),
            admission=admission,
            queue_record=transfer_queue_records.get(str(transfer_id), {}),
            runtime_state=runtime_state,
            now=now,
        )
        for transfer_id, admission in transfer_admissions.items()
        if isinstance(admission, Mapping)
        and admission.get("state") == _ADMISSION_DELAYED
    ]
    return tuple(
        sorted(
            records,
            key=lambda item: (
                -float(item["priority_score"]),
                -int(item["priority"]),
                float(item["delayed_at"]),
                str(item["transfer_id"]),
            ),
        )
    )


def admission_with_priority_evidence(
    admission: Mapping[str, object],
    priority_order: Mapping[str, object],
) -> dict[str, object]:
    updated = dict(admission)
    evidence = updated.get("multi_tenant_admission")
    if isinstance(evidence, Mapping):
        admission_evidence = dict(evidence)
        admission_evidence["priority_order"] = dict(priority_order)
        updated["multi_tenant_admission"] = admission_evidence
    return updated


def admission_priority_record(
    *,
    transfer_id: str,
    admission: Mapping[str, object] | None,
    queue_record: Mapping[str, object] | None,
    runtime_state: Mapping[str, object],
    now: float,
) -> dict[str, object]:
    admission_map = admission if isinstance(admission, Mapping) else {}
    record = queue_record if isinstance(queue_record, Mapping) else {}
    transfer_id = str(transfer_id)
    job_id = record.get("job_id")
    priority = int(record.get("priority", 0) or 0)
    requested_chunks = int(admission_map.get("requested_chunks", 0) or 0)
    bytes_total = int(record.get("bytes_total", 0) or 0)
    delayed_at = float(
        admission_map.get(
            "delayed_at",
            record.get("queued_at", now),
        )
        or now
    )
    wait_seconds = max(0.0, float(now) - delayed_at)
    runtime_jobs = runtime_state.get("job_runtime_state", {})
    job_active_bytes = 0
    job_backlog_bytes = 0
    job_running_count = 0
    if job_id is not None and isinstance(runtime_jobs, Mapping):
        job_record = runtime_jobs.get(str(job_id), {})
        if isinstance(job_record, Mapping):
            job_active_bytes = int(job_record.get("active_bytes_remaining", 0) or 0)
            job_backlog_bytes = _job_delayed_backlog_bytes(job_record)
            job_running_count = int(job_record.get("running_transfer_count", 0) or 0)
    readiness_bonus = admission_runtime_readiness_bonus(
        admission=admission_map,
        runtime_state=runtime_state,
    )
    fairness_penalty = admission_fairness_penalty(admission_map)
    score = 0.0
    score += max(0, priority) * 1000.0
    score += min(wait_seconds, 3600.0) * 0.20
    score += min(bytes_total / (64.0 * 1024 * 1024), 64.0) * 1.5
    score += readiness_bonus
    score -= fairness_penalty
    score -= min(max(requested_chunks, 0), 1024) * 0.35
    score -= min(job_active_bytes / (64.0 * 1024 * 1024), 64.0) * 2.0
    score -= min(job_backlog_bytes / (64.0 * 1024 * 1024), 64.0) * 1.0
    score -= min(max(job_running_count, 0), 32) * 3.0
    return {
        "transfer_id": transfer_id,
        "job_id": None if job_id is None else str(job_id),
        "priority": priority,
        "priority_score": score,
        "delayed_at": delayed_at,
        "wait_seconds": wait_seconds,
        "requested_chunks": requested_chunks,
        "bytes_total": bytes_total,
        "job_active_bytes": job_active_bytes,
        "job_backlog_bytes": job_backlog_bytes,
        "job_running_count": job_running_count,
        "runtime_readiness_bonus": readiness_bonus,
        "fairness_penalty": fairness_penalty,
        "source": "daemon_admission_priority_queue",
    }


def admission_runtime_readiness_bonus(
    *,
    admission: Mapping[str, object],
    runtime_state: Mapping[str, object],
) -> float:
    requested_chunks = int(admission.get("requested_chunks", 0) or 0)
    active_leases = int(
        dict(runtime_state.get("summary", {}) or {}).get("active_lease_count", 0)
        if isinstance(runtime_state.get("summary", {}), Mapping)
        else 0
    )
    active_reservations = int(
        dict(runtime_state.get("summary", {}) or {}).get(
            "active_reservation_count",
            0,
        )
        if isinstance(runtime_state.get("summary", {}), Mapping)
        else 0
    )
    delayed_count = int(
        dict(runtime_state.get("summary", {}) or {}).get("delayed_transfer_count", 0)
        if isinstance(runtime_state.get("summary", {}), Mapping)
        else 0
    )
    bonus = 25.0
    bonus -= min(active_leases, 32) * 0.8
    bonus -= min(active_reservations, 32) * 0.8
    bonus -= min(delayed_count, 32) * 0.3
    bonus -= min(max(requested_chunks, 0), 1024) * 0.05
    return bonus


def admission_fairness_penalty(
    admission: Mapping[str, object],
) -> float:
    fairness = admission.get("fairness")
    if not isinstance(fairness, Mapping):
        return 0.0
    threshold = max(1.0, float(fairness.get("fairness_threshold_bytes", 0.0) or 0.0))
    projected = float(
        fairness.get(
            "projected_weighted_fairness_bytes",
            fairness.get("projected_weighted_active_bytes", 0.0),
        )
        or 0.0
    )
    overage = max(0.0, projected - threshold)
    penalty = min(overage / threshold, 4.0) * 20.0
    if fairness.get("blocked_reason") is not None:
        penalty += 10.0
    return penalty


def _job_delayed_backlog_bytes(job_record: Mapping[str, object]) -> int:
    delayed = job_record.get("delayed_bytes_total")
    if delayed is not None:
        return max(0, int(delayed or 0))
    queued = int(job_record.get("queued_bytes_total", 0) or 0)
    admitted = int(job_record.get("admitted_bytes_total", 0) or 0)
    return max(0, queued - admitted)


__all__ = [
    "admission_priority_record",
    "admission_with_priority_evidence",
    "ordered_delayed_admission_records",
]
