from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict

from ..schema import (
    JobIdentity,
    PeerIdentity,
    RelayQuota,
    Session,
    TransferStatusState,
)
from ..scheduler.load_feedback import (
    relay_activity_from_runtime_state,
)
from .runtime_state_summary import (
    job_runtime_state_from_records as _job_runtime_state_from_records,
    runtime_mapping_records,
    runtime_mapping_records_from_sources,
)


_TERMINAL_TRANSFER_STATE_VALUES = {
    TransferStatusState.COMPLETE.value,
    TransferStatusState.FAILED.value,
    TransferStatusState.CANCELED.value,
}


def daemon_runtime_telemetry_snapshot(
    *,
    runtime_state: Mapping[str, object],
    relay_quotas: Mapping[int, RelayQuota],
    sessions: Mapping[str, Session],
    jobs: Mapping[str, JobIdentity],
    requester_peer_identity: PeerIdentity | None,
) -> dict[str, object]:
    summary = dict(runtime_state.get("summary", {}) or {})
    relay_activity = relay_activity_from_runtime_state(runtime_state)
    return {
        "schema_version": 1,
        "source": "daemon_runtime_telemetry",
        "version": int(runtime_state.get("version", 0) or 0),
        "captured_at": float(runtime_state.get("captured_at", 0.0) or 0.0),
        "requester_peer_identity": (
            None if requester_peer_identity is None else asdict(requester_peer_identity)
        ),
        "summary": runtime_telemetry_summary(summary),
        "queue": runtime_telemetry_queue_snapshot(runtime_state),
        "execution": runtime_telemetry_execution_snapshot(runtime_state, summary),
        "terminal": runtime_telemetry_terminal_snapshot(runtime_state, summary),
        "relays": runtime_telemetry_relay_snapshot(
            runtime_state=runtime_state,
            busy_relays=relay_activity["busy_relays"],
            relay_load=relay_activity["relay_load"],
            relay_quotas=relay_quotas,
        ),
        "pcie_bandwidth_pool": runtime_telemetry_pcie_bandwidth_pool(runtime_state),
        "hardware_monitoring": runtime_telemetry_hardware_monitoring(runtime_state),
        "tenant_usage": dict(runtime_state.get("tenant_usage", {}) or {}),
        "quota_rejections": tuple(
            dict(item)
            for item in runtime_state.get("quota_rejections", ()) or ()
            if isinstance(item, Mapping)
        ),
        "jobs": runtime_telemetry_jobs_snapshot(runtime_state, jobs),
        "sessions": runtime_telemetry_sessions_snapshot(sessions),
        "worker_feedback": dict(summary.get("runtime_feedback_metrics", {}) or {}),
    }


def runtime_telemetry_queue_snapshot(
    runtime_state: Mapping[str, object],
) -> dict[str, object]:
    return {
        "transfer_order": tuple(runtime_state.get("transfer_order", ()) or ()),
        "queued": _telemetry_transfer_records(runtime_state.get("queued_transfers", ())),
        "admitted": _telemetry_transfer_records(
            runtime_state.get("admitted_transfers", ())
        ),
        "delayed": _telemetry_transfer_records(
            runtime_state.get("delayed_transfers", ())
        ),
    }


def runtime_telemetry_execution_snapshot(
    runtime_state: Mapping[str, object],
    summary: Mapping[str, object],
) -> dict[str, object]:
    return {
        "running": _telemetry_transfer_records(
            runtime_state.get("running_transfers", ())
        ),
        "active": _telemetry_transfer_records(
            runtime_state.get("active_transfers", ())
        ),
        "active_paths": tuple(
            runtime_telemetry_path_record(record)
            for record in runtime_mapping_records(runtime_state.get("active_paths", ()))
        ),
        "active_resource_usage": dict(
            runtime_state.get("active_resource_usage", {}) or {}
        ),
        "active_execution_evidence": dict(
            summary.get("active_execution_evidence", {}) or {}
        ),
        "active_execution_evidence_by_source": _mapping_records_by_key(
            summary.get("active_execution_evidence_by_source", {})
        ),
    }


def runtime_telemetry_terminal_snapshot(
    runtime_state: Mapping[str, object],
    summary: Mapping[str, object],
) -> dict[str, object]:
    return {
        "recent": _telemetry_transfer_records(
            runtime_state.get("recent_terminal_transfers", ())
        ),
        "terminal_execution_evidence": dict(
            summary.get("terminal_execution_evidence", {}) or {}
        ),
        "terminal_execution_evidence_by_source": _mapping_records_by_key(
            summary.get("terminal_execution_evidence_by_source", {})
        ),
        "terminal_completion_source_counts": dict(
            summary.get("terminal_completion_source_counts", {}) or {}
        ),
    }


def runtime_telemetry_relay_snapshot(
    *,
    runtime_state: Mapping[str, object],
    busy_relays: object,
    relay_load: Mapping[int, Mapping[str, object]],
    relay_quotas: Mapping[int, RelayQuota],
) -> dict[str, object]:
    return {
        "busy_relays": tuple(int(item) for item in busy_relays or ()),
        "relay_load": {
            int(relay): dict(record)
            for relay, record in sorted(relay_load.items())
        },
        "active_reservations": _telemetry_mapping_tuple(
            runtime_state.get("active_reservations", ())
        ),
        "active_leases": _telemetry_mapping_tuple(
            runtime_state.get("active_leases", ())
        ),
        "relay_staging": _telemetry_mapping_tuple(
            runtime_state.get("relay_staging", ())
        ),
        "quota": {
            int(relay): runtime_telemetry_quota_record(quota)
            for relay, quota in sorted(relay_quotas.items())
        },
    }


def runtime_telemetry_pcie_bandwidth_pool(
    runtime_state: Mapping[str, object],
) -> dict[str, object]:
    pool = runtime_state.get("pcie_bandwidth_pool", {})
    if not isinstance(pool, Mapping):
        return {
            "source": "daemon_pcie_bandwidth_pool",
            "available": False,
            "reason": "missing_pcie_bandwidth_pool",
            "paths": {},
            "edges": {},
        }
    return {
        "source": str(pool.get("source", "daemon_pcie_bandwidth_pool")),
        "available": bool(pool.get("available", False)),
        "reason": pool.get("reason"),
        "topology_snapshot_id": pool.get("topology_snapshot_id"),
        "topology_version": pool.get("topology_version"),
        "paths": _mapping_records_by_key(pool.get("paths", {})),
        "edges": _mapping_records_by_key(pool.get("edges", {})),
    }


def runtime_telemetry_hardware_monitoring(
    runtime_state: Mapping[str, object],
) -> dict[str, object]:
    monitoring = runtime_state.get("hardware_monitoring", {})
    if not isinstance(monitoring, Mapping):
        return {
            "source": "nvidia_smi_dmon",
            "known": False,
            "error": "missing_hardware_monitoring",
            "counters": (),
        }
    return {
        "source": str(monitoring.get("source", "unknown")),
        "known": bool(monitoring.get("known", False)),
        "sampled_at": float(monitoring.get("sampled_at", 0.0) or 0.0),
        "sample_age_ms": float(monitoring.get("sample_age_ms", 0.0) or 0.0),
        "error": monitoring.get("error"),
        "counters": tuple(
            _public_hardware_counter(item)
            for item in monitoring.get("counters", ()) or ()
            if isinstance(item, Mapping)
        ),
    }


def _public_hardware_counter(counter: Mapping[str, object]) -> dict[str, object]:
    device_id = counter.get("device_id")
    rx_mib_s = counter.get("rx_mib_s")
    tx_mib_s = counter.get("tx_mib_s")
    sample_age_ms = counter.get("sample_age_ms")
    return {
        "device_id": -1 if device_id is None else int(device_id),
        "rx_mib_s": 0.0 if rx_mib_s is None else float(rx_mib_s),
        "tx_mib_s": 0.0 if tx_mib_s is None else float(tx_mib_s),
        "sample_age_ms": 0.0 if sample_age_ms is None else float(sample_age_ms),
        "source": str(counter.get("source", "unknown")),
        "known": bool(counter.get("known", False)),
        "error": counter.get("error"),
    }


def runtime_telemetry_jobs_snapshot(
    runtime_state: Mapping[str, object],
    jobs: Mapping[str, JobIdentity],
) -> dict[str, object]:
    job_runtime_state = _runtime_state_job_summary(runtime_state)
    return {
        "runtime_state": job_runtime_state,
        "registered": {
            str(job_id): runtime_telemetry_job_record(job)
            for job_id, job in sorted(jobs.items())
        },
    }


def _runtime_state_job_summary(
    runtime_state: Mapping[str, object],
) -> dict[str, dict[str, object]]:
    summary = runtime_state.get("summary", {})
    if isinstance(summary, Mapping):
        job_summary = summary.get("job_runtime_state")
        if isinstance(job_summary, Mapping):
            return {
                str(job_id): dict(record)
                for job_id, record in job_summary.items()
                if isinstance(record, Mapping)
            }
    job_runtime_state = runtime_state.get("job_runtime_state", {})
    if not isinstance(job_runtime_state, Mapping):
        job_runtime_state = {}
    return _job_runtime_state_from_records(
        job_runtime_state,
        runtime_state.get("transfers", ()),
    )


def runtime_telemetry_sessions_snapshot(
    sessions: Mapping[str, Session],
) -> dict[str, object]:
    return {
        str(session_id): runtime_telemetry_session_record(session)
        for session_id, session in sorted(sessions.items())
    }


def runtime_telemetry_summary(summary: Mapping[str, object]) -> dict[str, object]:
    return {
        "queued_transfer_count": int(summary.get("queued_transfer_count", 0) or 0),
        "admitted_transfer_count": int(summary.get("admitted_transfer_count", 0) or 0),
        "delayed_transfer_count": int(summary.get("delayed_transfer_count", 0) or 0),
        "running_transfer_count": int(summary.get("running_transfer_count", 0) or 0),
        "active_transfer_count": int(summary.get("active_transfer_count", 0) or 0),
        "recent_terminal_transfer_count": int(
            summary.get("recent_terminal_transfer_count", 0) or 0
        ),
        "terminal_transfer_count": int(summary.get("terminal_transfer_count", 0) or 0),
        "active_reservation_count": int(summary.get("active_reservation_count", 0) or 0),
        "active_lease_count": int(summary.get("active_lease_count", 0) or 0),
        "relay_staging_count": int(summary.get("relay_staging_count", 0) or 0),
        "relay_path_count": int(summary.get("relay_path_count", 0) or 0),
        "relay_path_bytes_total": int(summary.get("relay_path_bytes_total", 0) or 0),
        "completion_source_counts": dict(summary.get("completion_source_counts", {}) or {}),
        "queued_bytes_by_direction": dict(
            summary.get("queued_bytes_by_direction", {}) or {}
        ),
        "active_bytes_by_direction": dict(
            summary.get("active_bytes_by_direction", {}) or {}
        ),
        "active_paths": dict(summary.get("active_paths", {}) or {}),
    }


def _telemetry_transfer_records(records: object) -> tuple[dict[str, object], ...]:
    return tuple(
        runtime_telemetry_transfer_record(record)
        for record in runtime_mapping_records(records)
    )


def _telemetry_mapping_tuple(records: object) -> tuple[dict[str, object], ...]:
    return tuple(dict(record) for record in runtime_mapping_records(records))


def _mapping_records_by_key(records: object) -> dict[str, dict[str, object]]:
    return {
        str(key): dict(value)
        for key, value in dict(records or {}).items()
        if isinstance(value, Mapping)
    }


def runtime_telemetry_quota_record(quota: RelayQuota) -> dict[str, object]:
    return {
        "relay_gpu": int(quota.relay_gpu),
        "max_sessions": int(quota.max_sessions),
        "max_inflight_chunks": int(quota.max_inflight_chunks),
        "active_chunks": int(quota.active_chunks),
        "sessions": tuple(sorted(str(item) for item in quota.sessions)),
    }


def runtime_telemetry_job_record(job: JobIdentity) -> dict[str, object]:
    return {
        "job_id": job.job_id,
        "user_id": job.user_id,
        "session_id": job.session_id,
        "container_id": job.container_id,
        "process_id": job.process_id,
        "weight": float(job.weight),
    }


def runtime_telemetry_session_record(session: Session) -> dict[str, object]:
    return {
        "session_id": session.session_id,
        "target_gpu": int(session.target_gpu),
        "relay_gpus": tuple(int(gpu) for gpu in session.relay_gpus),
        "max_inflight_chunks": int(session.max_inflight_chunks),
        "active_chunks": int(session.active_chunks),
        "active": bool(session.active),
        "worker_relay_capable": bool(session.worker_relay_capable),
    }


def runtime_telemetry_transfer_record(record: Mapping[str, object]) -> dict[str, object]:
    return {
        "transfer_id": str(record.get("transfer_id", "")),
        "intent_id": record.get("intent_id"),
        "job_id": record.get("job_id"),
        "session_id": record.get("session_id"),
        "state": str(record.get("state", "")),
        "direction": str(record.get("direction", "unknown")),
        "bytes_total": int(record.get("bytes_total", 0) or 0),
        "bytes_completed": int(record.get("bytes_completed", 0) or 0),
        "bytes_remaining": max(
            0,
            int(record.get("bytes_total", 0) or 0)
            - int(record.get("bytes_completed", 0) or 0),
        ),
        "chunk_bytes": int(record.get("chunk_bytes", 0) or 0),
        "workload_kind": record.get("workload_kind"),
        "priority": int(record.get("priority", 0) or 0),
        "admission_state": record.get("admission_state"),
        "admission_reason": record.get("admission_reason"),
        "completion_source": record.get("completion_source"),
        "plan_generation": int(record.get("plan_generation", 0) or 0),
        "block_plan": (
            dict(record["block_plan"])
            if isinstance(record.get("block_plan"), Mapping)
            else {}
        ),
        "block_queue": (
            dict(record["block_queue"])
            if isinstance(record.get("block_queue"), Mapping)
            else {}
        ),
        "queued_at": record.get("queued_at"),
        "started_at": record.get("started_at"),
        "completed_at": record.get("completed_at"),
        "fallback_reason": record.get("fallback_reason"),
    }


def runtime_telemetry_path_record(record: Mapping[str, object]) -> dict[str, object]:
    return {
        "transfer_id": str(record.get("transfer_id", "")),
        "kind": str(record.get("kind", "unknown")),
        "direction": str(record.get("direction", "unknown")),
        "target_device": record.get("target_device"),
        "relay_device": record.get("relay_device"),
        "bytes_total": int(record.get("bytes_total", 0) or 0),
        "chunk_count": int(record.get("chunk_count", 0) or 0),
        "completion_source": record.get("completion_source"),
        "phase": record.get("phase"),
    }


def refresh_runtime_feedback_summary(runtime_state: dict[str, object]) -> None:
    summary = runtime_state.get("summary")
    if not isinstance(summary, Mapping):
        return
    summary_copy = dict(summary)
    transfers = runtime_state.get("transfers", ())
    recent_terminal_transfers = runtime_state.get("recent_terminal_transfers", ())
    terminal_records = runtime_mapping_records_from_sources(
        transfers,
        recent_terminal_transfers,
    )
    terminal_feedback = terminal_runtime_feedback_from_records(terminal_records)
    (
        path_summary,
        relay_path_summary,
        active_execution_evidence,
        active_execution_evidence_by_source,
    ) = active_path_runtime_feedback(runtime_state.get("active_paths", ()))
    active_by_direction = direction_bytes_from_summary_or_records(
        summary_copy,
        field_name="active_bytes_by_direction",
        records=runtime_state.get("active_transfers", ()),
        include_remaining=True,
    )
    queued_by_direction = direction_bytes_from_summary_or_records(
        summary_copy,
        field_name="queued_bytes_by_direction",
        records=runtime_state.get("queued_transfers", ()),
        include_remaining=False,
    )
    active_resource_usage = active_resource_usage_summary(
        summary_copy,
        runtime_state=runtime_state,
        active_by_direction=active_by_direction,
        path_summary=path_summary,
        relay_path_summary=relay_path_summary,
    )
    relay_activity = relay_activity_from_runtime_state(runtime_state)
    summary_copy.update(
        runtime_feedback_summary_update(
            runtime_state=runtime_state,
            relay_activity=relay_activity,
            relay_path_summary=relay_path_summary,
            queued_by_direction=queued_by_direction,
            active_by_direction=active_by_direction,
            path_summary=path_summary,
            active_resource_usage=active_resource_usage,
            completion_source_counts=terminal_feedback["completion_source_counts"],
            terminal_completion_source_counts=terminal_feedback[
                "terminal_completion_source_counts"
            ],
            active_execution_evidence=active_execution_evidence,
            active_execution_evidence_by_source=active_execution_evidence_by_source,
            terminal_execution_evidence=terminal_feedback[
                "terminal_execution_evidence"
            ],
            terminal_execution_evidence_by_source=terminal_feedback[
                "terminal_execution_evidence_by_source"
            ],
            runtime_feedback_metrics=terminal_feedback["runtime_feedback_metrics"],
        )
    )
    runtime_state["active_resource_usage"] = active_resource_usage
    runtime_state["summary"] = summary_copy


def active_path_runtime_feedback(
    active_paths: object,
) -> tuple[
    dict[str, dict[str, int]],
    dict[str, int],
    dict[str, int],
    dict[str, dict[str, int]],
]:
    path_summary: dict[str, dict[str, int]] = {}
    relay_path_summary = {"path_count": 0, "chunk_count": 0, "bytes_total": 0}
    active_execution_evidence = empty_execution_path_evidence()
    active_execution_evidence_by_source: dict[str, dict[str, int]] = {}
    for record in runtime_mapping_records(active_paths):
        _accumulate_active_path_runtime_feedback(
            record,
            path_summary=path_summary,
            relay_path_summary=relay_path_summary,
            active_execution_evidence=active_execution_evidence,
            active_execution_evidence_by_source=active_execution_evidence_by_source,
        )
    return (
        path_summary,
        relay_path_summary,
        active_execution_evidence,
        active_execution_evidence_by_source,
    )


def _accumulate_active_path_runtime_feedback(
    record: Mapping[str, object],
    *,
    path_summary: dict[str, dict[str, int]],
    relay_path_summary: dict[str, int],
    active_execution_evidence: dict[str, int],
    active_execution_evidence_by_source: dict[str, dict[str, int]],
) -> None:
    kind = str(record.get("kind", "unknown"))
    direction = str(record.get("direction", "unknown"))
    key = f"{direction}:{kind}"
    chunk_count = int(record.get("chunk_count", 0) or 0)
    bytes_total = int(record.get("bytes_total", 0) or 0)
    bucket = path_summary.setdefault(
        key,
        {"path_count": 0, "chunk_count": 0, "bytes_total": 0},
    )
    bucket["path_count"] += 1
    bucket["chunk_count"] += chunk_count
    bucket["bytes_total"] += bytes_total
    if kind == "relay":
        relay_path_summary["path_count"] += 1
        relay_path_summary["chunk_count"] += chunk_count
        relay_path_summary["bytes_total"] += bytes_total
    accumulate_execution_path_evidence(
        active_execution_evidence,
        kind=kind,
        bytes_total=bytes_total,
        chunk_count=chunk_count,
    )
    completion_source = str(record.get("completion_source", "")).lower()
    if not completion_source:
        return
    source_bucket = active_execution_evidence_by_source.setdefault(
        completion_source,
        empty_execution_path_evidence(),
    )
    accumulate_execution_path_evidence(
        source_bucket,
        kind=kind,
        bytes_total=bytes_total,
        chunk_count=chunk_count,
    )


def completion_source_count_summary(
    transfers: object,
    recent_terminal_transfers: object,
) -> tuple[dict[str, int], dict[str, int]]:
    terminal_records = runtime_mapping_records_from_sources(
        transfers,
        recent_terminal_transfers,
    )
    feedback = terminal_runtime_feedback_from_records(terminal_records)
    return (
        dict(feedback["completion_source_counts"]),
        dict(feedback["terminal_completion_source_counts"]),
    )


def active_resource_usage_summary(
    summary: Mapping[str, object],
    *,
    runtime_state: Mapping[str, object],
    active_by_direction: Mapping[str, object],
    path_summary: Mapping[str, Mapping[str, int]],
    relay_path_summary: Mapping[str, int],
) -> dict[str, object]:
    active_resource_usage = dict(summary.get("active_resource_usage", {}) or {})
    direct_path_usage = direct_path_resource_usage_by_direction(
        path_summary,
        active_by_direction=active_by_direction,
    )
    active_resource_usage["h2d"] = direct_path_usage["h2d"]
    active_resource_usage["d2h"] = direct_path_usage["d2h"]
    active_resource_usage["p2p"] = dict(relay_path_summary)
    relay_staging = dict(active_resource_usage.get("relay_staging", {}) or {})
    relay_staging.update(
        {
            "count": len(runtime_state.get("relay_staging", ()) or ()),
            "active_reservation_count": len(
                runtime_state.get("active_reservations", ()) or ()
            ),
            "active_lease_count": len(runtime_state.get("active_leases", ()) or ()),
        }
    )
    active_resource_usage["relay_staging"] = relay_staging
    return active_resource_usage


def direct_path_resource_usage_by_direction(
    path_summary: Mapping[str, Mapping[str, int]],
    *,
    active_by_direction: Mapping[str, object],
) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for direction in ("h2d", "d2h"):
        direct_key = f"{direction}:direct"
        has_direction_path = any(
            str(key).startswith(f"{direction}:") for key in path_summary
        )
        direct_summary = path_summary.get(direct_key, {})
        if isinstance(direct_summary, Mapping) and direct_summary:
            result[direction] = {
                "transfer_count": int(direct_summary.get("path_count", 0) or 0),
                "path_count": int(direct_summary.get("path_count", 0) or 0),
                "chunk_count": int(direct_summary.get("chunk_count", 0) or 0),
                "bytes_total": int(direct_summary.get("bytes_total", 0) or 0),
                "bytes_remaining": int(direct_summary.get("bytes_total", 0) or 0),
                "source": "active_direct_paths",
            }
            continue
        if has_direction_path:
            result[direction] = {
                "transfer_count": 0,
                "path_count": 0,
                "chunk_count": 0,
                "bytes_total": 0,
                "bytes_remaining": 0,
                "source": "active_direct_paths",
            }
            continue
        fallback = active_by_direction.get(direction, {})
        result[direction] = dict(fallback) if isinstance(fallback, Mapping) else {}
        if result[direction]:
            result[direction]["source"] = "active_transfer_direction"
    return result


def runtime_feedback_summary_update(
    *,
    runtime_state: Mapping[str, object],
    relay_activity: Mapping[str, object] | None = None,
    relay_path_summary: Mapping[str, int],
    queued_by_direction: Mapping[str, object],
    active_by_direction: Mapping[str, object],
    path_summary: Mapping[str, object],
    active_resource_usage: Mapping[str, object],
    completion_source_counts: Mapping[str, int],
    terminal_completion_source_counts: Mapping[str, int],
    active_execution_evidence: Mapping[str, int],
    active_execution_evidence_by_source: Mapping[str, Mapping[str, int]],
    terminal_execution_evidence: Mapping[str, int],
    terminal_execution_evidence_by_source: Mapping[str, Mapping[str, int]],
    runtime_feedback_metrics: Mapping[str, object],
) -> dict[str, object]:
    if not isinstance(relay_activity, Mapping):
        relay_activity = relay_activity_from_runtime_state(runtime_state)
    return {
        "queued_transfer_count": len(runtime_state.get("queued_transfers", ()) or ()),
        "admitted_transfer_count": len(
            runtime_state.get("admitted_transfers", ()) or ()
        ),
        "delayed_transfer_count": len(runtime_state.get("delayed_transfers", ()) or ()),
        "running_transfer_count": len(runtime_state.get("running_transfers", ()) or ()),
        "active_transfer_count": len(runtime_state.get("active_transfers", ()) or ()),
        "recent_terminal_transfer_count": len(
            runtime_state.get("recent_terminal_transfers", ()) or ()
        ),
        "active_reservation_count": len(
            runtime_state.get("active_reservations", ()) or ()
        ),
        "active_lease_count": len(runtime_state.get("active_leases", ()) or ()),
        "relay_staging_count": len(runtime_state.get("relay_staging", ()) or ()),
        "relay_path_count": int(relay_path_summary["path_count"]),
        "relay_path_bytes_total": int(relay_path_summary["bytes_total"]),
        "busy_relays": tuple(sorted(relay_activity["busy_relays"])),
        "relay_load": dict(relay_activity["relay_load"]),
        "queued_bytes_by_direction": dict(queued_by_direction),
        "active_bytes_by_direction": dict(active_by_direction),
        "active_paths": dict(path_summary),
        "active_resource_usage": dict(active_resource_usage),
        "completion_source_counts": dict(completion_source_counts),
        "terminal_completion_source_counts": dict(terminal_completion_source_counts),
        "active_execution_evidence": dict(active_execution_evidence),
        "active_execution_evidence_by_source": {
            str(source): dict(record)
            for source, record in active_execution_evidence_by_source.items()
        },
        "terminal_execution_evidence": dict(terminal_execution_evidence),
        "terminal_execution_evidence_by_source": {
            str(source): dict(record)
            for source, record in terminal_execution_evidence_by_source.items()
        },
        "runtime_feedback_metrics": dict(runtime_feedback_metrics),
    }


def empty_execution_path_evidence() -> dict[str, int]:
    return {
        "direct_bytes": 0,
        "direct_chunks": 0,
        "relay_bytes": 0,
        "relay_chunks": 0,
    }


def accumulate_execution_path_evidence(
    bucket: dict[str, int],
    *,
    kind: str,
    bytes_total: int,
    chunk_count: int,
) -> None:
    normalized_kind = str(kind).lower()
    if normalized_kind == "direct":
        bucket["direct_bytes"] = int(bucket.get("direct_bytes", 0)) + max(0, int(bytes_total))
        bucket["direct_chunks"] = int(bucket.get("direct_chunks", 0)) + max(0, int(chunk_count))
    elif normalized_kind == "relay":
        bucket["relay_bytes"] = int(bucket.get("relay_bytes", 0)) + max(0, int(bytes_total))
        bucket["relay_chunks"] = int(bucket.get("relay_chunks", 0)) + max(0, int(chunk_count))


def terminal_execution_evidence_from_records(
    records: object,
) -> dict[str, int]:
    feedback = terminal_runtime_feedback_from_records(records)
    return dict(feedback["terminal_execution_evidence"])


def terminal_execution_evidence_by_source_from_records(
    records: object,
) -> dict[str, dict[str, int]]:
    feedback = terminal_runtime_feedback_from_records(records)
    return {
        str(source): dict(record)
        for source, record in feedback["terminal_execution_evidence_by_source"].items()
    }


def terminal_runtime_feedback_from_records(records: object) -> dict[str, object]:
    terminal_execution_evidence = empty_execution_path_evidence()
    terminal_execution_evidence_by_source: dict[str, dict[str, int]] = {}
    completion_source_counts: dict[str, int] = {}
    terminal_completion_source_counts: dict[str, int] = {}
    metrics = _empty_runtime_feedback_metrics()
    for record in runtime_mapping_records(records):
        completion_source = str(record.get("completion_source", "")).lower()
        terminal_state = str(record.get("state")) in _TERMINAL_TRANSFER_STATE_VALUES
        if completion_source:
            completion_source_counts[completion_source] = (
                completion_source_counts.get(completion_source, 0) + 1
            )
            if terminal_state:
                terminal_completion_source_counts[completion_source] = (
                    terminal_completion_source_counts.get(completion_source, 0) + 1
                )
        _accumulate_runtime_feedback_metrics(
            metrics,
            record,
            completion_source=completion_source,
            terminal_state=terminal_state,
        )
        if not terminal_state:
            continue
        evidence = record.get("completion_evidence")
        if not isinstance(evidence, Mapping):
            continue
        path_evidence = evidence.get("execution_path_evidence")
        if not isinstance(path_evidence, Mapping):
            continue
        _accumulate_terminal_path_evidence(
            terminal_execution_evidence,
            path_evidence,
        )
        if completion_source:
            source_bucket = terminal_execution_evidence_by_source.setdefault(
                completion_source,
                empty_execution_path_evidence(),
            )
            _accumulate_terminal_path_evidence(source_bucket, path_evidence)
    return {
        "terminal_execution_evidence": terminal_execution_evidence,
        "terminal_execution_evidence_by_source": terminal_execution_evidence_by_source,
        "completion_source_counts": completion_source_counts,
        "terminal_completion_source_counts": terminal_completion_source_counts,
        "runtime_feedback_metrics": metrics,
    }


def _accumulate_terminal_path_evidence(
    bucket: dict[str, int],
    path_evidence: Mapping[str, object],
) -> None:
    bucket["direct_bytes"] += int(path_evidence.get("direct_bytes", 0) or 0)
    bucket["direct_chunks"] += int(path_evidence.get("direct_chunks", 0) or 0)
    bucket["relay_bytes"] += int(path_evidence.get("relay_bytes", 0) or 0)
    bucket["relay_chunks"] += int(path_evidence.get("relay_chunks", 0) or 0)


def runtime_feedback_metrics_from_records(
    records: object,
) -> dict[str, object]:
    metrics = _empty_runtime_feedback_metrics()
    for record in runtime_mapping_records(records):
        completion_source = str(record.get("completion_source", "")).lower()
        terminal_state = str(record.get("state")) in _TERMINAL_TRANSFER_STATE_VALUES
        _accumulate_runtime_feedback_metrics(
            metrics,
            record,
            completion_source=completion_source,
            terminal_state=terminal_state,
        )
    return metrics


def _empty_runtime_feedback_metrics() -> dict[str, object]:
    return {
        "source": "daemon_runtime_feedback_metrics",
        "worker_completion_count": 0,
        "backend_completion_count": 0,
        "cleanup_ok_count": 0,
        "cleanup_failed_count": 0,
        "worker_async_pool": {
            "queued": 0,
            "running": 0,
            "complete": 0,
            "failed": 0,
            "canceled": 0,
            "unknown": 0,
            "max_terminal_history_limit": 0,
            "terminal_history_evictions": 0,
        },
        "worker_executor_runtime": {
            "samples": 0,
            "executor_count": 0,
            "executors": {},
            "runtime_reused": 0,
            "runtime_created": 0,
            "max_runtime_cache_size": 0,
            "max_runtime_cache_limit": 0,
            "max_runtime_cache_over_limit": 0,
            "max_active_runtime_cache_key_count": 0,
            "max_active_cached_runtime_key_count": 0,
            "runtime_cache_evictions": 0,
            "runtime_cache_eviction_records": (),
            "max_runtime_key_lock_count": 0,
            "max_runtime_key_waiter_count": 0,
            "max_inflight_count": 0,
            "max_terminal_count": 0,
            "max_terminal_history_limit": 0,
            "terminal_history_evictions": 0,
            "max_submit_to_complete_ms": 0.0,
            "relay_gpu_count": 0,
            "target_devices": (),
        },
        "backend_direct_runtime": {
            "samples": 0,
            "runtime_reused": 0,
            "runtime_created": 0,
            "max_runtime_cache_size": 0,
            "max_runtime_cache_limit": 0,
            "max_runtime_cache_over_limit": 0,
            "runtime_cache_evictions": 0,
            "max_runtime_key_lock_count": 0,
            "max_runtime_key_waiter_count": 0,
            "target_devices": (),
        },
        "cuda_ipc_span_validation": {
            "validated": 0,
            "failed": 0,
            "missing": 0,
        },
        "recent_terminal_count": 0,
    }


def _accumulate_runtime_feedback_metrics(
    metrics: dict[str, object],
    record: Mapping[str, object],
    *,
    completion_source: str,
    terminal_state: bool,
) -> None:
    if completion_source == "worker":
        metrics["worker_completion_count"] = int(metrics["worker_completion_count"]) + 1
    elif completion_source == "backend":
        metrics["backend_completion_count"] = int(metrics["backend_completion_count"]) + 1
    if terminal_state:
        metrics["recent_terminal_count"] = int(metrics["recent_terminal_count"]) + 1
    evidence = record.get("completion_evidence")
    if not isinstance(evidence, Mapping):
        return
    cleanup = evidence.get("cleanup")
    if isinstance(cleanup, Mapping):
        if bool(cleanup.get("ok", False)):
            metrics["cleanup_ok_count"] = int(metrics["cleanup_ok_count"]) + 1
        else:
            metrics["cleanup_failed_count"] = int(metrics["cleanup_failed_count"]) + 1
    worker_async_pool = evidence.get("worker_async_pool")
    if isinstance(worker_async_pool, Mapping):
        state = str(worker_async_pool.get("state", "unknown")).lower()
        pool_metrics = dict(metrics["worker_async_pool"])
        if state not in pool_metrics:
            state = "unknown"
        pool_metrics[state] = int(pool_metrics.get(state, 0)) + 1
        pool_metrics["max_terminal_history_limit"] = max(
            int(pool_metrics.get("max_terminal_history_limit", 0) or 0),
            int(worker_async_pool.get("terminal_history_limit", 0) or 0),
        )
        pool_metrics["terminal_history_evictions"] = max(
            int(pool_metrics.get("terminal_history_evictions", 0) or 0),
            int(worker_async_pool.get("terminal_history_evictions", 0) or 0),
        )
        metrics["worker_async_pool"] = pool_metrics
    worker_runtime_feedback = evidence.get("worker_runtime_feedback")
    if isinstance(worker_runtime_feedback, Mapping):
        metrics["worker_executor_runtime"] = merge_worker_runtime_feedback_metrics(
            metrics["worker_executor_runtime"],
            worker_runtime_feedback,
        )
    direct_runtime = evidence.get("direct_runtime")
    if isinstance(direct_runtime, Mapping):
        metrics["backend_direct_runtime"] = merge_backend_direct_runtime_metrics(
            metrics["backend_direct_runtime"],
            direct_runtime,
        )
    span_state = cuda_ipc_span_validation_state(evidence)
    if span_state is not None:
        span_metrics = dict(metrics["cuda_ipc_span_validation"])
        span_metrics[span_state] = int(span_metrics.get(span_state, 0)) + 1
        metrics["cuda_ipc_span_validation"] = span_metrics


def merge_backend_direct_runtime_metrics(
    existing: object,
    feedback: Mapping[str, object],
) -> dict[str, object]:
    metrics = dict(existing) if isinstance(existing, Mapping) else {}
    metrics["samples"] = int(metrics.get("samples", 0) or 0) + 1
    if bool(feedback.get("runtime_reused", False)):
        metrics["runtime_reused"] = int(metrics.get("runtime_reused", 0) or 0) + 1
    else:
        metrics["runtime_created"] = int(metrics.get("runtime_created", 0) or 0) + 1
    metrics["max_runtime_cache_size"] = max(
        int(metrics.get("max_runtime_cache_size", 0) or 0),
        int(feedback.get("cache_size", 0) or 0),
    )
    metrics["max_runtime_cache_limit"] = max(
        int(metrics.get("max_runtime_cache_limit", 0) or 0),
        int(feedback.get("cache_limit", 0) or 0),
    )
    metrics["max_runtime_cache_over_limit"] = max(
        int(metrics.get("max_runtime_cache_over_limit", 0) or 0),
        int(
            feedback.get(
                "cache_over_limit",
                feedback.get("runtime_cache_over_limit", 0),
            )
            or 0
        ),
    )
    metrics["runtime_cache_evictions"] = max(
        int(metrics.get("runtime_cache_evictions", 0) or 0),
        int(feedback.get("cache_evictions", 0) or 0),
    )
    metrics["runtime_cache_eviction_records"] = _merge_runtime_cache_eviction_records(
        metrics.get("runtime_cache_eviction_records", ()),
        _runtime_cache_eviction_records(
            feedback,
            field_name="cache_eviction_records",
            legacy_field_name="cache_eviction_keys",
        ),
    )
    metrics["max_runtime_key_lock_count"] = max(
        int(metrics.get("max_runtime_key_lock_count", 0) or 0),
        int(
            feedback.get(
                "max_key_lock_count",
                feedback.get("key_lock_count", 0),
            )
            or 0
        ),
    )
    metrics["max_runtime_key_waiter_count"] = max(
        int(metrics.get("max_runtime_key_waiter_count", 0) or 0),
        int(
            feedback.get(
                "max_key_waiter_count",
                feedback.get("key_waiter_count", 0),
            )
            or 0
        ),
    )
    existing_devices = metrics.get("target_devices", ()) or ()
    if not isinstance(existing_devices, list | tuple | set | frozenset):
        existing_devices = ()
    target_devices = {int(item) for item in existing_devices}
    if feedback.get("target_device") is not None:
        target_devices.add(int(feedback["target_device"]))
    metrics["target_devices"] = tuple(sorted(target_devices))
    return metrics


def merge_worker_runtime_feedback_metrics(
    existing: object,
    feedback: Mapping[str, object],
) -> dict[str, object]:
    metrics = dict(existing) if isinstance(existing, Mapping) else {}
    samples = int(metrics.get("samples", 0) or 0) + 1
    metrics["samples"] = samples
    if bool(feedback.get("runtime_reused", False)):
        metrics["runtime_reused"] = int(metrics.get("runtime_reused", 0) or 0) + 1
    else:
        metrics["runtime_created"] = int(metrics.get("runtime_created", 0) or 0) + 1
    executor_id = _worker_runtime_feedback_executor_id(feedback)
    executor_records = _worker_runtime_executor_records(metrics)
    executor_record = dict(executor_records.get(executor_id, {}))
    executor_record.update(
        {
            "executor_id": executor_id,
            "runtime_cache_size": int(feedback.get("runtime_cache_size", 0) or 0),
            "runtime_cache_limit": int(feedback.get("runtime_cache_limit", 0) or 0),
            "runtime_cache_over_limit": int(
                feedback.get("runtime_cache_over_limit", 0) or 0
            ),
            "active_runtime_cache_key_count": int(
                feedback.get("active_runtime_cache_key_count", 0) or 0
            ),
            "active_cached_runtime_key_count": int(
                feedback.get("active_cached_runtime_key_count", 0) or 0
            ),
            "runtime_cache_evictions": int(
                feedback.get("runtime_cache_evictions", 0) or 0
            ),
            "runtime_cache_eviction_records": _runtime_cache_eviction_records(
                feedback,
                field_name="runtime_cache_eviction_records",
                legacy_field_name="runtime_cache_eviction_keys",
            ),
            "runtime_key_lock_count": int(
                feedback.get("runtime_key_lock_count", 0) or 0
            ),
            "runtime_key_waiter_count": int(
                feedback.get("runtime_key_waiter_count", 0) or 0
            ),
            "inflight_count": int(feedback.get("inflight_count", 0) or 0),
            "terminal_count": int(feedback.get("terminal_count", 0) or 0),
            "terminal_history_limit": int(
                feedback.get("terminal_history_limit", 0) or 0
            ),
            "terminal_history_evictions": int(
                feedback.get("terminal_history_evictions", 0) or 0
            ),
        }
    )
    executor_records[executor_id] = executor_record
    metrics["executors"] = executor_records
    metrics["executor_count"] = len(executor_records)
    metrics["max_runtime_cache_size"] = max(
        int(metrics.get("max_runtime_cache_size", 0) or 0),
        int(feedback.get("runtime_cache_size", 0) or 0),
    )
    metrics["max_runtime_cache_limit"] = max(
        int(metrics.get("max_runtime_cache_limit", 0) or 0),
        int(feedback.get("runtime_cache_limit", 0) or 0),
    )
    metrics["max_runtime_cache_over_limit"] = max(
        int(metrics.get("max_runtime_cache_over_limit", 0) or 0),
        int(feedback.get("runtime_cache_over_limit", 0) or 0),
    )
    metrics["max_active_runtime_cache_key_count"] = max(
        int(metrics.get("max_active_runtime_cache_key_count", 0) or 0),
        int(feedback.get("active_runtime_cache_key_count", 0) or 0),
    )
    metrics["max_active_cached_runtime_key_count"] = max(
        int(metrics.get("max_active_cached_runtime_key_count", 0) or 0),
        int(feedback.get("active_cached_runtime_key_count", 0) or 0),
    )
    metrics["runtime_cache_evictions"] = sum(
        int(record.get("runtime_cache_evictions", 0) or 0)
        for record in executor_records.values()
    )
    metrics["runtime_cache_eviction_records"] = _merged_runtime_cache_eviction_records(
        executor_records,
    )
    metrics["max_runtime_key_lock_count"] = max(
        int(metrics.get("max_runtime_key_lock_count", 0) or 0),
        int(
            feedback.get(
                "max_runtime_key_lock_count",
                feedback.get("runtime_key_lock_count", 0),
            )
            or 0
        ),
    )
    metrics["max_runtime_key_waiter_count"] = max(
        int(metrics.get("max_runtime_key_waiter_count", 0) or 0),
        int(
            feedback.get(
                "max_runtime_key_waiter_count",
                feedback.get("runtime_key_waiter_count", 0),
            )
            or 0
        ),
    )
    metrics["max_inflight_count"] = max(
        int(metrics.get("max_inflight_count", 0) or 0),
        int(feedback.get("inflight_count", 0) or 0),
    )
    metrics["max_terminal_count"] = max(
        int(metrics.get("max_terminal_count", 0) or 0),
        int(feedback.get("terminal_count", 0) or 0),
    )
    metrics["max_terminal_history_limit"] = max(
        int(metrics.get("max_terminal_history_limit", 0) or 0),
        int(feedback.get("terminal_history_limit", 0) or 0),
    )
    metrics["terminal_history_evictions"] = sum(
        int(record.get("terminal_history_evictions", 0) or 0)
        for record in executor_records.values()
    )
    submit_to_complete = feedback.get("submit_to_complete_ms")
    if submit_to_complete is not None:
        metrics["max_submit_to_complete_ms"] = max(
            float(metrics.get("max_submit_to_complete_ms", 0.0) or 0.0),
            float(submit_to_complete),
        )
    relay_gpus = feedback.get("relay_gpus", ()) or ()
    if isinstance(relay_gpus, list | tuple):
        metrics["relay_gpu_count"] = max(
            int(metrics.get("relay_gpu_count", 0) or 0),
            len(relay_gpus),
        )
    target_devices = {
        int(item)
        for item in metrics.get("target_devices", ()) or ()
    }
    if feedback.get("target_device") is not None:
        target_devices.add(int(feedback["target_device"]))
    metrics["target_devices"] = tuple(sorted(target_devices))
    return metrics


def _worker_runtime_feedback_executor_id(feedback: Mapping[str, object]) -> str:
    executor_id = feedback.get("executor_id")
    if executor_id is not None and str(executor_id).strip():
        return str(executor_id)
    runtime_cache_key = feedback.get("runtime_cache_key")
    if isinstance(runtime_cache_key, list | tuple) and runtime_cache_key:
        return f"legacy:{tuple(runtime_cache_key)!r}"
    transfer_id = feedback.get("transfer_id")
    if transfer_id is not None:
        return f"legacy-transfer:{transfer_id}"
    return "legacy-unknown"


def _worker_runtime_executor_records(
    metrics: Mapping[str, object],
) -> dict[str, dict[str, object]]:
    executors = metrics.get("executors", {})
    if not isinstance(executors, Mapping):
        return {}
    return {
        str(key): dict(value)
        for key, value in executors.items()
        if isinstance(value, Mapping)
    }


def _runtime_cache_eviction_records(
    feedback: Mapping[str, object],
    *,
    field_name: str = "runtime_cache_eviction_records",
    legacy_field_name: str = "runtime_cache_eviction_keys",
) -> tuple[dict[str, object], ...]:
    records = feedback.get(field_name, ()) or ()
    if not isinstance(records, list | tuple):
        records = feedback.get(legacy_field_name, ()) or ()
    if not isinstance(records, list | tuple):
        return ()
    normalized: list[dict[str, object]] = []
    for record in records:
        normalized_record = _runtime_cache_eviction_record(record)
        if normalized_record:
            normalized.append(normalized_record)
    return tuple(normalized[-8:])


def _merge_runtime_cache_eviction_records(
    existing: object,
    incoming: object,
) -> tuple[dict[str, object], ...]:
    merged: list[dict[str, object]] = []
    for value in (existing, incoming):
        if not isinstance(value, list | tuple):
            continue
        for record in value:
            normalized_record = _runtime_cache_eviction_record(record)
            if normalized_record:
                merged.append(normalized_record)
    return tuple(merged[-8:])


def _merged_runtime_cache_eviction_records(
    executor_records: Mapping[str, Mapping[str, object]],
) -> tuple[dict[str, object], ...]:
    merged: list[dict[str, object]] = []
    for record in executor_records.values():
        if not isinstance(record, Mapping):
            continue
        merged.extend(_runtime_cache_eviction_records(record))
    return tuple(merged[-8:])


def _runtime_cache_eviction_record(record: object) -> dict[str, object]:
    if isinstance(record, Mapping):
        normalized: dict[str, object] = {}
        for key, value in record.items():
            if isinstance(value, bool | int | float):
                normalized[str(key)] = value
            elif isinstance(value, str) or value is None:
                normalized[str(key)] = value
            elif isinstance(value, list | tuple):
                normalized[str(key)] = tuple(value)
        return normalized
    if isinstance(record, list | tuple):
        return {
            "source": "legacy_runtime_cache_key",
            "key_width": len(record),
        }
    return {}


def cuda_ipc_span_validation_state(evidence: Mapping[str, object]) -> str | None:
    resource_evidence = evidence.get("resource_evidence")
    candidates: list[Mapping[str, object]] = []
    if isinstance(resource_evidence, Mapping):
        candidates.append(resource_evidence)
    for field_name in (
        "direct_completion_evidence",
        "relay_completion_evidence",
        "worker_completion_evidence",
    ):
        nested = evidence.get(field_name)
        if isinstance(nested, Mapping):
            nested_resource = nested.get("resource_evidence")
            if isinstance(nested_resource, Mapping):
                candidates.append(nested_resource)
            candidates.append(nested)
    if not candidates:
        return None
    found_span = False
    saw_cuda_ipc = False
    for candidate in candidates:
        if str(candidate.get("device_handle_type", "")).lower() == "cuda_ipc_device":
            saw_cuda_ipc = True
        span = candidate.get("cuda_ipc_span_validation")
        if not isinstance(span, Mapping):
            continue
        found_span = True
        if bool(span.get("validated", False)):
            return "validated"
    if found_span:
        return "failed"
    return "missing" if saw_cuda_ipc else None


def terminal_feedback_record_from_record(
    record: Mapping[str, object],
    *,
    recorded_at: float,
) -> dict[str, object] | None:
    state = str(record.get("state", ""))
    if state not in {
        TransferStatusState.COMPLETE.value,
        TransferStatusState.FAILED.value,
        TransferStatusState.CANCELED.value,
    }:
        return None
    feedback = dict(record)
    feedback["recorded_at"] = float(recorded_at)
    return feedback


def transfer_bytes_by_direction(
    transfers: object,
    *,
    include_remaining: bool,
) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for record in runtime_mapping_records(transfers):
        direction = str(record.get("direction", "unknown"))
        bucket = result.setdefault(
            direction,
            {"transfer_count": 0, "bytes_total": 0},
        )
        bucket["transfer_count"] += 1
        bucket["bytes_total"] += int(record.get("bytes_total", 0) or 0)
        if include_remaining:
            bucket["bytes_remaining"] = int(bucket.get("bytes_remaining", 0)) + max(
                0,
                int(record.get("bytes_total", 0) or 0)
                - int(record.get("bytes_completed", 0) or 0),
            )
    return result


def direction_bytes_from_summary_or_records(
    summary: Mapping[str, object],
    *,
    field_name: str,
    records: object,
    include_remaining: bool,
) -> dict[str, dict[str, int]]:
    existing = summary.get(field_name)
    if isinstance(existing, Mapping):
        normalized = _direction_bytes_mapping(existing, include_remaining)
        if normalized:
            return normalized
    return transfer_bytes_by_direction(
        records,
        include_remaining=include_remaining,
    )


def _direction_bytes_mapping(
    value: Mapping[str, object],
    include_remaining: bool,
) -> dict[str, dict[str, int]]:
    normalized: dict[str, dict[str, int]] = {}
    for direction, record in value.items():
        if not isinstance(record, Mapping):
            continue
        bucket = {
            "transfer_count": int(record.get("transfer_count", 0) or 0),
            "bytes_total": int(record.get("bytes_total", 0) or 0),
        }
        if include_remaining:
            bucket["bytes_remaining"] = int(record.get("bytes_remaining", 0) or 0)
        normalized[str(direction)] = bucket
    return normalized


__all__ = [
    "accumulate_execution_path_evidence",
    "daemon_runtime_telemetry_snapshot",
    "empty_execution_path_evidence",
    "refresh_runtime_feedback_summary",
    "runtime_feedback_metrics_from_records",
    "runtime_mapping_records",
    "runtime_mapping_records_from_sources",
    "terminal_execution_evidence_by_source_from_records",
    "terminal_execution_evidence_from_records",
    "terminal_feedback_record_from_record",
]
