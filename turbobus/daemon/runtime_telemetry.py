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
    busy_relays_from_runtime_state,
    relay_load_from_runtime_state,
)


def daemon_runtime_telemetry_snapshot(
    *,
    runtime_state: Mapping[str, object],
    relay_quotas: Mapping[int, RelayQuota],
    sessions: Mapping[str, Session],
    jobs: Mapping[str, JobIdentity],
    requester_peer_identity: PeerIdentity | None,
) -> dict[str, object]:
    summary = dict(runtime_state.get("summary", {}) or {})
    relay_load = relay_load_from_runtime_state(runtime_state)
    active_resource_usage = dict(runtime_state.get("active_resource_usage", {}) or {})
    job_runtime_state = {
        str(job_id): dict(record)
        for job_id, record in dict(runtime_state.get("job_runtime_state", {}) or {}).items()
        if isinstance(record, Mapping)
    }
    return {
        "schema_version": 1,
        "source": "daemon_runtime_telemetry",
        "version": int(runtime_state.get("version", 0) or 0),
        "captured_at": float(runtime_state.get("captured_at", 0.0) or 0.0),
        "requester_peer_identity": (
            None if requester_peer_identity is None else asdict(requester_peer_identity)
        ),
        "summary": runtime_telemetry_summary(summary),
        "queue": {
            "transfer_order": tuple(runtime_state.get("transfer_order", ()) or ()),
            "queued": tuple(
                runtime_telemetry_transfer_record(record)
                for record in runtime_mapping_records(
                    runtime_state.get("queued_transfers", ())
                )
            ),
            "admitted": tuple(
                runtime_telemetry_transfer_record(record)
                for record in runtime_mapping_records(
                    runtime_state.get("admitted_transfers", ())
                )
            ),
            "delayed": tuple(
                runtime_telemetry_transfer_record(record)
                for record in runtime_mapping_records(
                    runtime_state.get("delayed_transfers", ())
                )
            ),
        },
        "execution": {
            "running": tuple(
                runtime_telemetry_transfer_record(record)
                for record in runtime_mapping_records(
                    runtime_state.get("running_transfers", ())
                )
            ),
            "active": tuple(
                runtime_telemetry_transfer_record(record)
                for record in runtime_mapping_records(
                    runtime_state.get("active_transfers", ())
                )
            ),
            "active_paths": tuple(
                runtime_telemetry_path_record(record)
                for record in runtime_mapping_records(runtime_state.get("active_paths", ()))
            ),
            "active_resource_usage": active_resource_usage,
            "active_execution_evidence": dict(
                summary.get("active_execution_evidence", {}) or {}
            ),
            "active_execution_evidence_by_source": {
                str(source): dict(value)
                for source, value in dict(
                    summary.get("active_execution_evidence_by_source", {}) or {}
                ).items()
                if isinstance(value, Mapping)
            },
        },
        "terminal": {
            "recent": tuple(
                runtime_telemetry_transfer_record(record)
                for record in runtime_mapping_records(
                    runtime_state.get("recent_terminal_transfers", ())
                )
            ),
            "terminal_execution_evidence": dict(
                summary.get("terminal_execution_evidence", {}) or {}
            ),
            "terminal_execution_evidence_by_source": {
                str(source): dict(value)
                for source, value in dict(
                    summary.get("terminal_execution_evidence_by_source", {}) or {}
                ).items()
                if isinstance(value, Mapping)
            },
            "terminal_completion_source_counts": dict(
                summary.get("terminal_completion_source_counts", {}) or {}
            ),
        },
        "relays": {
            "busy_relays": tuple(int(item) for item in summary.get("busy_relays", ()) or ()),
            "relay_load": {
                int(relay): dict(record)
                for relay, record in sorted(relay_load.items())
            },
            "active_reservations": tuple(
                dict(record)
                for record in runtime_mapping_records(
                    runtime_state.get("active_reservations", ())
                )
            ),
            "active_leases": tuple(
                dict(record)
                for record in runtime_mapping_records(
                    runtime_state.get("active_leases", ())
                )
            ),
            "relay_staging": tuple(
                dict(record)
                for record in runtime_mapping_records(
                    runtime_state.get("relay_staging", ())
                )
            ),
            "quota": {
                int(relay): {
                    "relay_gpu": int(quota.relay_gpu),
                    "max_sessions": int(quota.max_sessions),
                    "max_inflight_chunks": int(quota.max_inflight_chunks),
                    "active_chunks": int(quota.active_chunks),
                    "sessions": tuple(sorted(str(item) for item in quota.sessions)),
                }
                for relay, quota in sorted(relay_quotas.items())
            },
        },
        "jobs": {
            "runtime_state": job_runtime_state,
            "registered": {
                str(job_id): {
                    "job_id": job.job_id,
                    "user_id": job.user_id,
                    "session_id": job.session_id,
                    "container_id": job.container_id,
                    "process_id": job.process_id,
                    "weight": float(job.weight),
                }
                for job_id, job in sorted(jobs.items())
            },
        },
        "sessions": {
            str(session_id): {
                "session_id": session.session_id,
                "target_gpu": int(session.target_gpu),
                "relay_gpus": tuple(int(gpu) for gpu in session.relay_gpus),
                "max_inflight_chunks": int(session.max_inflight_chunks),
                "active_chunks": int(session.active_chunks),
                "active": bool(session.active),
                "worker_relay_capable": bool(session.worker_relay_capable),
            }
            for session_id, session in sorted(sessions.items())
        },
        "worker_feedback": dict(summary.get("runtime_feedback_metrics", {}) or {}),
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
    path_summary: dict[str, dict[str, int]] = {}
    relay_path_summary = {"path_count": 0, "chunk_count": 0, "bytes_total": 0}
    completion_source_counts: dict[str, int] = {}
    terminal_completion_source_counts: dict[str, int] = {}
    active_execution_evidence = empty_execution_path_evidence()
    active_execution_evidence_by_source: dict[str, dict[str, int]] = {}
    terminal_execution_evidence = terminal_execution_evidence_from_records(
        (*runtime_mapping_records(transfers), *runtime_mapping_records(recent_terminal_transfers))
    )
    terminal_execution_evidence_by_source = terminal_execution_evidence_by_source_from_records(
        recent_terminal_transfers
    )
    runtime_feedback_metrics = runtime_feedback_metrics_from_records(
        (*runtime_mapping_records(transfers), *runtime_mapping_records(recent_terminal_transfers))
    )
    active_by_direction = transfer_bytes_by_direction(
        runtime_state.get("active_transfers", ()),
        include_remaining=True,
    )
    queued_by_direction = transfer_bytes_by_direction(
        runtime_state.get("queued_transfers", ()),
        include_remaining=False,
    )
    for record in runtime_mapping_records(runtime_state.get("active_paths", ())):
        kind = str(record.get("kind", "unknown"))
        direction = str(record.get("direction", "unknown"))
        key = f"{direction}:{kind}"
        bucket = path_summary.setdefault(
            key,
            {"path_count": 0, "chunk_count": 0, "bytes_total": 0},
        )
        bucket["path_count"] += 1
        bucket["chunk_count"] += int(record.get("chunk_count", 0) or 0)
        bucket["bytes_total"] += int(record.get("bytes_total", 0) or 0)
        if kind == "relay":
            relay_path_summary["path_count"] += 1
            relay_path_summary["chunk_count"] += int(record.get("chunk_count", 0) or 0)
            relay_path_summary["bytes_total"] += int(record.get("bytes_total", 0) or 0)
        accumulate_execution_path_evidence(
            active_execution_evidence,
            kind=kind,
            bytes_total=int(record.get("bytes_total", 0) or 0),
            chunk_count=int(record.get("chunk_count", 0) or 0),
        )
        completion_source = str(record.get("completion_source", "")).lower()
        if completion_source:
            source_bucket = active_execution_evidence_by_source.setdefault(
                completion_source,
                empty_execution_path_evidence(),
            )
            accumulate_execution_path_evidence(
                source_bucket,
                kind=kind,
                bytes_total=int(record.get("bytes_total", 0) or 0),
                chunk_count=int(record.get("chunk_count", 0) or 0),
            )
    for record in (
        *runtime_mapping_records(transfers),
        *runtime_mapping_records(recent_terminal_transfers),
    ):
        completion_source = str(record.get("completion_source", "")).lower()
        if not completion_source:
            continue
        completion_source_counts[completion_source] = (
            completion_source_counts.get(completion_source, 0) + 1
        )
        if str(record.get("state")) in {
            TransferStatusState.COMPLETE.value,
            TransferStatusState.FAILED.value,
            TransferStatusState.CANCELED.value,
        }:
            terminal_completion_source_counts[completion_source] = (
                terminal_completion_source_counts.get(completion_source, 0) + 1
            )

    active_resource_usage = dict(summary_copy.get("active_resource_usage", {}) or {})
    active_resource_usage["h2d"] = dict(active_by_direction.get("h2d", {}))
    active_resource_usage["d2h"] = dict(active_by_direction.get("d2h", {}))
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

    summary_copy.update(
        {
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
            "active_reservation_count": len(runtime_state.get("active_reservations", ()) or ()),
            "active_lease_count": len(runtime_state.get("active_leases", ()) or ()),
            "relay_staging_count": len(runtime_state.get("relay_staging", ()) or ()),
            "relay_path_count": relay_path_summary["path_count"],
            "relay_path_bytes_total": relay_path_summary["bytes_total"],
            "busy_relays": tuple(sorted(busy_relays_from_runtime_state(runtime_state))),
            "relay_load": relay_load_from_runtime_state(runtime_state),
            "queued_bytes_by_direction": queued_by_direction,
            "active_bytes_by_direction": active_by_direction,
            "active_paths": path_summary,
            "active_resource_usage": active_resource_usage,
            "completion_source_counts": completion_source_counts,
            "terminal_completion_source_counts": terminal_completion_source_counts,
            "active_execution_evidence": active_execution_evidence,
            "active_execution_evidence_by_source": active_execution_evidence_by_source,
            "terminal_execution_evidence": terminal_execution_evidence,
            "terminal_execution_evidence_by_source": terminal_execution_evidence_by_source,
            "runtime_feedback_metrics": runtime_feedback_metrics,
        }
    )
    runtime_state["active_resource_usage"] = active_resource_usage
    runtime_state["summary"] = summary_copy


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
    result = empty_execution_path_evidence()
    for record in runtime_mapping_records(records):
        if str(record.get("state")) not in {
            TransferStatusState.COMPLETE.value,
            TransferStatusState.FAILED.value,
            TransferStatusState.CANCELED.value,
        }:
            continue
        evidence = record.get("completion_evidence")
        if not isinstance(evidence, Mapping):
            continue
        path_evidence = evidence.get("execution_path_evidence")
        if not isinstance(path_evidence, Mapping):
            continue
        result["direct_bytes"] += int(path_evidence.get("direct_bytes", 0) or 0)
        result["direct_chunks"] += int(path_evidence.get("direct_chunks", 0) or 0)
        result["relay_bytes"] += int(path_evidence.get("relay_bytes", 0) or 0)
        result["relay_chunks"] += int(path_evidence.get("relay_chunks", 0) or 0)
    return result


def terminal_execution_evidence_by_source_from_records(
    records: object,
) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for record in runtime_mapping_records(records):
        if str(record.get("state")) not in {
            TransferStatusState.COMPLETE.value,
            TransferStatusState.FAILED.value,
            TransferStatusState.CANCELED.value,
        }:
            continue
        completion_source = str(record.get("completion_source", "")).lower()
        if not completion_source:
            continue
        evidence = record.get("completion_evidence")
        if not isinstance(evidence, Mapping):
            continue
        path_evidence = evidence.get("execution_path_evidence")
        if not isinstance(path_evidence, Mapping):
            continue
        bucket = result.setdefault(completion_source, empty_execution_path_evidence())
        bucket["direct_bytes"] += int(path_evidence.get("direct_bytes", 0) or 0)
        bucket["direct_chunks"] += int(path_evidence.get("direct_chunks", 0) or 0)
        bucket["relay_bytes"] += int(path_evidence.get("relay_bytes", 0) or 0)
        bucket["relay_chunks"] += int(path_evidence.get("relay_chunks", 0) or 0)
    return result


def runtime_feedback_metrics_from_records(
    records: object,
) -> dict[str, object]:
    metrics = {
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
        },
        "worker_executor_runtime": {
            "samples": 0,
            "runtime_reused": 0,
            "runtime_created": 0,
            "max_runtime_cache_size": 0,
            "max_inflight_count": 0,
            "max_terminal_count": 0,
            "max_submit_to_complete_ms": 0.0,
            "relay_gpu_count": 0,
            "target_devices": (),
        },
        "cuda_ipc_span_validation": {
            "validated": 0,
            "failed": 0,
            "missing": 0,
        },
        "recent_terminal_count": 0,
    }
    for record in runtime_mapping_records(records):
        completion_source = str(record.get("completion_source", "")).lower()
        if completion_source == "worker":
            metrics["worker_completion_count"] = int(metrics["worker_completion_count"]) + 1
        elif completion_source == "backend":
            metrics["backend_completion_count"] = int(metrics["backend_completion_count"]) + 1
        if str(record.get("state")) in {
            TransferStatusState.COMPLETE.value,
            TransferStatusState.FAILED.value,
            TransferStatusState.CANCELED.value,
        }:
            metrics["recent_terminal_count"] = int(metrics["recent_terminal_count"]) + 1
        evidence = record.get("completion_evidence")
        if not isinstance(evidence, Mapping):
            continue
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
            metrics["worker_async_pool"] = pool_metrics
        worker_runtime_feedback = evidence.get("worker_runtime_feedback")
        if isinstance(worker_runtime_feedback, Mapping):
            metrics["worker_executor_runtime"] = merge_worker_runtime_feedback_metrics(
                metrics["worker_executor_runtime"],
                worker_runtime_feedback,
            )
        span_state = cuda_ipc_span_validation_state(evidence)
        if span_state is not None:
            span_metrics = dict(metrics["cuda_ipc_span_validation"])
            span_metrics[span_state] = int(span_metrics.get(span_state, 0)) + 1
            metrics["cuda_ipc_span_validation"] = span_metrics
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
    metrics["max_runtime_cache_size"] = max(
        int(metrics.get("max_runtime_cache_size", 0) or 0),
        int(feedback.get("runtime_cache_size", 0) or 0),
    )
    metrics["max_inflight_count"] = max(
        int(metrics.get("max_inflight_count", 0) or 0),
        int(feedback.get("inflight_count", 0) or 0),
    )
    metrics["max_terminal_count"] = max(
        int(metrics.get("max_terminal_count", 0) or 0),
        int(feedback.get("terminal_count", 0) or 0),
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


def runtime_mapping_records(value: object) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list | tuple):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


__all__ = [
    "accumulate_execution_path_evidence",
    "daemon_runtime_telemetry_snapshot",
    "empty_execution_path_evidence",
    "refresh_runtime_feedback_summary",
    "runtime_feedback_metrics_from_records",
    "runtime_mapping_records",
    "terminal_execution_evidence_by_source_from_records",
    "terminal_execution_evidence_from_records",
    "terminal_feedback_record_from_record",
]
