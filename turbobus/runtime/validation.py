from __future__ import annotations

from collections.abc import Mapping

from ..schema import (
    TransferIntent,
    TransferReceipt,
    TransferStatusState,
    require_complete_receipt_metadata_evidence,
)


def validate_intent_ranges_fit_buffers(
    intent: TransferIntent,
    *,
    source_bytes: int,
    target_bytes: int,
) -> None:
    total_bytes = 0
    for item in intent.ranges:
        source_offset = int(item["src_offset"])
        target_offset = int(item["dst_offset"])
        bytes_count = int(item["bytes"])
        if source_offset < 0 or target_offset < 0:
            raise ValueError("intent range offsets must be non-negative")
        if bytes_count <= 0:
            raise ValueError("intent range bytes must be positive")
        if source_offset + bytes_count > int(source_bytes):
            raise ValueError("intent range exceeds runtime source buffer")
        if target_offset + bytes_count > int(target_bytes):
            raise ValueError("intent range exceeds runtime destination buffer")
        total_bytes += bytes_count
    if total_bytes != int(intent.total_bytes):
        raise ValueError("intent total_bytes must match runtime buffer ranges")


def validate_runtime_receipt(
    receipt: TransferReceipt,
    *,
    intent_id: str,
    job_id: str,
    session_id: str,
    source_buffer_id: str | None = None,
    destination_buffer_id: str | None = None,
) -> None:
    if not isinstance(receipt, TransferReceipt):
        raise TypeError("runtime transfer must return a TransferReceipt")
    if receipt.intent_id != str(intent_id):
        raise ValueError("runtime receipt intent_id does not match submitted intent")
    if receipt.job_id != str(job_id):
        raise ValueError("runtime receipt job_id does not match runtime session")
    if receipt.session_id != str(session_id):
        raise ValueError("runtime receipt session_id does not match runtime session")
    validated_real_execution_evidence(receipt)
    metadata = receipt.metadata if isinstance(receipt.metadata, Mapping) else {}
    require_runtime_buffer_lifetime_evidence(
        metadata,
        session_id=session_id,
        source_buffer_id=source_buffer_id,
        destination_buffer_id=destination_buffer_id,
    )
    require_worker_startup_evidence(metadata)
    require_worker_async_pool_evidence(metadata)


def validated_real_execution_evidence(receipt: TransferReceipt) -> dict[str, object]:
    if not isinstance(receipt, TransferReceipt):
        raise TypeError("real-execution validation requires a TransferReceipt")
    state = TransferStatusState(receipt.state)
    if state not in {
        TransferStatusState.COMPLETE,
        TransferStatusState.FAILED,
        TransferStatusState.CANCELED,
    }:
        raise ValueError("real-execution validation requires a terminal receipt")
    metadata = receipt.metadata if isinstance(receipt.metadata, Mapping) else {}
    require_receipt_ticket_binding(receipt, metadata)
    require_complete_receipt_evidence(receipt)
    require_failed_receipt_evidence(receipt)
    reproduction = require_reproduction_evidence(metadata, receipt=receipt)
    _require_real_execution_view(receipt, reproduction)
    execution = reproduction["execution"]
    transfer = reproduction["transfer"]
    return {
        "schema": "turbobus.real_execution_validation.v1",
        "source": "TransferReceipt",
        "fake_receipt": False,
        "synthetic_evidence": False,
        "dry_run": False,
        "receipt_id": receipt.receipt_id,
        "ticket_id": receipt.ticket_id,
        "intent_id": receipt.intent_id,
        "decision_id": receipt.decision_id,
        "topology_snapshot_id": receipt.topology_snapshot_id,
        "job_id": receipt.job_id,
        "session_id": receipt.session_id,
        "state": TransferStatusState(receipt.state).value,
        "bytes_total": int(receipt.bytes_total),
        "bytes_completed": int(receipt.bytes_completed),
        "transfer_id": transfer.get("transfer_id"),
        "completion_source": execution.get("completion_source"),
        "execution_mode": execution.get("mode"),
        "verified": bool(execution.get("verified", False)),
        "verified_bytes": int(execution.get("verified_bytes", 0) or 0),
        "content_match": bool(execution.get("content_match", False)),
        "route_policy_source": "daemon_scheduler",
        "reproduction_evidence": dict(reproduction),
    }


def require_receipt_ticket_binding(
    receipt: TransferReceipt,
    metadata: Mapping[str, object],
) -> None:
    for key in ("execution_ticket_id", "evidence_ticket_id"):
        ticket_id = metadata.get(key)
        if ticket_id is not None and str(ticket_id) != receipt.ticket_id:
            raise ValueError(f"runtime receipt {key} does not match receipt ticket_id")
    metadata_transfer_id = metadata.get("transfer_id")
    evidence_transfer_id = metadata.get("evidence_transfer_id")
    if (
        metadata_transfer_id is not None
        and evidence_transfer_id is not None
        and str(evidence_transfer_id) != str(metadata_transfer_id)
    ):
        raise ValueError("runtime receipt evidence_transfer_id does not match transfer_id")
    plan_generation = metadata.get("plan_generation")
    evidence_generation = metadata.get("evidence_plan_generation")
    if (
        plan_generation is not None
        and evidence_generation is not None
        and int(evidence_generation) != int(plan_generation)
    ):
        raise ValueError(
            "runtime receipt evidence_plan_generation does not match plan_generation"
        )


def require_complete_receipt_evidence(receipt: TransferReceipt) -> None:
    if TransferStatusState(receipt.state) is not TransferStatusState.COMPLETE:
        return
    metadata = receipt.metadata if isinstance(receipt.metadata, Mapping) else {}
    require_complete_receipt_metadata_evidence(metadata, int(receipt.bytes_total))


def require_failed_receipt_evidence(receipt: TransferReceipt) -> None:
    state = TransferStatusState(receipt.state)
    if state not in {TransferStatusState.FAILED, TransferStatusState.CANCELED}:
        return
    metadata = receipt.metadata if isinstance(receipt.metadata, Mapping) else {}
    completion_source = str(metadata.get("completion_source", "")).lower()
    if completion_source not in {"worker", "backend"}:
        raise ValueError(
            "failed or canceled receipt missing worker/backend execution source"
        )
    if not bool(metadata.get("executed", False)):
        raise ValueError("failed or canceled receipt missing execution evidence")
    if receipt.error is None or not str(receipt.error).strip():
        raise ValueError("failed or canceled receipt missing error")
    if metadata.get("evidence_ticket_id") is None:
        raise ValueError("failed or canceled receipt missing daemon ticket evidence")
    if metadata.get("evidence_transfer_id") is None:
        raise ValueError("failed or canceled receipt missing transfer evidence")
    if metadata.get("evidence_plan_generation") is None:
        raise ValueError("failed or canceled receipt missing plan generation evidence")


def require_runtime_buffer_lifetime_evidence(
    metadata: Mapping[str, object],
    *,
    session_id: str,
    source_buffer_id: str | None,
    destination_buffer_id: str | None,
) -> None:
    lifetime = metadata.get("buffer_lifetime_evidence")
    if not isinstance(lifetime, Mapping):
        raise ValueError("runtime receipt missing buffer_lifetime_evidence")
    _require_runtime_buffer_record(
        lifetime.get("source_buffer"),
        expected_session_id=session_id,
        expected_buffer_id=source_buffer_id,
        label="source",
    )
    _require_runtime_buffer_record(
        lifetime.get("destination_buffer"),
        expected_session_id=session_id,
        expected_buffer_id=destination_buffer_id,
        label="destination",
    )


def require_worker_startup_evidence(metadata: Mapping[str, object]) -> None:
    completion_source = str(metadata.get("completion_source", "")).lower()
    if completion_source != "worker":
        return
    worker_startup = metadata.get("worker_startup")
    if not isinstance(worker_startup, Mapping):
        raise ValueError("runtime receipt missing worker_startup evidence")
    startup_source = worker_startup.get("startup_source")
    if startup_source is None or not str(startup_source).strip():
        raise ValueError("runtime receipt worker_startup missing startup_source")
    if worker_startup.get("topology_snapshot_id") is None:
        raise ValueError("runtime receipt worker_startup missing topology_snapshot_id")
    if worker_startup.get("require_authenticated_peers") is None:
        raise ValueError(
            "runtime receipt worker_startup missing authenticated peer requirement"
        )
    if bool(worker_startup.get("require_authenticated_peers", False)) and not bool(
        worker_startup.get("daemon_peer_authenticated", False)
    ):
        raise ValueError(
            "runtime receipt worker_startup missing authenticated daemon peer evidence"
        )


def require_worker_async_pool_evidence(metadata: Mapping[str, object]) -> None:
    completion_source = str(metadata.get("completion_source", "")).lower()
    if completion_source != "worker":
        return
    worker_async_pool = metadata.get("worker_async_pool")
    if not isinstance(worker_async_pool, Mapping):
        raise ValueError("runtime receipt missing worker_async_pool evidence")
    if str(worker_async_pool.get("pool", "")) != "worker_async_execution_pool":
        raise ValueError("runtime receipt worker_async_pool has the wrong pool source")
    if worker_async_pool.get("pool_ticket") is None:
        raise ValueError("runtime receipt worker_async_pool missing pool_ticket")
    if worker_async_pool.get("state") is None:
        raise ValueError("runtime receipt worker_async_pool missing state")


def require_reproduction_evidence(
    metadata: Mapping[str, object],
    *,
    receipt: TransferReceipt | None = None,
) -> dict[str, object]:
    reproduction = metadata.get("reproduction_evidence")
    if not isinstance(reproduction, Mapping):
        raise ValueError("runtime receipt missing reproduction_evidence")
    if reproduction.get("schema") != "turbobus.reproduction_evidence.v1":
        raise ValueError("runtime receipt reproduction_evidence has wrong schema")
    if reproduction.get("source") != "TransferReceipt":
        raise ValueError("runtime receipt reproduction_evidence must come from TransferReceipt")
    for forbidden in ("fake_receipt", "synthetic_evidence", "dry_run"):
        if bool(reproduction.get(forbidden, False)):
            raise ValueError(f"runtime receipt reproduction_evidence uses {forbidden}")
    transfer = reproduction.get("transfer")
    if not isinstance(transfer, Mapping):
        raise ValueError("runtime receipt reproduction_evidence missing transfer view")
    execution = reproduction.get("execution")
    if not isinstance(execution, Mapping):
        raise ValueError("runtime receipt reproduction_evidence missing execution view")
    if reproduction.get("route_policy_source") != "daemon_scheduler":
        raise ValueError("runtime receipt reproduction_evidence route policy is not daemon-owned")
    if not isinstance(reproduction.get("completion_contract"), Mapping):
        raise ValueError("runtime receipt reproduction_evidence missing completion contract")
    if not isinstance(reproduction.get("buffer_lifetime"), Mapping):
        raise ValueError("runtime receipt reproduction_evidence missing buffer lifetime")
    metadata_transfer_id = metadata.get("transfer_id")
    if metadata_transfer_id is not None:
        transfer_id = transfer.get("transfer_id")
        if transfer_id is None or str(transfer_id) != str(metadata_transfer_id):
            raise ValueError(
                "runtime receipt reproduction_evidence transfer_id does not "
                "match metadata"
            )
    if receipt is not None:
        _require_reproduction_receipt_binding(receipt, reproduction)
    return dict(reproduction)


def _require_reproduction_receipt_binding(
    receipt: TransferReceipt,
    reproduction: Mapping[str, object],
) -> None:
    transfer = reproduction.get("transfer")
    if not isinstance(transfer, Mapping):
        raise ValueError("runtime receipt reproduction_evidence missing transfer view")
    expected = {
        "intent_id": receipt.intent_id,
        "decision_id": receipt.decision_id,
        "topology_snapshot_id": receipt.topology_snapshot_id,
        "ticket_id": receipt.ticket_id,
        "job_id": receipt.job_id,
        "session_id": receipt.session_id,
        "state": TransferStatusState(receipt.state).value,
    }
    for field_name, expected_value in expected.items():
        observed = transfer.get(field_name)
        if observed is None or str(observed) != str(expected_value):
            raise ValueError(
                "runtime receipt reproduction_evidence "
                f"{field_name} does not match receipt"
            )
    for field_name in ("bytes_total", "bytes_completed"):
        observed = transfer.get(field_name)
        expected_bytes = getattr(receipt, field_name)
        if observed is None or int(observed) != int(expected_bytes):
            raise ValueError(
                "runtime receipt reproduction_evidence "
                f"{field_name} does not match receipt"
            )


def _require_real_execution_view(
    receipt: TransferReceipt,
    reproduction: Mapping[str, object],
) -> None:
    execution = reproduction.get("execution")
    if not isinstance(execution, Mapping):
        raise ValueError("real-execution evidence missing execution view")
    completion_source = str(execution.get("completion_source", "")).lower()
    if completion_source not in {"worker", "backend"}:
        raise ValueError("real-execution evidence missing worker/backend source")
    if not bool(execution.get("executed", False)):
        raise ValueError("real-execution evidence was not executed")
    mode = str(execution.get("mode", "")).lower()
    if mode not in {"direct_only", "relay_only", "mixed_pooled"}:
        raise ValueError("real-execution evidence has no validated path mode")
    if not isinstance(execution.get("path"), Mapping):
        raise ValueError("real-execution evidence missing path view")
    completion_contract = reproduction.get("completion_contract")
    if not isinstance(completion_contract, Mapping):
        raise ValueError("real-execution evidence missing completion contract")
    for field_name in ("ticket_id", "transfer_id", "plan_generation"):
        if completion_contract.get(field_name) is None:
            raise ValueError(
                "real-execution evidence completion contract missing "
                f"{field_name}"
            )
    if str(completion_contract.get("ticket_id")) != str(receipt.ticket_id):
        raise ValueError("real-execution evidence ticket_id does not match receipt")
    transfer = reproduction.get("transfer")
    if not isinstance(transfer, Mapping):
        raise ValueError("real-execution evidence missing transfer view")
    if str(completion_contract.get("transfer_id")) != str(transfer.get("transfer_id")):
        raise ValueError(
            "real-execution evidence transfer_id does not match reproduction view"
        )
    state = TransferStatusState(receipt.state)
    if state is TransferStatusState.COMPLETE:
        if not bool(execution.get("verified", False)):
            raise ValueError("real-execution evidence missing verification")
        if int(execution.get("verified_bytes", 0) or 0) != int(receipt.bytes_total):
            raise ValueError("real-execution evidence verified bytes mismatch")
        if not bool(execution.get("content_match", False)):
            raise ValueError("real-execution evidence missing content match")
    cleanup = reproduction.get("cleanup")
    failure_cleanup = reproduction.get("failure_cleanup_contract")
    if state in {TransferStatusState.FAILED, TransferStatusState.CANCELED}:
        if cleanup is None and failure_cleanup is None:
            raise ValueError("failed real-execution evidence missing cleanup contract")


def _require_runtime_buffer_record(
    record: object,
    *,
    expected_session_id: str,
    expected_buffer_id: str | None,
    label: str,
) -> None:
    if not isinstance(record, Mapping):
        raise ValueError(f"runtime receipt missing {label} buffer lifetime evidence")
    if expected_buffer_id is not None and str(record.get("buffer_id")) != str(expected_buffer_id):
        raise ValueError(f"runtime receipt {label} buffer_id does not match submitted intent")
    registration = record.get("registration")
    if not isinstance(registration, Mapping):
        raise ValueError(f"runtime receipt missing {label} buffer registration evidence")
    metadata = registration.get("metadata")
    if not isinstance(metadata, Mapping):
        raise ValueError(f"runtime receipt missing {label} buffer registration metadata")
    runtime_session_id = metadata.get("runtime_session_id")
    if runtime_session_id is None or str(runtime_session_id) != str(expected_session_id):
        raise ValueError(
            f"runtime receipt {label} buffer runtime_session_id does not match session"
        )
    resource_evidence = record.get("resource_evidence")
    if not isinstance(resource_evidence, Mapping):
        raise ValueError(f"runtime receipt missing {label} buffer resource evidence")
    _require_runtime_buffer_resource_evidence(
        resource_evidence,
        expected_buffer_id=expected_buffer_id,
        runtime_buffer_kind=metadata.get("runtime_buffer_kind"),
        label=label,
    )
    if bool(record.get("runtime_owned", False)) and str(
        metadata.get("runtime_buffer_kind", "")
    ).lower() == "shared_pinned_cpu":
        owned_cpu_buffer_release = record.get("owned_cpu_buffer_release")
        if not isinstance(owned_cpu_buffer_release, Mapping):
            raise ValueError(
                f"runtime receipt missing {label} runtime-owned CPU buffer release evidence"
            )
        if not bool(owned_cpu_buffer_release.get("ok", False)):
            raise ValueError(
                f"runtime receipt {label} runtime-owned CPU buffer release did not complete"
            )


def _require_runtime_buffer_resource_evidence(
    resource_evidence: Mapping[str, object],
    *,
    expected_buffer_id: str | None,
    runtime_buffer_kind: object,
    label: str,
) -> None:
    if expected_buffer_id is not None:
        matched = any(
            str(resource_evidence.get(field_name, "")) == str(expected_buffer_id)
            for field_name in (
                "src_buffer_id",
                "dst_buffer_id",
                "cpu_buffer_id",
                "device_buffer_id",
            )
        )
        if not matched:
            raise ValueError(
                f"runtime receipt {label} buffer resource evidence does not match buffer_id"
            )
    normalized_kind = str(runtime_buffer_kind or "").lower()
    if normalized_kind == "shared_pinned_cpu":
        if str(resource_evidence.get("cpu_handle_type", "")).lower() != "shared_pinned_cpu":
            raise ValueError(
                f"runtime receipt {label} CPU buffer resource evidence has the wrong handle type"
            )
        if not any(
            key in resource_evidence
            for key in ("cpu_buffer_opened", "cuda_host_registered", "close_evidence")
        ):
            raise ValueError(
                f"runtime receipt {label} CPU buffer resource evidence is missing lifecycle markers"
            )
        return
    if normalized_kind == "cuda_ipc_device":
        if str(resource_evidence.get("device_handle_type", "")).lower() != "cuda_ipc_device":
            raise ValueError(
                f"runtime receipt {label} CUDA buffer resource evidence has the wrong handle type"
            )
        if not any(
            key in resource_evidence
            for key in (
                "cuda_ipc_span_validation",
                "device_ipc_opened",
                "device_ptr",
                "device_index",
                "close_evidence",
            )
        ):
            raise ValueError(
                f"runtime receipt {label} CUDA buffer resource evidence is missing lifecycle markers"
            )
        span_validation = resource_evidence.get("cuda_ipc_span_validation")
        if isinstance(span_validation, Mapping) and not bool(
            span_validation.get("validated", False)
        ):
            raise ValueError(
                f"runtime receipt {label} CUDA buffer span validation did not complete"
            )


__all__ = [
    "require_complete_receipt_evidence",
    "require_failed_receipt_evidence",
    "require_reproduction_evidence",
    "require_receipt_ticket_binding",
    "require_runtime_buffer_lifetime_evidence",
    "require_worker_async_pool_evidence",
    "require_worker_startup_evidence",
    "validated_real_execution_evidence",
    "validate_intent_ranges_fit_buffers",
    "validate_runtime_receipt",
]
