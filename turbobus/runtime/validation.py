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
    metadata = receipt.metadata if isinstance(receipt.metadata, Mapping) else {}
    require_receipt_ticket_binding(receipt, metadata)
    require_complete_receipt_evidence(receipt)
    require_failed_receipt_evidence(receipt)
    require_runtime_buffer_lifetime_evidence(
        metadata,
        session_id=session_id,
        source_buffer_id=source_buffer_id,
        destination_buffer_id=destination_buffer_id,
    )
    require_worker_startup_evidence(metadata)


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
            for key in ("device_ipc_opened", "device_ptr", "device_index", "close_evidence")
        ):
            raise ValueError(
                f"runtime receipt {label} CUDA buffer resource evidence is missing lifecycle markers"
            )


__all__ = [
    "require_complete_receipt_evidence",
    "require_failed_receipt_evidence",
    "require_receipt_ticket_binding",
    "require_runtime_buffer_lifetime_evidence",
    "require_worker_startup_evidence",
    "validate_intent_ranges_fit_buffers",
    "validate_runtime_receipt",
]
