from __future__ import annotations

from collections.abc import Mapping

from ..schema import DaemonResponse
from . import validation as worker_validation
from .models import (
    WorkerTransferRequest,
    WorkerTransferResult,
    WorkerTransferState,
)
from .staging_pool import WorkerStagingSlot


_COMPLETION_BINDING_METADATA_KEYS = (
    "ticket_id",
    "transfer_id",
    "plan_generation",
    "owner_binding",
    "block_progress",
    "block_runtime",
    "worker_runtime_feedback",
    "async_data_plane",
)


_EXECUTION_CONTRACT_METADATA_KEYS = (
    "executor",
    "path",
    "plan_source",
    "target_device",
    "verified_bytes",
    "content_match",
    "verification_source",
    "verification_method",
    "source_digest",
    "destination_digest",
    "direct_bytes",
    "direct_chunks",
    "relay_bytes",
    "relay_chunks",
    "relay_gpu",
    "relay_gpus",
    "src_buffer_id",
    "dst_buffer_id",
    "staging_slot_id",
    "resource_evidence",
    "cuda_ipc_lifecycle",
    "worker_startup",
    "worker_async_pool",
    "worker_runtime_feedback",
    "async_data_plane",
    "path_level_evidence",
    "native_path_stats",
    "relay_device_stats",
    "ticket_id",
    "transfer_id",
    "plan_generation",
    "owner_binding",
    "block_progress",
    "block_runtime",
    "failure_source",
)


def status_evidence_for_result(
    result: WorkerTransferResult,
) -> dict[str, object] | None:
    metadata = dict(result.metadata)
    if result.state is not WorkerTransferState.COMPLETE:
        evidence = execution_contract_evidence_from_metadata(metadata)
        return evidence or None
    evidence = metadata.get("completion_evidence")
    if isinstance(evidence, Mapping):
        completion_evidence = dict(evidence)
        for key in _COMPLETION_BINDING_METADATA_KEYS:
            if key in metadata:
                completion_evidence.setdefault(key, metadata[key])
        return completion_evidence
    if not any(key in metadata for key in _EXECUTION_CONTRACT_METADATA_KEYS):
        return None
    contract = {
        key: metadata[key]
        for key in _EXECUTION_CONTRACT_METADATA_KEYS
        if key in metadata
    }
    return contract


def worker_pool_record(
    *,
    pool_ticket: str,
    state: str,
    worker_request: WorkerTransferRequest,
    staging_slot: WorkerStagingSlot,
    queued_at: float,
    started_at: float | None = None,
    completed_at: float | None = None,
) -> dict[str, object]:
    return {
        "pool": "worker_async_execution_pool",
        "pool_ticket": str(pool_ticket),
        "state": str(state),
        "transfer_id": worker_request.transfer_id,
        "ticket_id": worker_request.ticket.ticket_id,
        "plan_generation": int(worker_request.ticket.metadata["plan_generation"]),
        "session_id": worker_request.authorization.session_id,
        "job_id": worker_request.authorization.job_id,
        "lease_id": worker_request.authorization.lease_id,
        "relay_gpus": worker_validation.authorized_relay_gpus_for_request(
            worker_request
        ),
        "staging_slot_id": staging_slot.slot_id,
        "queued_at": float(queued_at),
        "started_at": None if started_at is None else float(started_at),
        "completed_at": None if completed_at is None else float(completed_at),
    }


def cleanup_completion_evidence(
    request: WorkerTransferRequest,
    result: WorkerTransferResult,
    cleanup_response: DaemonResponse,
) -> dict[str, object]:
    metadata = dict(result.metadata)
    evidence = execution_contract_evidence_from_metadata(metadata)
    evidence.setdefault("ticket_id", request.ticket.ticket_id)
    transfer_id = request.ticket.metadata.get("transfer_id")
    if transfer_id is not None:
        evidence.setdefault("transfer_id", str(transfer_id))
    plan_generation = request.ticket.metadata.get("plan_generation")
    if plan_generation is not None:
        evidence.setdefault("plan_generation", int(plan_generation))
    owner_binding = request.data_plane.metadata.get("owner_binding")
    if not isinstance(owner_binding, Mapping):
        owner_binding = request.ticket.metadata.get("owner_binding")
    normalized_owner_binding = (
        dict(owner_binding) if isinstance(owner_binding, Mapping) else None
    )
    payload = (
        cleanup_response.payload
        if isinstance(cleanup_response.payload, Mapping)
        else {}
    )
    cleanup_payload = dict(payload)
    cleanup_scope_target_ids = tuple(
        str(item)
        for item in cleanup_payload.get("cleanup_scope_target_ids", ()) or ()
    )
    lease_ids = tuple(
        str(item) for item in cleanup_payload.get("lease_ids", ()) or ()
    )
    if not cleanup_scope_target_ids:
        cleanup_scope_target_ids = lease_ids
    if not lease_ids:
        lease_ids = cleanup_scope_target_ids
    evidence["cleanup"] = {
        "ok": bool(cleanup_response.ok),
        "target_kind": cleanup_payload.get("cleanup_kind"),
        "target_id": cleanup_payload.get("reservation_id"),
        "mode": cleanup_payload.get("cleanup_mode"),
        "reason": cleanup_payload.get("reason"),
        "lease_ids": lease_ids,
        "cleanup_scope_target_ids": cleanup_scope_target_ids,
        "cleaned_reservation_ids": tuple(
            str(item)
            for item in cleanup_payload.get("cleaned_reservation_ids", ()) or ()
        ),
    }
    if normalized_owner_binding is not None:
        evidence["cleanup"]["owner_binding"] = normalized_owner_binding
    return evidence


def worker_block_progress_evidence(
    request: WorkerTransferRequest,
    result: WorkerTransferResult,
) -> dict[str, object] | None:
    block_runtime = request.ticket.metadata.get("block_runtime")
    if not isinstance(block_runtime, Mapping):
        return None
    records = tuple(
        dict(record)
        for record in block_runtime.get("records", ()) or ()
        if isinstance(record, Mapping)
    )
    if not records:
        return None
    progress_state = (
        "completed"
        if result.state is WorkerTransferState.COMPLETE
        else "failed"
    )
    completed_bytes = int(result.bytes_completed)
    remaining_bytes = max(0, completed_bytes)
    progress_records: list[dict[str, object]] = []
    for record in records:
        byte_count = int(record.get("bytes", 0) or 0)
        if result.state is WorkerTransferState.COMPLETE:
            record_state = "completed"
            record_completed_bytes = byte_count
        elif remaining_bytes >= byte_count:
            record_state = "completed"
            record_completed_bytes = byte_count
            remaining_bytes -= byte_count
        else:
            record_state = progress_state
            record_completed_bytes = 0
        progress_records.append(
            {
                "block_id": str(record.get("block_id")),
                "path_id": str(record.get("path_id")),
                "path_kind": str(record.get("path_kind", "unknown")),
                "state": record_state,
                "bytes": byte_count,
                "bytes_completed": record_completed_bytes,
                "attempt": int(record.get("attempt", 0) or 0),
                "ticket_id": request.ticket.ticket_id,
                "transfer_id": request.transfer_id,
                "plan_generation": int(
                    request.ticket.metadata.get("plan_generation", 0) or 0
                ),
            }
        )
    return {
        "source": "worker_daemon_block_runtime_progress",
        "transfer_id": request.transfer_id,
        "ticket_id": request.ticket.ticket_id,
        "plan_generation": int(request.ticket.metadata.get("plan_generation", 0) or 0),
        "state": result.state.value,
        "bytes_completed": int(result.bytes_completed),
        "block_count": len(progress_records),
        "records": tuple(progress_records),
    }


def execution_contract_evidence_from_metadata(
    metadata: Mapping[str, object],
) -> dict[str, object]:
    evidence = (
        dict(metadata.get("completion_evidence"))
        if isinstance(metadata.get("completion_evidence"), Mapping)
        else {}
    )
    for key in _EXECUTION_CONTRACT_METADATA_KEYS:
        if key in metadata:
            evidence.setdefault(key, metadata[key])
    cleanup = metadata.get("cleanup")
    if isinstance(cleanup, Mapping):
        evidence.setdefault("cleanup", dict(cleanup))
    resource_evidence = metadata.get("resource_evidence")
    if isinstance(resource_evidence, Mapping):
        cuda_ipc_lifecycle = resource_evidence.get("cuda_ipc_lifecycle")
        if isinstance(cuda_ipc_lifecycle, Mapping):
            evidence.setdefault("cuda_ipc_lifecycle", dict(cuda_ipc_lifecycle))
    return evidence


__all__ = [
    "cleanup_completion_evidence",
    "execution_contract_evidence_from_metadata",
    "status_evidence_for_result",
    "worker_block_progress_evidence",
    "worker_pool_record",
]
