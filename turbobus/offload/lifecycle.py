from __future__ import annotations

from typing import Any, Iterable, Mapping

from ..schema import TransferReceipt


def unique_receipts_from_handles(handles: Iterable[object]) -> list[TransferReceipt]:
    receipts: list[TransferReceipt] = []
    seen = set()
    for handle in handles:
        receipt = getattr(handle, "receipt", None)
        if not isinstance(receipt, TransferReceipt):
            continue
        if receipt.receipt_id in seen:
            continue
        seen.add(receipt.receipt_id)
        receipts.append(receipt)
    return receipts


def receipt_trace_from_receipts(receipts: Iterable[TransferReceipt]) -> dict[str, Any]:
    direct_bytes = 0
    relay_bytes = 0
    receipt_ids: list[str] = []
    intent_ids: list[str] = []
    decision_ids: list[str] = []
    topology_snapshot_ids: list[str] = []
    ticket_ids: list[str] = []
    receipt_states: list[str] = []
    completion_sources: list[str] = []
    transfer_ids: list[str] = []
    fallback_reasons: list[str] = []
    receipt_list = list(receipts)
    receipt_contracts: list[dict[str, Any]] = []
    runtime_buffer_bindings: list[dict[str, Any]] = []
    for receipt in receipt_list:
        receipt_ids.append(receipt.receipt_id)
        intent_ids.append(receipt.intent_id)
        decision_ids.append(receipt.decision_id)
        topology_snapshot_ids.append(receipt.topology_snapshot_id)
        ticket_ids.append(receipt.ticket_id)
        receipt_states.append(str(receipt.state.value))
        metadata = receipt.metadata if isinstance(receipt.metadata, Mapping) else {}
        receipt_contracts.append(_receipt_contract_summary(receipt, metadata))
        runtime_buffer_bindings.extend(_runtime_buffer_bindings(receipt, metadata))
        completion_source = metadata.get("completion_source")
        if completion_source:
            completion_sources.append(str(completion_source))
        transfer_id = metadata.get("transfer_id")
        if transfer_id:
            transfer_ids.append(str(transfer_id))
        fallback_reason = metadata.get("fallback_reason")
        if fallback_reason:
            fallback_reasons.append(str(fallback_reason))
        for path in receipt.path_stats:
            path_bytes = int(path.get("bytes", 0) or 0)
            if str(path.get("kind", "")).lower() == "relay":
                relay_bytes += path_bytes
            else:
                direct_bytes += path_bytes
    return {
        "direct_bytes": direct_bytes,
        "relay_bytes": relay_bytes,
        "receipt_count": len(receipt_list),
        "intent_ids": join_unique(intent_ids),
        "receipt_ids": join_unique(receipt_ids),
        "decision_ids": join_unique(decision_ids),
        "topology_snapshot_ids": join_unique(topology_snapshot_ids),
        "ticket_ids": join_unique(ticket_ids),
        "receipt_states": join_unique(receipt_states),
        "completion_sources": join_unique(completion_sources),
        "transfer_ids": join_unique(transfer_ids),
        "fallback_reason": join_unique(fallback_reasons),
        "receipt_contracts": receipt_contracts,
        "runtime_buffer_bindings": runtime_buffer_bindings,
    }


def adapter_lifecycle_evidence_from_handles(
    *,
    evidence_id: str,
    operation: str,
    transfer_context,
    item_field: str,
    item_count_field: str,
    item_names: Iterable[str],
    handles: Iterable[object],
    transfer_stats: Mapping[str, Any],
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    names = tuple(str(name) for name in item_names)
    handle_list = list(handles)
    receipts = unique_receipts_from_handles(handle_list)
    if names and not receipts:
        raise RuntimeError(
            f"{operation} completed without TransferReceipt evidence"
        )
    trace = receipt_trace_from_receipts(receipts)
    return {
        "evidence_id": str(evidence_id),
        "operation": str(operation),
        "job_id": transfer_context.job_id,
        "session_id": transfer_context.session_id,
        "workload_kind": str(transfer_context.workload_kind.value),
        "buffer_registration_source": "TurboBusRuntimeSession",
        "intent_source": "TransferIntent",
        "receipt_source": "TransferReceipt",
        "policy_source": "daemon_scheduler",
        "cpu_buffer_id": transfer_context.cpu_buffer_id,
        "gpu_buffer_id": transfer_context.gpu_buffer_id,
        item_field: names,
        item_count_field: len(names),
        **trace,
        **dict(transfer_stats),
        **({} if extra is None else dict(extra)),
    }


def join_unique(values: Iterable[object]) -> str:
    seen = set()
    ordered = []
    for value in values:
        if value is None:
            continue
        text = str(value)
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ",".join(ordered)


def _receipt_contract_summary(
    receipt: TransferReceipt,
    metadata: Mapping[str, Any],
) -> dict[str, Any]:
    completion_contract = metadata.get("completion_contract")
    cuda_ipc_lifecycle = metadata.get("cuda_ipc_lifecycle")
    return {
        "receipt_id": receipt.receipt_id,
        "intent_id": receipt.intent_id,
        "decision_id": receipt.decision_id,
        "topology_snapshot_id": receipt.topology_snapshot_id,
        "ticket_id": receipt.ticket_id,
        "job_id": receipt.job_id,
        "session_id": receipt.session_id,
        "state": str(receipt.state.value),
        "bytes_total": int(receipt.bytes_total),
        "bytes_completed": int(receipt.bytes_completed),
        "completion_source": metadata.get("completion_source"),
        "transfer_id": metadata.get("transfer_id"),
        "verified": bool(metadata.get("verified", False)),
        "verified_bytes": int(metadata.get("verified_bytes", 0) or 0),
        "completion_contract": (
            dict(completion_contract)
            if isinstance(completion_contract, Mapping)
            else None
        ),
        "cuda_ipc_lifecycle": (
            dict(cuda_ipc_lifecycle)
            if isinstance(cuda_ipc_lifecycle, Mapping)
            else None
        ),
    }


def _runtime_buffer_bindings(
    receipt: TransferReceipt,
    metadata: Mapping[str, Any],
) -> list[dict[str, Any]]:
    lifetime = metadata.get("buffer_lifetime_evidence")
    if not isinstance(lifetime, Mapping):
        return []
    bindings: list[dict[str, Any]] = []
    for role, key in (
        ("source", "source_buffer"),
        ("destination", "destination_buffer"),
    ):
        record = lifetime.get(key)
        if not isinstance(record, Mapping):
            continue
        registration = record.get("registration")
        registration_mapping = (
            dict(registration) if isinstance(registration, Mapping) else {}
        )
        registration_metadata = registration_mapping.get("metadata")
        bindings.append(
            {
                "receipt_id": receipt.receipt_id,
                "intent_id": receipt.intent_id,
                "role": role,
                "buffer_id": record.get("buffer_id"),
                "handle_type": registration_mapping.get("handle_type"),
                "runtime_buffer_kind": record.get("runtime_buffer_kind"),
                "runtime_session_id": record.get("runtime_session_id"),
                "runtime_owned": bool(record.get("runtime_owned", False)),
                "registration_metadata": (
                    dict(registration_metadata)
                    if isinstance(registration_metadata, Mapping)
                    else None
                ),
                "resource_evidence": (
                    dict(record["resource_evidence"])
                    if isinstance(record.get("resource_evidence"), Mapping)
                    else None
                ),
                "cuda_ipc_lifecycle": (
                    dict(record["cuda_ipc_lifecycle"])
                    if isinstance(record.get("cuda_ipc_lifecycle"), Mapping)
                    else None
                ),
            }
        )
    return bindings


__all__ = [
    "adapter_lifecycle_evidence_from_handles",
    "join_unique",
    "receipt_trace_from_receipts",
    "unique_receipts_from_handles",
]
