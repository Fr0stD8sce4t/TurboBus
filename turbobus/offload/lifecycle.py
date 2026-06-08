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
    for receipt in receipt_list:
        receipt_ids.append(receipt.receipt_id)
        intent_ids.append(receipt.intent_id)
        decision_ids.append(receipt.decision_id)
        topology_snapshot_ids.append(receipt.topology_snapshot_id)
        ticket_ids.append(receipt.ticket_id)
        receipt_states.append(str(receipt.state.value))
        completion_source = receipt.metadata.get("completion_source")
        if completion_source:
            completion_sources.append(str(completion_source))
        transfer_id = receipt.metadata.get("transfer_id")
        if transfer_id:
            transfer_ids.append(str(transfer_id))
        fallback_reason = receipt.metadata.get("fallback_reason")
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


__all__ = [
    "adapter_lifecycle_evidence_from_handles",
    "join_unique",
    "receipt_trace_from_receipts",
    "unique_receipts_from_handles",
]
