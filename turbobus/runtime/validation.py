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


__all__ = [
    "require_complete_receipt_evidence",
    "require_failed_receipt_evidence",
    "require_receipt_ticket_binding",
    "validate_intent_ranges_fit_buffers",
    "validate_runtime_receipt",
]
