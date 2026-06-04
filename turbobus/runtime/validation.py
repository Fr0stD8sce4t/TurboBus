from __future__ import annotations

from collections.abc import Mapping

from ..schema import TransferIntent, TransferReceipt


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
    for key in ("execution_ticket_id", "evidence_ticket_id"):
        ticket_id = metadata.get(key)
        if ticket_id is not None and str(ticket_id) != receipt.ticket_id:
            raise ValueError(f"runtime receipt {key} does not match receipt ticket_id")


__all__ = [
    "validate_intent_ranges_fit_buffers",
    "validate_runtime_receipt",
]
