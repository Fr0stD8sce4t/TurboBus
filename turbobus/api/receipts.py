from __future__ import annotations

from collections.abc import Mapping

from ..schema import TransferReceipt, TransferStatusState


def require_complete_receipt_evidence(receipt: TransferReceipt) -> None:
    if TransferStatusState(receipt.state) is not TransferStatusState.COMPLETE:
        return
    metadata = receipt.metadata if isinstance(receipt.metadata, Mapping) else {}
    completion_source = str(metadata.get("completion_source", "")).lower()
    if completion_source not in {"worker", "backend"}:
        raise ValueError("complete receipt missing worker/backend execution source")
    if not bool(metadata.get("executed", False)):
        raise ValueError("complete receipt missing execution evidence")
    if not bool(metadata.get("verified", False)):
        raise ValueError("complete receipt missing verification evidence")
    try:
        verified_bytes = int(metadata.get("verified_bytes", 0) or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError("complete receipt has invalid verified bytes") from exc
    if verified_bytes != int(receipt.bytes_total):
        raise ValueError(
            "complete receipt verified byte mismatch: "
            f"{verified_bytes} != {int(receipt.bytes_total)}"
        )
    if not bool(metadata.get("content_match", False)):
        raise ValueError("complete receipt missing content-match evidence")


__all__ = ["require_complete_receipt_evidence"]
