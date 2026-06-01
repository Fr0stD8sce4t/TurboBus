from __future__ import annotations

from collections.abc import Mapping

from ..schema import (
    TransferReceipt,
    TransferStatusState,
    require_complete_receipt_metadata_evidence,
)


def require_complete_receipt_evidence(receipt: TransferReceipt) -> None:
    if TransferStatusState(receipt.state) is not TransferStatusState.COMPLETE:
        return
    metadata = receipt.metadata if isinstance(receipt.metadata, Mapping) else {}
    require_complete_receipt_metadata_evidence(metadata, int(receipt.bytes_total))


__all__ = ["require_complete_receipt_evidence"]
