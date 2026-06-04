from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from ..runtime_session import TurboBusRuntimeSession
from ..runtime.validation import require_complete_receipt_evidence
from ..schema import TransferIntent, TransferReceipt, TransferStatusState
from .context import AdapterTransferContext, require_runtime_session_open
from .stats import TransferStats, transfer_stats_from_receipt


@dataclass
class ReceiptTransferHandle:
    client: TurboBusRuntimeSession
    intent: TransferIntent
    receipt: TransferReceipt
    transfer_context: AdapterTransferContext | None = None
    wait_timeout_seconds: float | None = None
    wait_calls: int = 0
    _waited: bool = field(default=False, init=False, repr=False)

    @property
    def stats(self) -> TransferStats:
        return transfer_stats_from_receipt(self.receipt)

    def wait(self) -> TransferReceipt:
        if self._waited:
            return self.receipt
        require_runtime_session_open(self.client)
        self.receipt = self.client.wait_transfer_receipt(
            self.intent.intent_id,
            timeout_seconds=self.wait_timeout_seconds,
        )
        if not isinstance(self.receipt, TransferReceipt):
            raise TypeError("wait_transfer_receipt must return a TransferReceipt")
        validate_adapter_receipt(
            self.receipt,
            self.intent,
            transfer_context=self.transfer_context,
        )
        self.wait_calls += 1
        self._waited = True
        state = TransferStatusState(self.receipt.state)
        if state in {TransferStatusState.FAILED, TransferStatusState.CANCELED}:
            raise RuntimeError(self.receipt.error or f"transfer {state.value}")
        return self.receipt


def validate_adapter_receipt(
    receipt: TransferReceipt,
    intent: TransferIntent,
    *,
    transfer_context: AdapterTransferContext | None = None,
) -> None:
    if receipt.intent_id != intent.intent_id:
        raise ValueError("receipt intent_id does not match transfer intent")
    if receipt.job_id != intent.job_id:
        raise ValueError("receipt job_id does not match transfer intent")
    if receipt.session_id != intent.session_id:
        raise ValueError("receipt session_id does not match transfer intent")
    if transfer_context is not None:
        if receipt.job_id != transfer_context.job_id:
            raise ValueError("receipt job_id does not match adapter context")
        if receipt.session_id != transfer_context.session_id:
            raise ValueError("receipt session_id does not match adapter context")
    metadata = receipt.metadata if isinstance(receipt.metadata, Mapping) else {}
    for key in ("execution_ticket_id", "evidence_ticket_id"):
        ticket_id = metadata.get(key)
        if ticket_id is not None and str(ticket_id) != receipt.ticket_id:
            raise ValueError(f"receipt {key} does not match receipt ticket_id")
    require_complete_receipt_evidence(receipt)


__all__ = [
    "ReceiptTransferHandle",
    "validate_adapter_receipt",
]
