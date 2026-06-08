"""Offload implementation modules for TurboBus runtime-session adapters."""

from .lifecycle import (
    adapter_lifecycle_evidence_from_handles,
    join_unique,
    receipt_trace_from_receipts,
    unique_receipts_from_handles,
)

__all__ = [
    "adapter_lifecycle_evidence_from_handles",
    "join_unique",
    "receipt_trace_from_receipts",
    "unique_receipts_from_handles",
]
