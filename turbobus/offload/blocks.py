from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .stats import TransferStats, summarize_transfer_handles


class BlockState(str, Enum):
    CPU = "cpu"
    GPU = "gpu"
    PREFETCHING = "prefetching"
    EVICTING = "evicting"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class OffloadBlockInfo:
    name: str
    block_id: object
    cpu_slot: object | None
    gpu_slot: object | None
    cpu_offset: int
    gpu_offset: int
    bytes: int
    state: BlockState
    last_operation: str | None
    transfer_stats: TransferStats | None
    last_intent_id: str | None = None
    last_receipt_id: str | None = None
    last_ticket_id: str | None = None
    last_decision_id: str | None = None
    last_topology_snapshot_id: str | None = None
    last_job_id: str | None = None
    last_session_id: str | None = None
    last_receipt_state: str | None = None
    last_transfer_error: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "block_id": self.block_id,
            "cpu_slot": self.cpu_slot,
            "gpu_slot": self.gpu_slot,
            "cpu_offset": self.cpu_offset,
            "gpu_offset": self.gpu_offset,
            "bytes": self.bytes,
            "state": self.state.value,
            "last_operation": self.last_operation,
            "transfer_stats": (
                self.transfer_stats.as_dict()
                if self.transfer_stats is not None
                else None
            ),
            "last_intent_id": self.last_intent_id,
            "last_receipt_id": self.last_receipt_id,
            "last_ticket_id": self.last_ticket_id,
            "last_decision_id": self.last_decision_id,
            "last_topology_snapshot_id": self.last_topology_snapshot_id,
            "last_job_id": self.last_job_id,
            "last_session_id": self.last_session_id,
            "last_receipt_state": self.last_receipt_state,
            "last_transfer_error": self.last_transfer_error,
        }


@dataclass
class OffloadBlock:
    name: str
    cpu_tensor: object
    gpu_tensor: object
    block_id: object | None = None
    cpu_slot: object | None = None
    gpu_slot: object | None = None
    cpu_offset: int = 0
    gpu_offset: int = 0
    byte_count: int | None = None
    state: BlockState = BlockState.CPU
    last_prefetch: object | None = None
    last_evict: object | None = None
    last_handle: object | None = None
    last_operation: str | None = None

    def __post_init__(self) -> None:
        if self.block_id is None:
            self.block_id = self.name

    @property
    def bytes(self) -> int:
        if self.byte_count is not None:
            return int(self.byte_count)
        return int(self.cpu_tensor.numel() * self.cpu_tensor.element_size())

    @property
    def last_stats(self):
        if self.last_handle is None:
            return None
        return self.last_handle.stats

    @property
    def last_transfer_stats(self) -> TransferStats | None:
        if self.last_handle is None:
            return None
        return summarize_transfer_handles([self.last_handle])

    def info(self) -> OffloadBlockInfo:
        transfer_identity = transfer_identity_from_handle(self.last_handle)
        return OffloadBlockInfo(
            name=self.name,
            block_id=self.block_id,
            cpu_slot=self.cpu_slot,
            gpu_slot=self.gpu_slot,
            cpu_offset=self.cpu_offset,
            gpu_offset=self.gpu_offset,
            bytes=self.bytes,
            state=self.state,
            last_operation=self.last_operation,
            transfer_stats=self.last_transfer_stats,
            **transfer_identity,
        )


def transfer_identity_from_handle(handle: object | None) -> dict[str, str | None]:
    if handle is None:
        return {
            "last_intent_id": None,
            "last_receipt_id": None,
            "last_ticket_id": None,
            "last_decision_id": None,
            "last_topology_snapshot_id": None,
            "last_job_id": None,
            "last_session_id": None,
            "last_receipt_state": None,
            "last_transfer_error": None,
        }
    intent = getattr(handle, "intent", None)
    receipt = getattr(handle, "receipt", None)
    return {
        "last_intent_id": _optional_str(getattr(intent, "intent_id", None)),
        "last_receipt_id": _optional_str(getattr(receipt, "receipt_id", None)),
        "last_ticket_id": _optional_str(getattr(receipt, "ticket_id", None)),
        "last_decision_id": _optional_str(getattr(receipt, "decision_id", None)),
        "last_topology_snapshot_id": _optional_str(
            getattr(receipt, "topology_snapshot_id", None)
        ),
        "last_job_id": _optional_str(getattr(receipt, "job_id", None)),
        "last_session_id": _optional_str(getattr(receipt, "session_id", None)),
        "last_receipt_state": _optional_str(getattr(receipt, "state", None)),
        "last_transfer_error": _optional_str(getattr(receipt, "error", None)),
    }


def _optional_str(value: object | None) -> str | None:
    if value is None:
        return None
    return str(value)


__all__ = [
    "BlockState",
    "OffloadBlock",
    "OffloadBlockInfo",
    "transfer_identity_from_handle",
]
