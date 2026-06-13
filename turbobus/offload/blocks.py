from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import logging

from .stats import TransferStats, summarize_transfer_handles

logger = logging.getLogger(__name__)


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
        # /*
        #  * ========================================================================
        #  * 步骤1：生成公开 block 结构快照
        #  * ========================================================================
        #  * 目标对象：OffloadBlockInfo public snapshot
        #  * 操作：
        #  *   1) 只公开 block 结构、位置和逻辑状态
        #  *   2) 不在裸 block 快照中暴露 receipt/ticket/decision 运行态字段
        #  */
        logger.info("开始生成公开 block 结构快照...")

        # // 1.1 返回不含运行态 receipt 字段的结构信息
        snapshot = {
            "name": self.name,
            "block_id": self.block_id,
            "cpu_slot": self.cpu_slot,
            "gpu_slot": self.gpu_slot,
            "cpu_offset": self.cpu_offset,
            "gpu_offset": self.gpu_offset,
            "bytes": self.bytes,
            "state": self.state.value,
            "last_operation": self.last_operation,
        }
        logger.info("公开 block 结构快照生成完成, name: %s", self.name)
        return snapshot


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
        if self.cpu_tensor is None:
            raise ValueError("offload blocks require a real CPU backing object")
        if self.gpu_tensor is None:
            raise ValueError("offload blocks require a real GPU backing object")
        if self.block_id is None:
            self.block_id = self.name

    @property
    def bytes(self) -> int:
        if self.byte_count is not None:
            return int(self.byte_count)
        return _backing_nbytes(self.cpu_tensor)

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


def _backing_nbytes(backing: object) -> int:
    numel = getattr(backing, "numel", None)
    element_size = getattr(backing, "element_size", None)
    if callable(numel) and callable(element_size):
        return int(numel() * element_size())
    size_bytes = getattr(backing, "size_bytes", None)
    if size_bytes is not None:
        return int(size_bytes)
    nbytes = getattr(backing, "nbytes", None)
    if nbytes is not None:
        return int(nbytes)
    raise TypeError(
        "offload block backing must expose numel/element_size, size_bytes, or nbytes"
    )


__all__ = [
    "BlockState",
    "OffloadBlock",
    "OffloadBlockInfo",
    "transfer_identity_from_handle",
]
