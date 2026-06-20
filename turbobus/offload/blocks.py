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
    transfer_stats: object | None = None

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


class OffloadBlock:
    def __init__(self, block: "_OffloadBlock") -> None:
        self._block = block

    @property
    def name(self) -> str:
        return self._block.name

    @property
    def block_id(self) -> object:
        return self._block.block_id

    @property
    def cpu_tensor(self) -> object:
        return self._block.cpu_tensor

    @property
    def gpu_tensor(self) -> object:
        return self._block.gpu_tensor

    @property
    def cpu_slot(self) -> object | None:
        return self._block.cpu_slot

    @property
    def gpu_slot(self) -> object | None:
        return self._block.gpu_slot

    @property
    def cpu_offset(self) -> int:
        return self._block.cpu_offset

    @property
    def gpu_offset(self) -> int:
        return self._block.gpu_offset

    @property
    def bytes(self) -> int:
        return self._block.bytes

    @property
    def state(self) -> BlockState:
        return self._block.state

    @property
    def last_operation(self) -> str | None:
        return self._block.last_operation

    @property
    def last_handle(self) -> object | None:
        return self._block._last_handle

    def info(self) -> OffloadBlockInfo:
        return self._block.info()

@dataclass
class _OffloadBlock:
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
    _last_prefetch: object | None = None
    _last_evict: object | None = None
    _last_handle: object | None = None
    _last_operation: str | None = None

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
        if self._last_handle is None:
            return None
        return self._last_handle.stats

    @property
    def last_transfer_stats(self) -> TransferStats | None:
        if self._last_handle is None:
            return None
        return summarize_transfer_handles([self._last_handle])

    @property
    def last_operation(self) -> str | None:
        return self._last_operation

    def info(self) -> OffloadBlockInfo:
        # /*
        #  * ========================================================================
        #  * 步骤1：生成公开 block 信息对象
        #  * ========================================================================
        #  * 数据源：_OffloadBlock structural fields
        #  * 操作：
        #  *   1) 只复制 block 结构、位置和逻辑状态
        #  *   2) 不从 handle 提取 receipt/ticket/decision/topology identity
        #  */
        logger.info("开始生成公开 block 信息对象...")

        # // 1.1 返回结构化 block 信息，不携带运行态 transfer identity
        info = OffloadBlockInfo(
            name=self.name,
            block_id=self.block_id,
            cpu_slot=self.cpu_slot,
            gpu_slot=self.gpu_slot,
            cpu_offset=self.cpu_offset,
            gpu_offset=self.gpu_offset,
            bytes=self.bytes,
            state=self.state,
            last_operation=self.last_operation,
        )
        logger.info("公开 block 信息对象生成完成, name: %s", self.name)
        return info


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
]
