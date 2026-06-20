from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Iterable, Mapping

from ..schema import TransferReceipt

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TransferStats:
    bytes: int = 0
    direct_chunks: int = 0
    relay_chunks: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "bytes": self.bytes,
            "direct_chunks": self.direct_chunks,
            "relay_chunks": self.relay_chunks,
        }


@dataclass(frozen=True)
class TransferStatsSnapshot:
    payload: Mapping[str, object]

    def __eq__(self, other: object) -> bool:
        if isinstance(other, TransferStats):
            return (
                self.bytes == other.bytes
                and self.direct_chunks == other.direct_chunks
                and self.relay_chunks == other.relay_chunks
            )
        if isinstance(other, TransferStatsSnapshot):
            return self.as_dict() == other.as_dict()
        return NotImplemented

    @property
    def bytes(self) -> int:
        return int(self.payload.get("bytes", 0) or 0)

    @property
    def direct_chunks(self) -> int:
        return int(self.payload.get("direct_chunks", 0) or 0)

    @property
    def relay_chunks(self) -> int:
        return int(self.payload.get("relay_chunks", 0) or 0)

    def as_dict(self) -> dict[str, object]:
        # /*
        #  * ========================================================================
        #  * 步骤1：导出 RuntimeSession 绑定 stats 快照
        #  * ========================================================================
        #  * 数据源：TransferStatsSnapshot payload
        #  * 操作：
        #  *   1) 返回已包含 RuntimeSession adapter evidence 的统计快照
        #  *   2) 不降级为裸 direct/relay 计数
        #  */
        logger.info("开始导出 RuntimeSession 绑定 stats 快照...")

        # // 1.1 复制 evidence-bound payload
        snapshot = dict(self.payload)
        logger.info(
            "RuntimeSession 绑定 stats 快照导出完成, receipts: %s",
            snapshot.get("receipt_count", 0),
        )
        return snapshot


def summarize_transfer_handles(handles: Iterable) -> TransferStats:
    unique = []
    seen = set()
    for handle in handles:
        if id(handle) in seen:
            continue
        stats = getattr(handle, "_raw_stats", None)
        if stats is None:
            stats = getattr(handle, "stats", None)
        if stats is None:
            continue
        seen.add(id(handle))
        unique.append(stats)
    return TransferStats(
        bytes=sum(_stat_value(stats, "bytes") for stats in unique),
        direct_chunks=sum(_stat_value(stats, "direct_chunks") for stats in unique),
        relay_chunks=sum(_stat_value(stats, "relay_chunks") for stats in unique),
    )


def transfer_stats_from_receipt(receipt: TransferReceipt) -> TransferStats:
    direct_bytes = 0
    relay_bytes = 0
    direct_chunks = 0
    relay_chunks = 0
    for path in receipt.path_stats:
        bytes_count = int(path.get("bytes", 0) or 0)
        chunk_count = int(path.get("chunk_count", path.get("chunks", 0)) or 0)
        if str(path.get("kind", "")).lower() == "relay":
            relay_bytes += bytes_count
            relay_chunks += chunk_count
        else:
            direct_bytes += bytes_count
            direct_chunks += chunk_count
    return TransferStats(
        bytes=direct_bytes + relay_bytes,
        direct_chunks=direct_chunks,
        relay_chunks=relay_chunks,
    )


def _stat_value(stats, name: str) -> int:
    if isinstance(stats, dict):
        return int(stats.get(name, 0) or 0)
    return int(getattr(stats, name, 0) or 0)


__all__ = [
    "TransferStats",
    "TransferStatsSnapshot",
    "summarize_transfer_handles",
    "transfer_stats_from_receipt",
]
