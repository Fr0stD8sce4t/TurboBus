from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from ..schema import TransferReceipt


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


def summarize_transfer_handles(handles: Iterable) -> TransferStats:
    unique = []
    seen = set()
    for handle in handles:
        if id(handle) in seen:
            continue
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
    "summarize_transfer_handles",
    "transfer_stats_from_receipt",
]
