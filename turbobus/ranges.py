from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Mapping


@dataclass(frozen=True)
class TransferRange:
    src_offset: int
    dst_offset: int
    bytes: int

    def __post_init__(self) -> None:
        if int(self.src_offset) < 0 or int(self.dst_offset) < 0:
            raise ValueError("range offsets must be non-negative")
        if int(self.bytes) <= 0:
            raise ValueError("range bytes must be positive")
        object.__setattr__(self, "src_offset", int(self.src_offset))
        object.__setattr__(self, "dst_offset", int(self.dst_offset))
        object.__setattr__(self, "bytes", int(self.bytes))

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


def range_as_dict(item: TransferRange | tuple[int, int, int] | dict) -> dict[str, int]:
    if isinstance(item, TransferRange):
        return item.as_dict()
    if isinstance(item, Mapping):
        return {
            "src_offset": int(item["src_offset"]),
            "dst_offset": int(item["dst_offset"]),
            "bytes": int(item["bytes"]),
        }
    if isinstance(item, tuple) or isinstance(item, list):
        if len(item) != 3:
            raise ValueError("range tuples must be (src_offset, dst_offset, bytes)")
        return {
            "src_offset": int(item[0]),
            "dst_offset": int(item[1]),
            "bytes": int(item[2]),
        }
    return {
        "src_offset": int(getattr(item, "src_offset")),
        "dst_offset": int(getattr(item, "dst_offset")),
        "bytes": int(getattr(item, "bytes")),
    }


__all__ = ["TransferRange", "range_as_dict"]
