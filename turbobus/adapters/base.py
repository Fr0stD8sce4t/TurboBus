from __future__ import annotations

from typing import Protocol


class FrameworkBinding(Protocol):
    """Marker protocol for framework-facing TurboBus bindings."""


__all__ = ["FrameworkBinding"]
