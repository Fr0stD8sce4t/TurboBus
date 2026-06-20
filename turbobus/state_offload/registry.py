from __future__ import annotations

from typing import Iterable, Protocol

from .core import StateDescriptor


class StateRegistry(Protocol):
    def rebuild(self) -> tuple[StateDescriptor, ...]:
        ...

    def names(self) -> list[str]:
        ...

    def select(self, names: Iterable[str] | None = None) -> list[str]:
        ...


class StaticStateRegistry:
    def __init__(self, states: Iterable[StateDescriptor]) -> None:
        self._states = tuple(states)
        self._state_by_name = {state.name: state for state in self._states}

    @property
    def states(self) -> tuple[StateDescriptor, ...]:
        return self._states

    def rebuild(self) -> tuple[StateDescriptor, ...]:
        return self._states

    def names(self) -> list[str]:
        return [state.name for state in self._states]

    def select(self, names: Iterable[str] | None = None) -> list[str]:
        if names is None:
            return self.names()
        selected = [str(name) for name in names]
        missing = [name for name in selected if name not in self._state_by_name]
        if missing:
            raise KeyError(f"unknown state: {missing[0]}")
        return selected


class PackedStateRegistry(StaticStateRegistry):
    def __init__(
        self,
        *,
        prefix: str,
        cpu_tensor,
        gpu_tensor,
        bucket_bytes: int,
        bucket_count: int,
        start_offset: int = 0,
    ) -> None:
        if bucket_bytes <= 0:
            raise ValueError("bucket_bytes must be positive")
        if bucket_count <= 0:
            raise ValueError("bucket_count must be positive")
        if start_offset < 0:
            raise ValueError("start_offset must be non-negative")
        super().__init__(
            StateDescriptor(
                name=f"{prefix}{index}",
                state_id=index,
                cpu_tensor=cpu_tensor,
                gpu_tensor=gpu_tensor,
                cpu_slot=index,
                gpu_slot=index,
                cpu_offset=start_offset + index * bucket_bytes,
                gpu_offset=start_offset + index * bucket_bytes,
                byte_count=bucket_bytes,
            )
            for index in range(bucket_count)
        )


__all__ = [
    "StateRegistry",
    "PackedStateRegistry",
    "StaticStateRegistry",
]
