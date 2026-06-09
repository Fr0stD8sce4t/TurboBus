from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Protocol


@dataclass(frozen=True)
class BackendExactPlanRequest:
    """Exact daemon-issued plan request passed to a transfer backend."""

    direction: str
    host_ptr: int
    host_bytes: int
    device_ptr: int
    device_bytes: int
    plan: Any

    def __post_init__(self) -> None:
        normalized_direction = str(self.direction).lower()
        if normalized_direction not in {"h2d", "d2h"}:
            raise ValueError("direction must be h2d or d2h")
        if int(self.host_ptr) <= 0:
            raise ValueError("host_ptr must be positive")
        if int(self.host_bytes) <= 0:
            raise ValueError("host_bytes must be positive")
        if int(self.device_ptr) <= 0:
            raise ValueError("device_ptr must be positive")
        if int(self.device_bytes) <= 0:
            raise ValueError("device_bytes must be positive")
        object.__setattr__(self, "direction", normalized_direction)
        object.__setattr__(self, "host_ptr", int(self.host_ptr))
        object.__setattr__(self, "host_bytes", int(self.host_bytes))
        object.__setattr__(self, "device_ptr", int(self.device_ptr))
        object.__setattr__(self, "device_bytes", int(self.device_bytes))


@dataclass(frozen=True)
class BackendSubmission:
    """Backend-owned runtime submission for one exact daemon-issued plan."""

    backend_name: str
    runtime: Any
    handle: Any
    native_plan: Any
    direction: str

    def __post_init__(self) -> None:
        normalized_direction = str(self.direction).lower()
        if normalized_direction not in {"h2d", "d2h"}:
            raise ValueError("direction must be h2d or d2h")
        object.__setattr__(self, "backend_name", str(self.backend_name))
        object.__setattr__(self, "direction", normalized_direction)


class TransferBackend(Protocol):
    @property
    def name(self) -> str:
        ...

    def bind_runtime(self, native_module: Any, torch_module: Any) -> None:
        ...

    def require_available(self) -> None:
        ...

    def require_torch(self) -> None:
        ...

    def create_runtime(self, options: Any) -> Any:
        ...

    def initialize_runtime(
        self,
        runtime: Any,
        target_device: int,
        relay_gpus: Iterable[int],
    ) -> None:
        ...

    def make_ranges(
        self,
        ranges: Iterable,
        source_bytes: int,
        destination_bytes: int,
    ) -> list:
        ...

    def make_transfer_plan(self, plan: Any) -> Any:
        ...

    def submit_exact_plan(
        self,
        runtime: Any,
        request: BackendExactPlanRequest,
    ) -> BackendSubmission:
        ...

    def transfer_mode_value(self, mode: Any) -> Any:
        ...

    def allocate_device_memory(self, bytes_: int) -> int:
        ...

    def free_device_memory(self, device_ptr: int) -> None:
        ...

    def verify_transfer(
        self,
        *,
        target_device: int,
        direction: str,
        host_ptr: int,
        host_bytes: int,
        device_ptr: int,
        device_bytes: int,
        ranges: Iterable,
    ) -> dict[str, object]:
        ...

    def fetch_plan_to_gpu(
        self,
        runtime: Any,
        host_ptr: int,
        host_bytes: int,
        target_ptr: int,
        target_bytes: int,
        plan: Any,
    ) -> Any:
        ...

    def offload_plan_to_cpu(
        self,
        runtime: Any,
        target_ptr: int,
        target_bytes: int,
        host_ptr: int,
        host_bytes: int,
        plan: Any,
    ) -> Any:
        ...

    def wait(self, runtime: Any, handle: Any) -> None:
        ...

    def stats(self, runtime: Any, handle: Any) -> Any:
        ...


__all__ = ["BackendExactPlanRequest", "BackendSubmission", "TransferBackend"]
