from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping
import uuid

from ..client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer
from ..runtime_session import TurboBusRuntimeSession
from ..schema import WorkloadKind


@dataclass(frozen=True)
class AdapterTransferContext:
    job_id: str
    session_id: str
    cpu_buffer_id: str
    gpu_buffer_id: str
    cpu_buffer: SharedPinnedCpuBuffer | object = field(repr=False, compare=False)
    gpu_buffer: CudaIpcDeviceBuffer | object = field(repr=False, compare=False)
    workload_kind: WorkloadKind | str = WorkloadKind.GENERIC
    priority: int = 0
    policy_hints: Mapping[str, object] = field(default_factory=dict)
    metadata: Mapping[str, object] = field(default_factory=dict)
    intent_prefix: str | None = None
    wait_timeout_seconds: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "job_id", _require_non_empty(self.job_id, "job_id"))
        object.__setattr__(
            self,
            "session_id",
            _require_non_empty(self.session_id, "session_id"),
        )
        object.__setattr__(
            self,
            "cpu_buffer_id",
            _require_non_empty(self.cpu_buffer_id, "cpu_buffer_id"),
        )
        object.__setattr__(
            self,
            "gpu_buffer_id",
            _require_non_empty(self.gpu_buffer_id, "gpu_buffer_id"),
        )
        object.__setattr__(self, "workload_kind", WorkloadKind(self.workload_kind))
        object.__setattr__(self, "priority", int(self.priority))
        object.__setattr__(
            self,
            "policy_hints",
            validate_policy_hints_no_physical(self.policy_hints),
        )
        object.__setattr__(self, "metadata", dict(self.metadata))
        prefix = self.intent_prefix or f"adapter-{uuid.uuid4()}"
        object.__setattr__(
            self,
            "intent_prefix",
            _require_non_empty(prefix, "intent_prefix"),
        )
        if self.wait_timeout_seconds is not None:
            timeout = float(self.wait_timeout_seconds)
            if timeout < 0:
                raise ValueError("wait_timeout_seconds must be non-negative")
            object.__setattr__(self, "wait_timeout_seconds", timeout)

    @classmethod
    def from_runtime_session(
        cls,
        runtime_session: TurboBusRuntimeSession,
        cpu_buffer,
        gpu_buffer,
        *,
        workload_kind: WorkloadKind | str = WorkloadKind.GENERIC,
        priority: int = 0,
        policy_hints: Mapping[str, object] | None = None,
        metadata: Mapping[str, object] | None = None,
        intent_prefix: str | None = None,
        wait_timeout_seconds: float | None = None,
    ) -> "AdapterTransferContext":
        require_runtime_session_open(runtime_session)
        context = runtime_session.make_adapter_transfer_context(
            cpu_buffer,
            gpu_buffer,
            workload_kind=workload_kind,
            priority=priority,
            policy_hints=policy_hints,
            metadata=metadata,
            intent_prefix=intent_prefix,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        if not isinstance(context, cls):
            raise TypeError(
                "runtime session adapter context builder must return an AdapterTransferContext"
            )
        return context


def require_runtime_session_open(runtime_session) -> None:
    if not isinstance(runtime_session, TurboBusRuntimeSession):
        raise TypeError("offload adapters require a TurboBusRuntimeSession")
    if bool(getattr(runtime_session, "closed", False)):
        raise RuntimeError("runtime session is closed")


def validate_policy_hints_no_physical(value: Mapping[str, object]) -> dict[str, object]:
    policy_hints = dict(value)
    forbidden_keys = {
        "mode",
        "path",
        "paths",
        "route",
        "routes",
        "relay",
        "relays",
        "relay_gpu",
        "relay_gpus",
        "target_device",
        "target_gpu",
    }
    invalid_keys = sorted(
        key for key in policy_hints if str(key).lower() in forbidden_keys
    )
    if invalid_keys:
        raise ValueError(
            "policy_hints must not choose physical paths: "
            + ", ".join(str(key) for key in invalid_keys)
        )
    return policy_hints


def _require_non_empty(value: object, field_name: str) -> str:
    normalized = str(value)
    if not normalized.strip():
        raise ValueError(f"{field_name} must be non-empty")
    return normalized


__all__ = [
    "AdapterTransferContext",
    "require_runtime_session_open",
    "validate_policy_hints_no_physical",
]
