from __future__ import annotations

from typing import Mapping

from ..schema import WorkloadKind
from .specs import model_weight_state_spec, optimizer_state_spec, training_state_spec


def make_training_state_offload(
    runtime_session,
    cpu_buffer,
    gpu_buffer,
    *,
    workload_kind: WorkloadKind | str = WorkloadKind.TRAINING_STATE,
    priority: int = 0,
    metadata: Mapping[str, object] | None = None,
    intent_prefix: str | None = None,
    wait_timeout_seconds: float | None = None,
):
    return runtime_session.make_state_offload(
        training_state_spec(),
        cpu_buffer,
        gpu_buffer,
        workload_kind=workload_kind,
        priority=priority,
        metadata=metadata,
        intent_prefix=intent_prefix,
        wait_timeout_seconds=wait_timeout_seconds,
    )


def make_optimizer_state_offload(
    runtime_session,
    cpu_buffer,
    gpu_buffer,
    *,
    workload_kind: WorkloadKind | str = WorkloadKind.OPTIMIZER_STATE,
    priority: int = 0,
    metadata: Mapping[str, object] | None = None,
    intent_prefix: str | None = None,
    wait_timeout_seconds: float | None = None,
):
    return runtime_session.make_state_offload(
        optimizer_state_spec(),
        cpu_buffer,
        gpu_buffer,
        workload_kind=workload_kind,
        priority=priority,
        metadata=metadata,
        intent_prefix=intent_prefix,
        wait_timeout_seconds=wait_timeout_seconds,
    )


def make_model_weight_state_offload(
    runtime_session,
    cpu_buffer,
    gpu_buffer,
    *,
    priority: int = 0,
    policy_hints: Mapping[str, object] | None = None,
    metadata: Mapping[str, object] | None = None,
    intent_prefix: str | None = None,
    wait_timeout_seconds: float | None = None,
    extra_evidence=None,
):
    return runtime_session.make_state_offload(
        model_weight_state_spec(extra_evidence=extra_evidence),
        cpu_buffer,
        gpu_buffer,
        workload_kind=WorkloadKind.MODEL_WEIGHTS,
        priority=priority,
        policy_hints=policy_hints,
        metadata=metadata,
        intent_prefix=intent_prefix,
        wait_timeout_seconds=wait_timeout_seconds,
    )


__all__ = [
    "make_model_weight_state_offload",
    "make_optimizer_state_offload",
    "make_training_state_offload",
]
