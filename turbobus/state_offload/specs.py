from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from ..offload.context import forbidden_physical_policy_keys
from ..schema import WorkloadKind
from .core import StateOffloadCore, StateOffloadLifecycle, StateOffloadSpec


@dataclass(frozen=True)
class ModelWeightStateLifecycle(StateOffloadLifecycle):
    evidence: dict[str, Any]


@dataclass(frozen=True)
class TrainingStateLifecycle(StateOffloadLifecycle):
    evidence: dict[str, Any]


@dataclass(frozen=True)
class OptimizerStateLifecycle(StateOffloadLifecycle):
    evidence: dict[str, Any]


def model_weight_state_spec(
    *,
    extra_evidence=None,
) -> StateOffloadSpec:
    return StateOffloadSpec(
        state_kind="model_weights",
        evidence_prefix="model-load",
        item_field="tensor_names",
        item_count_field="tensor_count",
        binding_field="tensor_bindings",
        range_field="bucket_ranges",
        lifecycle_type=ModelWeightStateLifecycle,
        metadata_validator=validate_model_weight_metadata,
        metadata_field_name="model loader context metadata",
        extra_evidence=extra_evidence,
    )


def training_state_spec() -> StateOffloadSpec:
    return StateOffloadSpec(
        state_kind="training_state",
        evidence_prefix="training-state",
        item_field="bucket_names",
        item_count_field="bucket_count",
        binding_field="bucket_bindings",
        range_field="bucket_ranges",
        lifecycle_type=TrainingStateLifecycle,
        metadata_validator=validate_training_state_metadata,
        metadata_field_name="training offload context metadata",
    )


def optimizer_state_spec() -> StateOffloadSpec:
    return StateOffloadSpec(
        state_kind="optimizer_state",
        evidence_prefix="optimizer-state",
        item_field="bucket_names",
        item_count_field="bucket_count",
        binding_field="bucket_bindings",
        range_field="bucket_ranges",
        lifecycle_type=OptimizerStateLifecycle,
        metadata_validator=validate_optimizer_state_metadata,
        metadata_field_name="optimizer offload context metadata",
    )


def model_manifest_extra_evidence(
    core: StateOffloadCore,
    operation: str,
    names: Iterable[str],
) -> dict[str, object]:
    registry = getattr(core, "state_registry", None)
    manifest = getattr(registry, "manifest", None)
    return {
        "load_direction": "h2d",
        "manifest_tensor_count": 0 if manifest is None else len(manifest.tensors),
        "manifest_tensor_names": [] if manifest is None else manifest.names(),
        "manifest_cpu_span_bytes": 0 if manifest is None else manifest.cpu_span_bytes,
        "manifest_gpu_span_bytes": 0 if manifest is None else manifest.gpu_span_bytes,
        "manifest_metadata": {} if manifest is None else dict(manifest.metadata),
    }


def validate_model_weight_metadata(
    metadata: Mapping[str, object] | None,
    *,
    field_name: str,
) -> dict[str, object]:
    return _validate_no_physical_policy(metadata, field_name=field_name)


def validate_training_state_metadata(
    metadata: Mapping[str, object] | None,
    *,
    field_name: str,
) -> dict[str, object]:
    return _validate_no_physical_policy(metadata, field_name=field_name)


def validate_optimizer_state_metadata(
    metadata: Mapping[str, object] | None,
    *,
    field_name: str,
) -> dict[str, object]:
    return _validate_no_physical_policy(metadata, field_name=field_name)


def workload_kind_for_spec(spec: StateOffloadSpec) -> WorkloadKind:
    if spec.state_kind == "model_weights":
        return WorkloadKind.MODEL_WEIGHTS
    if spec.state_kind == "training_state":
        return WorkloadKind.TRAINING_STATE
    if spec.state_kind == "optimizer_state":
        return WorkloadKind.OPTIMIZER_STATE
    return WorkloadKind.GENERIC


def _validate_no_physical_policy(
    metadata: Mapping[str, object] | None,
    *,
    field_name: str,
) -> dict[str, object]:
    resolved = {} if metadata is None else dict(metadata)
    invalid_keys = forbidden_physical_policy_keys(resolved)
    if invalid_keys:
        raise ValueError(
            f"{field_name} must not choose physical paths: "
            + ", ".join(str(key) for key in invalid_keys)
        )
    return resolved


__all__ = [
    "ModelWeightStateLifecycle",
    "OptimizerStateLifecycle",
    "TrainingStateLifecycle",
    "model_manifest_extra_evidence",
    "model_weight_state_spec",
    "optimizer_state_spec",
    "training_state_spec",
    "validate_model_weight_metadata",
    "validate_optimizer_state_metadata",
    "validate_training_state_metadata",
    "workload_kind_for_spec",
]
