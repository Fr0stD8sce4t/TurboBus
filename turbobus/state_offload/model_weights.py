from __future__ import annotations

from typing import Iterable, Mapping

from ..model_manifest import ModelWeightManifest, ModelWeightTensor
from .core import StateDescriptor
from .registry import StaticStateRegistry
from .specs import validate_model_weight_metadata


class ModelWeightStateRegistry(StaticStateRegistry):
    def __init__(
        self,
        manifest: ModelWeightManifest | Iterable[ModelWeightTensor | Mapping[str, object]],
        *,
        cpu_buffer,
        gpu_buffer,
    ) -> None:
        self.manifest = _coerce_manifest(manifest)
        _validate_manifest_metadata_no_physical_policy(self.manifest)
        _validate_manifest_backing_span(
            self.manifest,
            cpu_buffer=cpu_buffer,
            gpu_buffer=gpu_buffer,
        )
        super().__init__(
            StateDescriptor(
                name=tensor.name,
                state_id=tensor.tensor_id,
                cpu_tensor=cpu_buffer,
                gpu_tensor=gpu_buffer,
                cpu_slot=tensor.name,
                gpu_slot=tensor.name,
                cpu_offset=tensor.cpu_offset,
                gpu_offset=int(tensor.gpu_offset),
                byte_count=tensor.byte_count,
                metadata=tensor.metadata,
            )
            for tensor in self.manifest.tensors
        )


def model_weight_manifest_extra_evidence(registry: ModelWeightStateRegistry):
    def extra(core, operation: str, names: Iterable[str]) -> dict[str, object]:
        manifest = registry.manifest
        return {
            "load_direction": "h2d",
            "manifest_tensor_count": len(manifest.tensors),
            "manifest_tensor_names": manifest.names(),
            "manifest_cpu_span_bytes": manifest.cpu_span_bytes,
            "manifest_gpu_span_bytes": manifest.gpu_span_bytes,
            "manifest_metadata": dict(manifest.metadata),
        }

    return extra


def _coerce_manifest(
    manifest: ModelWeightManifest | Iterable[ModelWeightTensor | Mapping[str, object]],
) -> ModelWeightManifest:
    if isinstance(manifest, ModelWeightManifest):
        return manifest
    return ModelWeightManifest(tuple(_coerce_tensor(item) for item in manifest))


def _coerce_tensor(tensor: ModelWeightTensor | Mapping[str, object]) -> ModelWeightTensor:
    if isinstance(tensor, ModelWeightTensor):
        return tensor
    if isinstance(tensor, Mapping):
        return ModelWeightTensor(
            name=str(tensor["name"]),
            dtype=str(tensor["dtype"]),
            shape=tuple(tensor.get("shape", ())),
            byte_count=int(tensor["byte_count"]),
            cpu_offset=int(tensor["cpu_offset"]),
            gpu_offset=(
                None if tensor.get("gpu_offset") is None else int(tensor["gpu_offset"])
            ),
            tensor_id=tensor.get("tensor_id", tensor.get("name")),
            metadata=dict(tensor.get("metadata", {})),
        )
    raise TypeError("model weight tensor must be a ModelWeightTensor or mapping")


def _validate_manifest_metadata_no_physical_policy(
    manifest: ModelWeightManifest,
) -> None:
    validate_model_weight_metadata(
        manifest.metadata,
        field_name="model weight manifest metadata",
    )
    for tensor in manifest.tensors:
        validate_model_weight_metadata(
            tensor.metadata,
            field_name=f"model weight tensor {tensor.name} metadata",
        )


def _validate_manifest_backing_span(
    manifest: ModelWeightManifest,
    *,
    cpu_buffer,
    gpu_buffer,
) -> None:
    cpu_size = _optional_backing_nbytes(cpu_buffer)
    if cpu_size is not None and manifest.cpu_span_bytes > cpu_size:
        raise ValueError("model weight manifest exceeds CPU backing size")
    gpu_size = _optional_backing_nbytes(gpu_buffer)
    if gpu_size is not None and manifest.gpu_span_bytes > gpu_size:
        raise ValueError("model weight manifest exceeds GPU backing size")


def _optional_backing_nbytes(backing) -> int | None:
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
    return None


__all__ = [
    "ModelWeightStateRegistry",
    "model_weight_manifest_extra_evidence",
]
