from __future__ import annotations

import json
import struct
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ModelWeightTensor:
    name: str
    dtype: str
    shape: tuple[int, ...]
    byte_count: int
    cpu_offset: int
    gpu_offset: int | None = None
    tensor_id: object | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        name = _require_non_empty(self.name, "name")
        dtype = _require_non_empty(self.dtype, "dtype")
        shape = tuple(int(dim) for dim in self.shape)
        if any(dim < 0 for dim in shape):
            raise ValueError("tensor shape dimensions must be non-negative")
        byte_count = int(self.byte_count)
        if byte_count <= 0:
            raise ValueError("tensor byte_count must be positive")
        cpu_offset = int(self.cpu_offset)
        if cpu_offset < 0:
            raise ValueError("tensor cpu_offset must be non-negative")
        gpu_offset = cpu_offset if self.gpu_offset is None else int(self.gpu_offset)
        if gpu_offset < 0:
            raise ValueError("tensor gpu_offset must be non-negative")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "dtype", dtype)
        object.__setattr__(self, "shape", shape)
        object.__setattr__(self, "byte_count", byte_count)
        object.__setattr__(self, "cpu_offset", cpu_offset)
        object.__setattr__(self, "gpu_offset", gpu_offset)
        object.__setattr__(self, "metadata", dict(self.metadata))

    @property
    def cpu_end_offset(self) -> int:
        return self.cpu_offset + self.byte_count

    @property
    def gpu_end_offset(self) -> int:
        return int(self.gpu_offset) + self.byte_count

    def as_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "dtype": self.dtype,
            "shape": list(self.shape),
            "byte_count": self.byte_count,
            "cpu_offset": self.cpu_offset,
            "gpu_offset": self.gpu_offset,
            "tensor_id": self.tensor_id,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class ModelWeightManifest:
    tensors: tuple[ModelWeightTensor, ...]
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        tensors = tuple(_coerce_tensor(item) for item in self.tensors)
        seen: set[str] = set()
        for tensor in tensors:
            if tensor.name in seen:
                raise ValueError(f"duplicate tensor in manifest: {tensor.name}")
            seen.add(tensor.name)
        object.__setattr__(self, "tensors", tensors)
        object.__setattr__(self, "metadata", dict(self.metadata))

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ModelWeightManifest":
        if not isinstance(payload, Mapping):
            raise TypeError("model weight manifest payload must be a mapping")
        tensors = payload.get("tensors")
        if not isinstance(tensors, Iterable):
            raise ValueError("model weight manifest payload requires tensors")
        metadata = payload.get("metadata", {})
        if not isinstance(metadata, Mapping):
            raise ValueError("model weight manifest metadata must be a mapping")
        return cls(
            tuple(_coerce_tensor(item) for item in tensors),
            metadata=dict(metadata),
        )

    @classmethod
    def from_json_file(cls, path: str | Path) -> "ModelWeightManifest":
        with Path(path).open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return cls.from_dict(payload)

    @classmethod
    def from_tensor_specs(
        cls,
        specs: Iterable[ModelWeightTensor | Mapping[str, Any]],
        *,
        cpu_base_offset: int = 0,
        gpu_base_offset: int = 0,
        alignment_bytes: int = 1,
        metadata: Mapping[str, Any] | None = None,
    ) -> "ModelWeightManifest":
        alignment = _normalize_alignment(alignment_bytes)
        cpu_cursor = int(cpu_base_offset)
        gpu_cursor = int(gpu_base_offset)
        if cpu_cursor < 0 or gpu_cursor < 0:
            raise ValueError("base offsets must be non-negative")
        tensors: list[ModelWeightTensor] = []
        for item in specs:
            if isinstance(item, ModelWeightTensor):
                tensors.append(item)
                cpu_cursor = max(cpu_cursor, item.cpu_end_offset)
                gpu_cursor = max(gpu_cursor, item.gpu_end_offset)
                continue
            if not isinstance(item, Mapping):
                raise TypeError("tensor specs must be mappings or ModelWeightTensor")
            cpu_offset = item.get("cpu_offset")
            gpu_offset = item.get("gpu_offset")
            if cpu_offset is None:
                cpu_cursor = _align_up(cpu_cursor, alignment)
                cpu_offset = cpu_cursor
            if gpu_offset is None:
                gpu_cursor = _align_up(gpu_cursor, alignment)
                gpu_offset = gpu_cursor
            tensor = ModelWeightTensor(
                name=str(item["name"]),
                dtype=str(item["dtype"]),
                shape=tuple(item.get("shape", ())),
                byte_count=int(item["byte_count"]),
                cpu_offset=int(cpu_offset),
                gpu_offset=int(gpu_offset),
                tensor_id=item.get("tensor_id", item.get("name")),
                metadata=dict(item.get("metadata", {})),
            )
            tensors.append(tensor)
            cpu_cursor = tensor.cpu_end_offset
            gpu_cursor = tensor.gpu_end_offset
        return cls(tuple(tensors), metadata={} if metadata is None else metadata)

    @classmethod
    def from_torch_state_dict(
        cls,
        state_dict: Mapping[str, Any],
        *,
        cpu_base_offset: int = 0,
        gpu_base_offset: int = 0,
        alignment_bytes: int = 1,
        metadata: Mapping[str, Any] | None = None,
    ) -> "ModelWeightManifest":
        specs: list[dict[str, object]] = []
        for name, tensor in state_dict.items():
            specs.append(
                {
                    "name": str(name),
                    "dtype": _torch_dtype_name(tensor),
                    "shape": tuple(int(dim) for dim in getattr(tensor, "shape")),
                    "byte_count": _tensor_nbytes(tensor),
                    "tensor_id": str(name),
                }
            )
        manifest_metadata = {"source": "torch_state_dict"}
        if metadata is not None:
            manifest_metadata.update(dict(metadata))
        return cls.from_tensor_specs(
            specs,
            cpu_base_offset=cpu_base_offset,
            gpu_base_offset=gpu_base_offset,
            alignment_bytes=alignment_bytes,
            metadata=manifest_metadata,
        )

    @classmethod
    def from_safetensors_file(
        cls,
        path: str | Path,
        *,
        cpu_data_base_offset: int | None = None,
        gpu_data_base_offset: int = 0,
        metadata: Mapping[str, Any] | None = None,
    ) -> "ModelWeightManifest":
        safetensors_path = Path(path)
        with safetensors_path.open("rb") as handle:
            header_size_raw = handle.read(8)
            if len(header_size_raw) != 8:
                raise ValueError("safetensors file is missing header size")
            header_size = struct.unpack("<Q", header_size_raw)[0]
            header_raw = handle.read(header_size)
            if len(header_raw) != header_size:
                raise ValueError("safetensors file header is truncated")
        header = json.loads(header_raw.decode("utf-8"))
        data_base = 8 + int(header_size) if cpu_data_base_offset is None else int(cpu_data_base_offset)
        manifest_metadata = {
            "source": "safetensors",
            "path": str(safetensors_path),
            "header_bytes": int(header_size),
        }
        if metadata is not None:
            manifest_metadata.update(dict(metadata))
        return cls.from_safetensors_header(
            header,
            cpu_data_base_offset=data_base,
            gpu_data_base_offset=gpu_data_base_offset,
            metadata=manifest_metadata,
        )

    @classmethod
    def from_safetensors_header(
        cls,
        header: Mapping[str, Any],
        *,
        cpu_data_base_offset: int = 0,
        gpu_data_base_offset: int = 0,
        metadata: Mapping[str, Any] | None = None,
    ) -> "ModelWeightManifest":
        cpu_base = int(cpu_data_base_offset)
        gpu_base = int(gpu_data_base_offset)
        if cpu_base < 0 or gpu_base < 0:
            raise ValueError("safetensors base offsets must be non-negative")
        tensors: list[ModelWeightTensor] = []
        for name, entry in header.items():
            if name == "__metadata__":
                continue
            if not isinstance(entry, Mapping):
                raise ValueError(f"safetensors entry must be a mapping: {name}")
            offsets = entry.get("data_offsets")
            if not isinstance(offsets, list | tuple) or len(offsets) != 2:
                raise ValueError(f"safetensors entry missing data_offsets: {name}")
            start = int(offsets[0])
            end = int(offsets[1])
            if start < 0 or end <= start:
                raise ValueError(f"invalid safetensors data_offsets: {name}")
            tensors.append(
                ModelWeightTensor(
                    name=str(name),
                    dtype=str(entry["dtype"]),
                    shape=tuple(int(dim) for dim in entry.get("shape", ())),
                    byte_count=end - start,
                    cpu_offset=cpu_base + start,
                    gpu_offset=gpu_base + start,
                    tensor_id=str(name),
                    metadata={"format": "safetensors"},
                )
            )
        manifest_metadata = {"source": "safetensors_header"}
        if "__metadata__" in header and isinstance(header["__metadata__"], Mapping):
            manifest_metadata["safetensors_metadata"] = dict(header["__metadata__"])
        if metadata is not None:
            manifest_metadata.update(dict(metadata))
        return cls(tuple(tensors), metadata=manifest_metadata)

    def names(self) -> list[str]:
        return [tensor.name for tensor in self.tensors]

    def tensor(self, name: str) -> ModelWeightTensor:
        for tensor in self.tensors:
            if tensor.name == name:
                return tensor
        raise KeyError(f"unknown model tensor: {name}")

    def select(self, names: Iterable[str] | None = None) -> list[ModelWeightTensor]:
        if names is None:
            return list(self.tensors)
        return [self.tensor(name) for name in names]

    @property
    def cpu_span_bytes(self) -> int:
        if not self.tensors:
            return 0
        return max(tensor.cpu_end_offset for tensor in self.tensors)

    @property
    def gpu_span_bytes(self) -> int:
        if not self.tensors:
            return 0
        return max(tensor.gpu_end_offset for tensor in self.tensors)

    def as_dict(self) -> dict[str, object]:
        return {
            "tensors": [tensor.as_dict() for tensor in self.tensors],
            "metadata": dict(self.metadata),
            "cpu_span_bytes": self.cpu_span_bytes,
            "gpu_span_bytes": self.gpu_span_bytes,
        }

    def to_json_file(self, path: str | Path) -> None:
        with Path(path).open("w", encoding="utf-8") as handle:
            json.dump(self.as_dict(), handle, indent=2, sort_keys=True)
            handle.write("\n")


def _coerce_tensor(value: ModelWeightTensor | Mapping[str, Any]) -> ModelWeightTensor:
    if isinstance(value, ModelWeightTensor):
        return value
    if isinstance(value, Mapping):
        return ModelWeightTensor(
            name=str(value["name"]),
            dtype=str(value["dtype"]),
            shape=tuple(value.get("shape", ())),
            byte_count=int(value["byte_count"]),
            cpu_offset=int(value["cpu_offset"]),
            gpu_offset=(
                None if value.get("gpu_offset") is None else int(value["gpu_offset"])
            ),
            tensor_id=value.get("tensor_id", value.get("name")),
            metadata=dict(value.get("metadata", {})),
        )
    raise TypeError("manifest tensors must be ModelWeightTensor or mappings")


def _tensor_nbytes(tensor: Any) -> int:
    nbytes = getattr(tensor, "nbytes", None)
    if nbytes is not None:
        return int(nbytes)
    numel = getattr(tensor, "numel", None)
    element_size = getattr(tensor, "element_size", None)
    if callable(numel) and callable(element_size):
        return int(numel() * element_size())
    raise TypeError("torch state_dict values must expose nbytes or numel/element_size")


def _torch_dtype_name(tensor: Any) -> str:
    dtype = getattr(tensor, "dtype", None)
    if dtype is None:
        raise TypeError("torch state_dict values must expose dtype")
    return str(dtype).removeprefix("torch.")


def _normalize_alignment(alignment_bytes: int) -> int:
    alignment = int(alignment_bytes)
    if alignment <= 0:
        raise ValueError("alignment_bytes must be positive")
    return alignment


def _align_up(value: int, alignment: int) -> int:
    remainder = value % alignment
    if remainder == 0:
        return value
    return value + alignment - remainder


def _require_non_empty(value: object, field_name: str) -> str:
    normalized = str(value)
    if not normalized.strip():
        raise ValueError(f"{field_name} must be non-empty")
    return normalized


__all__ = [
    "ModelWeightManifest",
    "ModelWeightTensor",
]
