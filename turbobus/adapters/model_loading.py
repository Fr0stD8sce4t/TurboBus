from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from ..offload.blocks import BlockState, OffloadBlock, OffloadBlockInfo
from ..offload.context import AdapterTransferContext
from ..offload.stats import TransferStats
from ..offload.store import OffloadBatch, OffloadStore
from ..model_manifest import ModelWeightManifest, ModelWeightTensor
from ..schema import WorkloadKind


@dataclass(frozen=True)
class ModelWeightBucket:
    """One runtime-session-bound model-weight bucket descriptor."""

    name: str
    bucket_id: object
    cpu_offset: int
    gpu_offset: int
    byte_count: int | None = None
    cpu_slot: object | None = None
    gpu_slot: object | None = None


class ModelWeightLoader(OffloadStore):
    """Runtime-session-owned model-weight loading API over daemon transfer intent."""

    def __init__(
        self,
        runtime_session,
        cpu_buffer,
        gpu_buffer,
        *,
        priority: int = 0,
        policy_hints: Mapping[str, object] | None = None,
        metadata: Mapping[str, object] | None = None,
        intent_prefix: str | None = None,
        wait_timeout_seconds: float | None = None,
        manifest: (
            ModelWeightManifest
            | Iterable[ModelWeightTensor | Mapping[str, object]]
            | None
        ) = None,
    ) -> None:
        context = runtime_session.make_adapter_transfer_context(
            cpu_buffer,
            gpu_buffer,
            workload_kind=WorkloadKind.MODEL_WEIGHTS,
            priority=priority,
            policy_hints=policy_hints,
            metadata=metadata,
            intent_prefix=intent_prefix,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        if not isinstance(context, AdapterTransferContext):
            raise TypeError(
                "runtime session adapter context factory must return an AdapterTransferContext"
            )
        self._init_from_transfer_context(
            runtime_session,
            context,
            cpu_buffer,
            gpu_buffer,
            manifest=manifest,
        )

    @classmethod
    def _from_transfer_context(
        cls,
        runtime_session,
        transfer_context: AdapterTransferContext,
        cpu_buffer,
        gpu_buffer,
        *,
        manifest: (
            ModelWeightManifest
            | Iterable[ModelWeightTensor | Mapping[str, object]]
            | None
        ) = None,
    ) -> "ModelWeightLoader":
        instance = cls.__new__(cls)
        instance._init_from_transfer_context(
            runtime_session,
            transfer_context,
            cpu_buffer,
            gpu_buffer,
            manifest=manifest,
        )
        return instance

    def _init_from_transfer_context(
        self,
        runtime_session,
        transfer_context: AdapterTransferContext,
        cpu_buffer,
        gpu_buffer,
        *,
        manifest: (
            ModelWeightManifest
            | Iterable[ModelWeightTensor | Mapping[str, object]]
            | None
        ) = None,
    ) -> None:
        super().__init__(runtime_session, transfer_context)
        self.cpu_buffer = cpu_buffer
        self.gpu_buffer = gpu_buffer
        self._manifest: ModelWeightManifest | None = None
        if manifest is not None:
            self.register_manifest(manifest)

    @property
    def manifest(self) -> ModelWeightManifest | None:
        return self._manifest

    def register_manifest(
        self,
        manifest: ModelWeightManifest | Iterable[ModelWeightTensor | Mapping[str, object]],
        *,
        replace: bool = False,
    ) -> list[OffloadBlock]:
        resolved = _coerce_manifest(manifest)
        if self._manifest is not None and not replace:
            raise ValueError("model weight manifest is already registered")
        _validate_manifest_backing_span(
            resolved,
            cpu_buffer=self.cpu_buffer,
            gpu_buffer=self.gpu_buffer,
        )
        if replace:
            for name in tuple(self.names()):
                self.remove(name)
        blocks: list[OffloadBlock] = []
        for tensor in resolved.tensors:
            blocks.append(self.register_tensor(tensor))
        self._manifest = resolved
        return blocks

    def register_tensor(
        self,
        tensor: ModelWeightTensor | Mapping[str, object],
    ) -> OffloadBlock:
        resolved = _coerce_tensor(tensor)
        return self.add_bucket(
            resolved.name,
            bucket_id=resolved.tensor_id,
            cpu_slot=resolved.name,
            gpu_slot=resolved.name,
            cpu_offset=resolved.cpu_offset,
            gpu_offset=int(resolved.gpu_offset),
            byte_count=resolved.byte_count,
        )

    def register_buckets(self, buckets: Iterable[ModelWeightBucket]) -> list[OffloadBlock]:
        registered: list[OffloadBlock] = []
        for bucket in buckets:
            registered.append(
                self.add_bucket(
                    bucket.name,
                    bucket_id=bucket.bucket_id,
                    cpu_slot=bucket.cpu_slot,
                    gpu_slot=bucket.gpu_slot,
                    cpu_offset=bucket.cpu_offset,
                    gpu_offset=bucket.gpu_offset,
                    byte_count=bucket.byte_count,
                )
            )
        return registered

    def add_bucket(
        self,
        name: str,
        *,
        bucket_id=None,
        cpu_slot=None,
        gpu_slot=None,
        cpu_offset: int = 0,
        gpu_offset: int = 0,
        byte_count: int | None = None,
    ) -> OffloadBlock:
        return self.add(
            name,
            self.cpu_buffer,
            self.gpu_buffer,
            block_id=name if bucket_id is None else bucket_id,
            cpu_slot=cpu_slot if cpu_slot is not None else bucket_id,
            gpu_slot=gpu_slot if gpu_slot is not None else bucket_id,
            cpu_offset=cpu_offset,
            gpu_offset=gpu_offset,
            byte_count=byte_count,
        )

    def add_packed_buckets(
        self,
        prefix: str,
        *,
        bucket_bytes: int,
        bucket_count: int,
        start_offset: int = 0,
    ) -> list[OffloadBlock]:
        if bucket_bytes <= 0:
            raise ValueError("bucket_bytes must be positive")
        if bucket_count <= 0:
            raise ValueError("bucket_count must be positive")
        if start_offset < 0:
            raise ValueError("start_offset must be non-negative")

        blocks = []
        for index in range(bucket_count):
            offset = start_offset + index * bucket_bytes
            blocks.append(
                self.add_bucket(
                    f"{prefix}{index}",
                    bucket_id=index,
                    cpu_offset=offset,
                    gpu_offset=offset,
                    byte_count=bucket_bytes,
                )
            )
        return blocks

    def bucket(self, name: str) -> OffloadBlock:
        return self.block(name)

    def bucket_info(self, name: str) -> OffloadBlockInfo:
        return self.block_info(name)

    def bucket_infos(self, names: Iterable[str] | None = None) -> list[OffloadBlockInfo]:
        return self.block_infos(names)

    def tensor(self, name: str) -> OffloadBlock:
        return self.bucket(name)

    def tensor_info(self, name: str) -> OffloadBlockInfo:
        return self.bucket_info(name)

    def tensor_infos(self, names: Iterable[str] | None = None) -> list[OffloadBlockInfo]:
        return self.bucket_infos(names)

    def load_bucket(self, name: str):
        return self.prefetch(name)

    def load_buckets(self, names: Iterable[str]) -> list:
        return self.prefetch_many(names)

    def submit_load_buckets(self, names: Iterable[str]) -> OffloadBatch:
        return self.submit_prefetch_many(names)

    def load_tensor(self, name: str):
        return self.load_bucket(name)

    def load_tensors(self, names: Iterable[str]) -> list:
        return self.load_buckets(names)

    def submit_load_tensors(self, names: Iterable[str]) -> OffloadBatch:
        return self.submit_load_buckets(names)

    def load_manifest(self, names: Iterable[str] | None = None) -> list:
        selected = self._manifest_names(names)
        batch = self.submit_load_tensors(selected)
        self.wait_many(selected)
        return list(batch.handles)

    def load_batch(self, names: Iterable[str]) -> OffloadBatch:
        return self.submit_load_buckets(names)

    def load_prefix(self, names: Iterable[str]) -> list:
        names = list(names)
        batch = self.submit_load_buckets(names)
        self.wait_many(names)
        return list(batch.handles)

    def load_all(self) -> list:
        return self.prefetch_many(self.names())

    def wait_all(self) -> None:
        self.wait_many(self.names())

    def transfer_stats(self, names: Iterable[str]) -> TransferStats:
        return self.transfer_stats_many(names)

    def mark_unloaded(self, names: Iterable[str] | None = None) -> None:
        selected = self.names() if names is None else list(names)
        for name in selected:
            self.set_block_state(name, BlockState.CPU, clear_transfer_state=True)

    def _manifest_names(self, names: Iterable[str] | None) -> list[str]:
        if names is not None:
            return list(names)
        if self._manifest is not None:
            return self._manifest.names()
        return self.names()


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
    "ModelWeightBucket",
    "ModelWeightManifest",
    "ModelWeightTensor",
    "ModelWeightLoader",
]
