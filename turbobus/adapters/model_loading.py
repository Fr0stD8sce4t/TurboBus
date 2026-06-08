from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from ..offload.blocks import BlockState, OffloadBlock, OffloadBlockInfo
from ..offload.context import AdapterTransferContext
from ..offload.stats import TransferStats
from ..offload.store import OffloadBatch, OffloadStore
from ..model_manifest import ModelWeightManifest, ModelWeightTensor
from ..schema import TransferReceipt, WorkloadKind


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


@dataclass(frozen=True)
class ModelWeightLoadLifecycle:
    operation: str
    names: tuple[str, ...]
    evidence: dict[str, Any] = field(default_factory=dict)


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
        self._load_lifecycle_history: list[ModelWeightLoadLifecycle] = []
        self._last_load_lifecycle: ModelWeightLoadLifecycle | None = None
        if manifest is not None:
            self.register_manifest(manifest)

    @property
    def manifest(self) -> ModelWeightManifest | None:
        return self._manifest

    @property
    def last_load_lifecycle(self) -> ModelWeightLoadLifecycle | None:
        return self._last_load_lifecycle

    @property
    def load_lifecycle_history(self) -> tuple[ModelWeightLoadLifecycle, ...]:
        return tuple(self._load_lifecycle_history)

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
        handles = self._load_selected([name], operation="load_bucket")
        return handles[0]

    def load_buckets(self, names: Iterable[str]) -> list:
        return self._load_selected(names, operation="load_buckets")

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
        return self._load_selected(selected, operation="load_manifest")

    def load_batch(self, names: Iterable[str]) -> OffloadBatch:
        return self.submit_load_buckets(names)

    def load_prefix(self, names: Iterable[str]) -> list:
        return self._load_selected(names, operation="load_prefix")

    def load_all(self) -> list:
        return self._load_selected(self.names(), operation="load_all")

    def wait_all(self) -> None:
        self.wait_many(self.names())

    def wait_many(self, names: Iterable[str]) -> None:
        names = self._normalize_names(names)
        super().wait_many(names)
        self._record_load_lifecycle(
            operation="wait_many",
            names=names,
            handles=self._handles_for_names(names),
        )

    def transfer_stats(self, names: Iterable[str]) -> TransferStats:
        return self.transfer_stats_many(names)

    def mark_unloaded(self, names: Iterable[str] | None = None) -> None:
        selected = self.names() if names is None else list(names)
        for name in selected:
            self.set_block_state(name, BlockState.CPU, clear_transfer_state=True)

    def _manifest_names(self, names: Iterable[str] | None) -> list[str]:
        if names is not None:
            return self._normalize_names(names)
        if self._manifest is not None:
            return self._manifest.names()
        return self.names()

    def _load_selected(self, names: Iterable[str], *, operation: str) -> list:
        names = self._normalize_names(names)
        if not names:
            return []
        batch = self.submit_load_tensors(names)
        super().wait_many(names)
        self._record_load_lifecycle(
            operation=operation,
            names=names,
            handles=batch.handles,
        )
        return list(batch.handles)

    def _record_load_lifecycle(
        self,
        *,
        operation: str,
        names: list[str],
        handles: Iterable[object],
    ) -> None:
        handles = list(handles)
        if not handles:
            return
        lifecycle = ModelWeightLoadLifecycle(
            operation=str(operation),
            names=tuple(names),
            evidence=self._load_lifecycle_evidence(
                operation=operation,
                names=names,
                handles=handles,
            ),
        )
        self._last_load_lifecycle = lifecycle
        self._load_lifecycle_history.append(lifecycle)

    def _load_lifecycle_evidence(
        self,
        *,
        operation: str,
        names: list[str],
        handles: Iterable[object],
    ) -> dict[str, Any]:
        receipts = _unique_receipts_from_handles(handles)
        if names and not receipts:
            raise RuntimeError("model weight load completed without TransferReceipt evidence")
        transfer_stats = self.transfer_stats_many(names).as_dict()
        return {
            "evidence_id": (
                f"model-load-{self.transfer_context.session_id}-"
                f"{self.transfer_context.intent_prefix}-{len(self._load_lifecycle_history) + 1}"
            ),
            "operation": str(operation),
            "job_id": self.transfer_context.job_id,
            "session_id": self.transfer_context.session_id,
            "workload_kind": str(self.transfer_context.workload_kind.value),
            "buffer_registration_source": "TurboBusRuntimeSession",
            "intent_source": "TransferIntent",
            "receipt_source": "TransferReceipt",
            "policy_source": "daemon_scheduler",
            "cpu_buffer_id": self.transfer_context.cpu_buffer_id,
            "gpu_buffer_id": self.transfer_context.gpu_buffer_id,
            "tensor_names": tuple(names),
            "tensor_count": len(names),
            "manifest_tensor_count": (
                0 if self._manifest is None else len(self._manifest.tensors)
            ),
            "manifest_cpu_span_bytes": (
                0 if self._manifest is None else self._manifest.cpu_span_bytes
            ),
            "manifest_gpu_span_bytes": (
                0 if self._manifest is None else self._manifest.gpu_span_bytes
            ),
            "receipt_count": len(receipts),
            "intent_ids": _join_unique(
                getattr(getattr(handle, "intent", None), "intent_id", None)
                for handle in handles
            ),
            "receipt_ids": _join_unique(receipt.receipt_id for receipt in receipts),
            "ticket_ids": _join_unique(receipt.ticket_id for receipt in receipts),
            "decision_ids": _join_unique(receipt.decision_id for receipt in receipts),
            "topology_snapshot_ids": _join_unique(
                receipt.topology_snapshot_id for receipt in receipts
            ),
            "transfer_ids": _join_unique(
                receipt.metadata.get("transfer_id") for receipt in receipts
            ),
            "receipt_states": _join_unique(
                getattr(receipt.state, "value", str(receipt.state))
                for receipt in receipts
            ),
            "completion_sources": _join_unique(
                receipt.metadata.get("completion_source") for receipt in receipts
            ),
            **transfer_stats,
        }

    def _handles_for_names(self, names: Iterable[str]) -> list[object]:
        handles = []
        seen = set()
        for name in names:
            handle = self.block(name).last_handle
            if handle is None or id(handle) in seen:
                continue
            seen.add(id(handle))
            handles.append(handle)
        return handles

    @staticmethod
    def _normalize_names(names: Iterable[str]) -> list[str]:
        return [str(name) for name in names]


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


def _unique_receipts_from_handles(handles: Iterable[object]) -> list[TransferReceipt]:
    receipts: list[TransferReceipt] = []
    seen = set()
    for handle in handles:
        receipt = getattr(handle, "receipt", None)
        if not isinstance(receipt, TransferReceipt):
            continue
        if receipt.receipt_id in seen:
            continue
        seen.add(receipt.receipt_id)
        receipts.append(receipt)
    return receipts


def _join_unique(values: Iterable[object]) -> str:
    seen = set()
    ordered = []
    for value in values:
        if value is None:
            continue
        text = str(value)
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ",".join(ordered)


__all__ = [
    "ModelWeightBucket",
    "ModelWeightLoadLifecycle",
    "ModelWeightManifest",
    "ModelWeightTensor",
    "ModelWeightLoader",
]
