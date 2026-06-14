from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Any, Iterable, Mapping

from ..offload.blocks import BlockState, OffloadBlock, OffloadBlockInfo
from ..offload.context import AdapterTransferContext, forbidden_physical_policy_keys
from ..offload.lifecycle import adapter_lifecycle_evidence_from_handles
from ..offload.stats import TransferStatsSnapshot
from ..offload.store import OffloadBatch, OffloadStore
from ..model_manifest import ModelWeightManifest, ModelWeightTensor
from ..schema import WorkloadKind

logger = logging.getLogger(__name__)


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
            metadata=_validate_model_loading_metadata(
                metadata,
                field_name="model loader metadata",
            ),
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
        _validate_model_loading_metadata(
            transfer_context.metadata,
            field_name="model loader context metadata",
        )
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
        _validate_manifest_metadata_no_physical_policy(resolved)
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
        return self._submit_selected(names, operation="submit_load_buckets")

    def load_tensor(self, name: str):
        return self.load_bucket(name)

    def load_tensors(self, names: Iterable[str]) -> list:
        return self.load_buckets(names)

    def submit_load_tensors(self, names: Iterable[str]) -> OffloadBatch:
        return self.submit_load_buckets(names)

    def load_manifest(self, names: Iterable[str] | None = None) -> list:
        selected = self._manifest_names(names)
        return self._load_selected(selected, operation="load_manifest")

    def submit_load_manifest(self, names: Iterable[str] | None = None) -> OffloadBatch:
        selected = self._manifest_names(names)
        return self._submit_selected(selected, operation="submit_load_manifest")

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

    def transfer_stats(self, names: Iterable[str]) -> TransferStatsSnapshot:
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
        batch = self.submit_prefetch_many(names)
        super().wait_many(names)
        self._record_load_lifecycle(
            operation=operation,
            names=names,
            handles=batch._handles,
        )
        return list(batch.handles)

    def _submit_selected(self, names: Iterable[str], *, operation: str) -> OffloadBatch:
        names = self._normalize_names(names)
        batch = self.submit_prefetch_many(names)
        self._record_load_lifecycle(
            operation=operation,
            names=names,
            handles=batch._handles,
        )
        return batch

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
        transfer_stats = self._raw_transfer_stats_many(names).as_dict()
        return adapter_lifecycle_evidence_from_handles(
            evidence_id=(
                f"model-load-{self.transfer_context.session_id}-"
                f"{self.transfer_context.intent_prefix}-{len(self._load_lifecycle_history) + 1}"
            ),
            operation=operation,
            transfer_context=self.transfer_context,
            item_field="tensor_names",
            item_count_field="tensor_count",
            item_names=names,
            handles=handles,
            transfer_stats=transfer_stats,
            runtime_session=self.client,
            extra={
                "adapter": "model_weight_loader",
                "load_direction": "h2d",
                "adapter_submit_source": "TurboBusRuntimeSession",
                "adapter_handle_source": "RuntimeSessionTransferHandle",
                "runtime_buffer_binding": self._runtime_buffer_binding_evidence(
                    evidence_source="TurboBusRuntimeSession.adapter_evidence_record",
                ),
                "tensor_bindings": self._tensor_binding_evidence(names),
                "bucket_ranges": self._bucket_range_evidence(names),
                "manifest_tensor_count": (
                    0 if self._manifest is None else len(self._manifest.tensors)
                ),
                "manifest_tensor_names": (
                    [] if self._manifest is None else self._manifest.names()
                ),
                "manifest_cpu_span_bytes": (
                    0 if self._manifest is None else self._manifest.cpu_span_bytes
                ),
                "manifest_gpu_span_bytes": (
                    0 if self._manifest is None else self._manifest.gpu_span_bytes
                ),
                "manifest_metadata": (
                    {} if self._manifest is None else dict(self._manifest.metadata)
                ),
            },
        )

    def _runtime_buffer_binding_evidence(
        self,
        *,
        evidence_source: str,
    ) -> dict[str, Any]:
        # /*
        #  * ========================================================================
        #  * 步骤1：生成 RuntimeSession buffer binding 摘要
        #  * ========================================================================
        #  * 数据源：AdapterTransferContext
        #  * 操作：
        #  *   1) 只记录 RuntimeSession buffer/context 结构绑定
        #  *   2) 不在 binding 内创建 route/receipt/plan 运行态证据
        #  */
        logger.info("开始生成 RuntimeSession buffer binding 摘要...")

        # // 1.1 返回 adapter buffer 结构绑定
        binding = {
            "evidence_source": str(evidence_source),
            "job_id": self.transfer_context.job_id,
            "session_id": self.transfer_context.session_id,
            "workload_kind": str(self.transfer_context.workload_kind.value),
            "cpu_buffer_id": self.transfer_context.cpu_buffer_id,
            "gpu_buffer_id": self.transfer_context.gpu_buffer_id,
            "intent_prefix": self.transfer_context.intent_prefix,
            "policy_hints": dict(self.transfer_context.policy_hints),
            "metadata": dict(self.transfer_context.metadata),
            "route_policy_visible_to_adapter": False,
        }
        logger.info("RuntimeSession buffer binding 摘要生成完成")
        return binding

    def _tensor_binding_evidence(self, names: Iterable[str]) -> list[dict[str, Any]]:
        bindings: list[dict[str, Any]] = []
        for name in names:
            block = self.block(name)
            tensor = None
            if self._manifest is not None:
                try:
                    tensor = self._manifest.tensor(name)
                except KeyError:
                    tensor = None
            record = {
                "name": str(name),
                "bucket_id": block.block_id,
                "cpu_slot": block.cpu_slot,
                "gpu_slot": block.gpu_slot,
                "cpu_offset": int(block.cpu_offset),
                "gpu_offset": int(block.gpu_offset),
                "byte_count": int(block.bytes),
            }
            if tensor is not None:
                record["tensor"] = tensor.as_dict()
            bindings.append(record)
        return bindings

    def _bucket_range_evidence(self, names: Iterable[str]) -> list[dict[str, Any]]:
        # /*
        #  * ========================================================================
        #  * 步骤2：生成结构化 bucket range 摘要
        #  * ========================================================================
        #  * 数据源：OffloadBlockInfo structural fields
        #  * 操作：
        #  *   1) 只描述 tensor bucket 的 range 结构
        #  *   2) 运行态 receipt/ticket/decision 只能来自外层 RuntimeSession evidence
        #  */
        logger.info("开始生成结构化 bucket range 摘要...")

        # // 2.1 构造不含 receipt 字段的 range 摘要
        ranges: list[dict[str, Any]] = []
        for name in names:
            info = self.block_info(name)
            ranges.append(
                {
                    "name": info.name,
                    "block_id": info.block_id,
                    "cpu_slot": info.cpu_slot,
                    "gpu_slot": info.gpu_slot,
                    "src_offset": int(info.cpu_offset),
                    "dst_offset": int(info.gpu_offset),
                    "bytes": int(info.bytes),
                    "state": info.state.value,
                    "last_operation": info.last_operation,
                    "runtime_evidence_source": (
                        "RuntimeSession.adapter_lifecycle_evidence"
                    ),
                    "route_policy_visible_to_adapter": False,
                }
            )
        logger.info("结构化 bucket range 摘要生成完成, count: %s", len(ranges))
        return ranges

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


def _validate_model_loading_metadata(
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


def _validate_manifest_metadata_no_physical_policy(
    manifest: ModelWeightManifest,
) -> None:
    _validate_model_loading_metadata(
        manifest.metadata,
        field_name="model weight manifest metadata",
    )
    for tensor in manifest.tensors:
        _validate_model_loading_metadata(
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
    "ModelWeightBucket",
    "ModelWeightLoadLifecycle",
    "ModelWeightManifest",
    "ModelWeightTensor",
    "ModelWeightLoader",
]
