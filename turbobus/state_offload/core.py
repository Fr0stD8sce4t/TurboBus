from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Any, Callable, Iterable, Mapping

from ..offload.blocks import BlockState, OffloadBlock, OffloadBlockInfo
from ..offload.lifecycle import transfer_lifecycle_evidence_from_handles
from ..offload.stats import TransferStatsSnapshot
from ..offload.store import OffloadBatch, OffloadStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StateDescriptor:
    name: str
    state_id: object
    cpu_tensor: object
    gpu_tensor: object
    cpu_offset: int = 0
    gpu_offset: int = 0
    byte_count: int | None = None
    cpu_slot: object | None = None
    gpu_slot: object | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class StateOffloadLifecycle:
    operation: str
    names: tuple[str, ...]
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StateOffloadSpec:
    state_kind: str
    evidence_prefix: str
    item_field: str = "state_names"
    item_count_field: str = "state_count"
    binding_field: str = "state_bindings"
    range_field: str = "state_ranges"
    lifecycle_type: type = StateOffloadLifecycle
    metadata_validator: Callable[..., dict[str, object]] | None = None
    metadata_field_name: str = "state offload metadata"
    extra_evidence: Callable[["StateOffloadCore", str, Iterable[str]], dict[str, Any]] | None = None

    def validate_metadata(self, metadata: Mapping[str, object] | None) -> dict[str, object]:
        if self.metadata_validator is None:
            return {} if metadata is None else dict(metadata)
        return self.metadata_validator(
            metadata,
            field_name=self.metadata_field_name,
        )


class StateOffloadCore(OffloadStore):
    def __init__(
        self,
        runtime_session,
        transfer_context,
        spec: StateOffloadSpec,
    ) -> None:
        # /*
        #  * ========================================================================
        #  * 步骤1：初始化 state offload core
        #  * ========================================================================
        #  * 目标对象：RuntimeSession-bound OffloadStore
        #  * 操作：
        #  *   1) 校验 spec 与 transfer context metadata
        #  *   2) 初始化统一 lifecycle 记录
        #  */
        logger.info("开始初始化 state offload core...")

        # // 1.1 校验 spec 类型和 metadata 边界
        if not isinstance(spec, StateOffloadSpec):
            raise TypeError("spec must be a StateOffloadSpec")
        spec.validate_metadata(transfer_context.metadata)

        # // 1.2 初始化底层 OffloadStore
        super().__init__(runtime_session, transfer_context)
        self.runtime_session = runtime_session
        self.transfer_context = transfer_context
        self.state_registry = None
        self.spec = spec
        self._transfer_lifecycle_history: list[StateOffloadLifecycle] = []
        self._last_transfer_lifecycle: StateOffloadLifecycle | None = None
        logger.info("state offload core 初始化完成, state_kind: %s", spec.state_kind)

    @property
    def last_transfer_lifecycle(self) -> StateOffloadLifecycle | None:
        return self._last_transfer_lifecycle

    @property
    def transfer_lifecycle_history(self) -> tuple[StateOffloadLifecycle, ...]:
        return tuple(self._transfer_lifecycle_history)

    def register_states(
        self,
        states: Iterable[StateDescriptor],
        *,
        replace: bool = False,
    ) -> list[OffloadBlock]:
        # /*
        #  * ========================================================================
        #  * 步骤2：注册 state descriptors
        #  * ========================================================================
        #  * 数据源：StateDescriptor 序列
        #  * 操作：
        #  *   1) 已存在 state 默认幂等跳过
        #  *   2) replace=True 时删除旧 block 后重建
        #  */
        logger.info("开始注册 state descriptors...")

        # // 2.1 逐个 descriptor 绑定 CPU/GPU backing
        registered: list[OffloadBlock] = []
        known_names = set(self.names())
        for descriptor in states:
            name = str(descriptor.name)
            if name in known_names:
                if not replace:
                    continue
                self.remove(name)
            registered.append(self.add_state(descriptor))
            known_names.add(name)

        logger.info("state descriptors 注册完成, count: %s", len(registered))
        return registered

    def register_registry(self, registry, *, replace: bool = False) -> list[OffloadBlock]:
        # /*
        #  * ========================================================================
        #  * 步骤2：从 registry 注册 state
        #  * ========================================================================
        #  * 数据源：StateRegistry.rebuild()
        #  * 操作：
        #  *   1) 由 registry 发现最新 state
        #  *   2) 交给统一 register_states 处理幂等和 replace
        #  */
        logger.info("开始从 registry 注册 state...")

        # // 2.1 从 registry 重建 descriptor
        states = registry.rebuild()
        self.state_registry = registry

        # // 2.2 交给统一注册入口
        registered = self.register_states(states, replace=replace)
        logger.info("registry state 注册完成, count: %s", len(registered))
        return registered

    def add_state(self, descriptor: StateDescriptor) -> OffloadBlock:
        # /*
        #  * ========================================================================
        #  * 步骤3：添加单个 state block
        #  * ========================================================================
        #  * 数据源：StateDescriptor
        #  * 操作：
        #  *   1) 将 state 映射为 OffloadStore block
        #  *   2) 保留稳定 state_id、slot、range 字段
        #  */
        logger.info("开始添加 state block, name: %s", descriptor.name)

        # // 3.1 写入 OffloadStore block 表
        block = self.add(
            descriptor.name,
            descriptor.cpu_tensor,
            descriptor.gpu_tensor,
            block_id=descriptor.state_id,
            cpu_slot=descriptor.cpu_slot
            if descriptor.cpu_slot is not None
            else descriptor.state_id,
            gpu_slot=descriptor.gpu_slot
            if descriptor.gpu_slot is not None
            else descriptor.state_id,
            cpu_offset=descriptor.cpu_offset,
            gpu_offset=descriptor.gpu_offset,
            byte_count=descriptor.byte_count,
        )
        logger.info("state block 添加完成, name: %s", descriptor.name)
        return block

    def state(self, name: str) -> OffloadBlock:
        return self.block(name)

    def state_info(self, name: str) -> OffloadBlockInfo:
        return self.block_info(name)

    def state_infos(self, names: Iterable[str] | None = None) -> list[OffloadBlockInfo]:
        return self.block_infos(names)

    def prefetch_state(self, name: str) -> OffloadBatch:
        batch = self.submit_prefetch_states([name], operation="prefetch_state")
        batch.wait()
        return batch

    def prefetch_states(self, names: Iterable[str]) -> OffloadBatch:
        batch = self.submit_prefetch_states(names, operation="prefetch_states")
        batch.wait()
        return batch

    def submit_prefetch_states(
        self,
        names: Iterable[str],
        *,
        operation: str = "submit_prefetch_states",
    ) -> OffloadBatch:
        return self._submit_selected(
            names,
            operation=operation,
            submitter=self.submit_prefetch_many,
        )

    def prefetch_all(self) -> OffloadBatch:
        batch = self.submit_prefetch_states(self.names(), operation="prefetch_all")
        batch.wait()
        return batch

    def offload_state(self, name: str) -> OffloadBatch:
        batch = self.submit_offload_states([name], operation="offload_state")
        batch.wait()
        return batch

    def offload_states(self, names: Iterable[str]) -> OffloadBatch:
        batch = self.submit_offload_states(names, operation="offload_states")
        batch.wait()
        return batch

    def submit_offload_states(
        self,
        names: Iterable[str],
        *,
        operation: str = "submit_offload_states",
    ) -> OffloadBatch:
        return self._submit_selected(
            names,
            operation=operation,
            submitter=self.submit_evict_many,
        )

    def offload_all(self) -> OffloadBatch:
        batch = self.submit_offload_states(self.names(), operation="offload_all")
        batch.wait()
        return batch

    def wait_all(self) -> None:
        self.wait_many(self.names())

    def wait_many(self, names: Iterable[str]) -> None:
        # /*
        #  * ========================================================================
        #  * 步骤4：等待 state transfer 并记录 lifecycle
        #  * ========================================================================
        #  * 数据源：OffloadStore block handle
        #  * 操作：
        #  *   1) 等待底层 transfer receipt
        #  *   2) 写入 RuntimeSession-bound lifecycle evidence
        #  */
        logger.info("开始等待 state transfer...")

        # // 4.1 等待底层 transfer 完成
        selected = self._normalize_names(names)
        super().wait_many(selected)

        # // 4.2 记录 wait lifecycle evidence
        self._record_transfer_lifecycle(
            operation="wait_many",
            names=selected,
            handles=self._handles_for_names(selected),
        )
        logger.info("state transfer 等待完成, count: %s", len(selected))

    def transfer_stats(self, names: Iterable[str]) -> TransferStatsSnapshot:
        return self.transfer_stats_many(names)

    def mark_on_cpu(self, names: Iterable[str] | None = None) -> None:
        selected = self.names() if names is None else list(names)
        for name in selected:
            self.set_block_state(name, BlockState.CPU, clear_transfer_state=True)

    def mark_on_gpu(self, names: Iterable[str] | None = None) -> None:
        selected = self.names() if names is None else list(names)
        for name in selected:
            self.set_block_state(name, BlockState.GPU, clear_transfer_state=True)

    def _submit_selected(
        self,
        names: Iterable[str],
        *,
        operation: str,
        submitter,
    ) -> OffloadBatch:
        # /*
        #  * ========================================================================
        #  * 步骤5：提交批量 state transfer
        #  * ========================================================================
        #  * 数据源：state name 集合
        #  * 操作：
        #  *   1) 空集合返回 empty batch
        #  *   2) 非空集合提交到底层 OffloadStore
        #  */
        logger.info("开始提交 state transfer, operation: %s", operation)

        # // 5.1 规范化 state name
        selected = self._normalize_names(names)
        if not selected:
            batch = OffloadBatch(operation, (), (), self)
            logger.info("state transfer 提交完成, empty: true")
            return batch

        # // 5.2 提交 transfer 并记录 lifecycle evidence
        batch = submitter(selected)
        self._record_transfer_lifecycle(
            operation=operation,
            names=selected,
            handles=batch.receipt_handles,
        )
        logger.info("state transfer 提交完成, count: %s", len(selected))
        return batch

    def _record_transfer_lifecycle(
        self,
        *,
        operation: str,
        names: list[str],
        handles: Iterable[object],
    ) -> None:
        handles = list(handles)
        if not handles:
            return
        lifecycle = self.spec.lifecycle_type(
            operation=str(operation),
            names=tuple(names),
            evidence=self._transfer_lifecycle_evidence(
                operation=operation,
                names=names,
                handles=handles,
            ),
        )
        self._last_transfer_lifecycle = lifecycle
        self._transfer_lifecycle_history.append(lifecycle)

    def _transfer_lifecycle_evidence(
        self,
        *,
        operation: str,
        names: list[str],
        handles: Iterable[object],
    ) -> dict[str, Any]:
        transfer_stats = self._raw_transfer_stats_many(names).as_dict()
        return transfer_lifecycle_evidence_from_handles(
            evidence_id=(
                f"{self.spec.evidence_prefix}-{self.transfer_context.session_id}-"
                f"{self.transfer_context.intent_prefix}-"
                f"{len(self._transfer_lifecycle_history) + 1}"
            ),
            operation=operation,
            transfer_context=self.transfer_context,
            item_field=self.spec.item_field,
            item_count_field=self.spec.item_count_field,
            item_names=names,
            handles=handles,
            transfer_stats=transfer_stats,
            runtime_session=self.client,
            extra=self._lifecycle_extra(operation=operation, names=names),
        )

    def _lifecycle_extra(
        self,
        *,
        operation: str,
        names: Iterable[str],
    ) -> dict[str, Any]:
        extra = {
            "state_kind": self.spec.state_kind,
            "operation_direction": self._operation_direction(operation, names),
            "state_submit_source": "TurboBusRuntimeSession",
            "state_handle_source": "RuntimeSessionTransferHandle",
            "runtime_buffer_binding": self._runtime_buffer_binding_evidence(
                evidence_source="TurboBusRuntimeSession.state_evidence_record",
            ),
            self.spec.binding_field: self._state_binding_evidence(names),
            self.spec.range_field: self._state_range_evidence(names),
        }
        if self.spec.extra_evidence is not None:
            extra.update(self.spec.extra_evidence(self, operation, names))
        return extra

    def _runtime_buffer_binding_evidence(
        self,
        *,
        evidence_source: str,
    ) -> dict[str, Any]:
        logger.info("开始生成 RuntimeSession buffer binding 摘要...")
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
            "route_policy_visible_to_transfer": False,
        }
        logger.info("RuntimeSession buffer binding 摘要生成完成")
        return binding

    def _state_binding_evidence(self, names: Iterable[str]) -> list[dict[str, Any]]:
        bindings: list[dict[str, Any]] = []
        for name in names:
            block = self.block(name)
            bindings.append(
                {
                    "name": str(name),
                    "state_id": block.block_id,
                    "bucket_id": block.block_id,
                    "cpu_slot": block.cpu_slot,
                    "gpu_slot": block.gpu_slot,
                    "cpu_offset": int(block.cpu_offset),
                    "gpu_offset": int(block.gpu_offset),
                    "byte_count": int(block.bytes),
                    "last_operation": block.last_operation,
                    "last_direction": self._block_direction(block.last_operation),
                }
            )
        return bindings

    def _state_range_evidence(self, names: Iterable[str]) -> list[dict[str, Any]]:
        logger.info("开始生成结构化 state range 摘要...")
        ranges: list[dict[str, Any]] = []
        for name in names:
            info = self.block_info(name)
            direction = self._block_direction(info.last_operation)
            if direction == "d2h":
                src_offset = int(info.gpu_offset)
                dst_offset = int(info.cpu_offset)
            else:
                src_offset = int(info.cpu_offset)
                dst_offset = int(info.gpu_offset)
            ranges.append(
                {
                    "name": info.name,
                    "state_id": info.block_id,
                    "bucket_id": info.block_id,
                    "cpu_slot": info.cpu_slot,
                    "gpu_slot": info.gpu_slot,
                    "direction": direction,
                    "src_offset": src_offset,
                    "dst_offset": dst_offset,
                    "cpu_offset": int(info.cpu_offset),
                    "gpu_offset": int(info.gpu_offset),
                    "bytes": int(info.bytes),
                    "state": info.state.value,
                    "last_operation": info.last_operation,
                    "runtime_evidence_source": (
                        "TurboBusRuntimeSession.state_lifecycle_evidence"
                    ),
                    "route_policy_visible_to_state": False,
                }
            )
        logger.info("结构化 state range 摘要生成完成, count: %s", len(ranges))
        return ranges

    def _handles_for_names(self, names: Iterable[str]) -> list[object]:
        handles = []
        seen = set()
        for name in names:
            handle = self._blocks[str(name)]._last_handle
            if handle is None or id(handle) in seen:
                continue
            seen.add(id(handle))
            handles.append(handle)
        return handles

    @staticmethod
    def _normalize_names(names: Iterable[str]) -> list[str]:
        return [str(name) for name in names]

    def _operation_direction(self, operation: str, names: Iterable[str]) -> str:
        normalized = str(operation)
        if normalized.startswith("prefetch") or normalized.startswith("load"):
            return "h2d"
        if normalized.startswith("offload"):
            return "d2h"
        directions = {
            self._block_direction(self.block(name).last_operation)
            for name in names
            if self.block(name).last_operation is not None
        }
        directions.discard("unknown")
        if len(directions) == 1:
            return next(iter(directions))
        if len(directions) > 1:
            return "mixed"
        return "unknown"

    @staticmethod
    def _block_direction(operation: str | None) -> str:
        if operation == "prefetch":
            return "h2d"
        if operation == "evict":
            return "d2h"
        return "unknown"


__all__ = [
    "StateDescriptor",
    "StateOffloadCore",
    "StateOffloadLifecycle",
    "StateOffloadSpec",
]
