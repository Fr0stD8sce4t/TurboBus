from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Iterable, Mapping

from ..runtime.evidence import (
    validate_adapter_batch_snapshot,
    validate_adapter_lifecycle_evidence,
    validate_adapter_transfer_stats_snapshot,
)
from ..runtime_session import TurboBusRuntimeSession
from ..schema import TransferIntent, TransferReceipt, WorkloadKind
from .blocks import BlockState, OffloadBlock, OffloadBlockInfo
from .context import AdapterTransferContext, require_runtime_session_open
from .handles import ReceiptTransferHandle, validate_adapter_receipt
from .lifecycle import adapter_lifecycle_evidence_from_handles
from .stats import TransferStats, TransferStatsSnapshot, summarize_transfer_handles

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OffloadBatch:
    operation: str
    names: tuple[str, ...]
    handles: tuple[object, ...]
    store: "OffloadStore" = field(repr=False, compare=False)

    def wait(self) -> None:
        self.store.wait_many(self.names)

    def transfer_stats(self) -> TransferStatsSnapshot:
        return self.store.transfer_stats_snapshot(self.names)

    def block_infos(self) -> list[OffloadBlockInfo]:
        return self.store.block_infos(self.names)

    def as_dict(self) -> dict[str, object]:
        # /*
        #  * ========================================================================
        #  * 步骤1：生成 RuntimeSession 绑定 batch 快照
        #  * ========================================================================
        #  * 数据源：OffloadBatch handles 与 TurboBusRuntimeSession entrypoint record
        #  * 操作：
        #  *   1) 通过 adapter lifecycle evidence 绑定真实 TransferReceipt
        #  *   2) 只导出 RuntimeSession adapter evidence 对齐后的运行态摘要
        #  */
        logger.info("开始生成 RuntimeSession 绑定 batch 快照...")

        # // 1.1 读取 block 结构快照
        block_snapshots = [info.as_dict() for info in self.block_infos()]

        # // 1.2 空 batch 不伪造 transfer evidence
        if not self.handles:
            snapshot = {
                "operation": self.operation,
                "names": list(self.names),
                "transfer_state": "empty",
                "blocks": block_snapshots,
                "route_policy_visible_to_adapter": False,
            }
            validate_adapter_batch_snapshot(snapshot)
            logger.info("RuntimeSession 绑定 batch 快照生成完成, receipts: %s", 0)
            return snapshot

        # // 1.3 生成 RuntimeSession adapter evidence
        evidence = self.store.batch_lifecycle_evidence(
            operation=self.operation,
            names=self.names,
            handles=self.handles,
        )
        snapshot = {
            "operation": self.operation,
            "names": list(self.names),
            "transfer_state": "runtime_session_bound",
            "blocks": block_snapshots,
            "runtime_entrypoint": dict(evidence["runtime_entrypoint"]),
            "adapter_evidence_record": dict(
                evidence["runtime_entrypoint"]["adapter_evidence_record"]
            ),
            "receipt_contracts": list(evidence["receipt_contracts"]),
            "receipt_count": int(evidence["receipt_count"]),
            "receipt_ids": str(evidence["receipt_ids"]),
            "intent_ids": str(evidence["intent_ids"]),
            "decision_ids": str(evidence["decision_ids"]),
            "topology_snapshot_ids": str(evidence["topology_snapshot_ids"]),
            "ticket_ids": str(evidence["ticket_ids"]),
            "receipt_states": str(evidence["receipt_states"]),
            "direct_bytes": int(evidence["direct_bytes"]),
            "relay_bytes": int(evidence["relay_bytes"]),
            "route_policy_visible_to_adapter": False,
        }
        validate_adapter_batch_snapshot(snapshot)
        logger.info(
            "RuntimeSession 绑定 batch 快照生成完成, receipts: %s",
            snapshot["receipt_count"],
        )
        return snapshot


class OffloadStore:
    """Connector-shaped named-block layer over daemon transfer intent."""

    def __init__(
        self,
        client: TurboBusRuntimeSession,
        transfer_context: AdapterTransferContext,
    ) -> None:
        if not isinstance(transfer_context, AdapterTransferContext):
            raise TypeError("transfer_context must be an AdapterTransferContext")
        _require_runtime_session_client(client, transfer_context)
        self.client = client
        self.transfer_context = transfer_context
        self._blocks: dict[str, OffloadBlock] = {}
        self._intent_counter = 0

    def add(
        self,
        name: str,
        cpu_tensor,
        gpu_tensor,
        *,
        block_id=None,
        cpu_slot=None,
        gpu_slot=None,
        cpu_offset: int = 0,
        gpu_offset: int = 0,
        byte_count: int | None = None,
    ) -> OffloadBlock:
        self._validate_name(name)
        self._validate_range_fields(cpu_offset, gpu_offset, byte_count)
        if name in self._blocks:
            raise ValueError(f"offload block already exists: {name}")
        block = OffloadBlock(
            name=name,
            cpu_tensor=cpu_tensor,
            gpu_tensor=gpu_tensor,
            block_id=block_id,
            cpu_slot=cpu_slot,
            gpu_slot=gpu_slot,
            cpu_offset=int(cpu_offset),
            gpu_offset=int(gpu_offset),
            byte_count=int(byte_count) if byte_count is not None else None,
        )
        self._blocks[name] = block
        return block

    def remove(self, name: str) -> OffloadBlock:
        return self._blocks.pop(name)

    def block(self, name: str) -> OffloadBlock:
        try:
            return self._blocks[name]
        except KeyError as exc:
            raise KeyError(f"unknown offload block: {name}") from exc

    def names(self) -> list[str]:
        return list(self._blocks)

    def block_ids(self) -> list[object]:
        return [block.block_id for block in self._blocks.values()]

    def blocks(self) -> Iterable[OffloadBlock]:
        return self._blocks.values()

    def block_info(self, name: str) -> OffloadBlockInfo:
        return self.block(name).info()

    def block_infos(self, names: Iterable[str] | None = None) -> list[OffloadBlockInfo]:
        if names is None:
            return [block.info() for block in self._blocks.values()]
        return [self.block(name).info() for name in names]

    def prefetch(self, name: str):
        return self.submit_prefetch_many([name]).handles[0]

    def evict(self, name: str):
        return self.submit_evict_many([name]).handles[0]

    def prefetch_many(self, names: Iterable[str]) -> list:
        return list(self.submit_prefetch_many(names).handles)

    def submit_prefetch_many(self, names: Iterable[str]) -> OffloadBatch:
        return self._submit_many(names, "prefetch")

    def evict_many(self, names: Iterable[str]) -> list:
        return list(self.submit_evict_many(names).handles)

    def submit_evict_many(self, names: Iterable[str]) -> OffloadBatch:
        return self._submit_many(names, "evict")

    def _submit_many(self, names: Iterable[str], operation: str) -> OffloadBatch:
        blocks = [self.block(name) for name in names]
        if not blocks:
            return OffloadBatch(operation, (), (), self)
        if self._can_use_range_batch(blocks):
            ranges = self._ranges(blocks, operation)
            if operation == "prefetch":
                handle = self._submit_transfer(blocks, "prefetch", ranges)
                state = BlockState.PREFETCHING
            elif operation == "evict":
                handle = self._submit_transfer(blocks, "evict", ranges)
                state = BlockState.EVICTING
            else:
                raise ValueError(f"unknown offload operation: {operation}")
            self._record_many(blocks, handle, operation, state)
            handles = tuple(handle for _ in blocks)
        else:
            if operation == "prefetch":
                state = BlockState.PREFETCHING
            elif operation == "evict":
                state = BlockState.EVICTING
            else:
                raise ValueError(f"unknown offload operation: {operation}")
            handles = tuple(
                self._submit_transfer([block], operation, self._ranges([block], operation))
                for block in blocks
            )
            for block, handle in zip(blocks, handles):
                self._record_many([block], handle, operation, state)
        return OffloadBatch(operation, tuple(block.name for block in blocks), handles, self)

    def wait(self, name: str) -> None:
        block = self.block(name)
        if block.last_handle is None:
            return
        block.last_handle.wait()
        self._mark_waited(block)

    def wait_many(self, names: Iterable[str]) -> None:
        waited = set()
        for name in names:
            block = self.block(name)
            handle_key = id(block.last_handle)
            if block.last_handle is not None and handle_key not in waited:
                block.last_handle.wait()
                waited.add(handle_key)
            self._mark_waited(block)

    def stats(self, name: str) -> TransferStatsSnapshot:
        return self.transfer_stats_snapshot([name])

    def transfer_stats(self, name: str) -> TransferStatsSnapshot:
        return self.transfer_stats_snapshot([name])

    def _raw_transfer_stats(self, name: str) -> TransferStats | None:
        return self.block(name).last_transfer_stats

    def transfer_stats_many(self, names: Iterable[str]) -> TransferStatsSnapshot:
        return self.transfer_stats_snapshot(names)

    def transfer_stats_snapshot(self, names: Iterable[str]) -> TransferStatsSnapshot:
        # /*
        #  * ========================================================================
        #  * 步骤3：生成 RuntimeSession 绑定 transfer stats 快照
        #  * ========================================================================
        #  * 数据源：OffloadStore handles 与 RuntimeSession adapter evidence record
        #  * 操作：
        #  *   1) 禁止公开 raw direct/relay stats
        #  *   2) 只返回经过 RuntimeSession evidence 校验的统计快照
        #  */
        logger.info("开始生成 RuntimeSession 绑定 transfer stats 快照...")

        # // 3.1 归一化名称并提取已提交 handle
        selected_names = tuple(str(name) for name in names)
        handles = tuple(self._handles_for_names(selected_names))
        if not selected_names or not handles:
            raise RuntimeError("adapter transfer stats require RuntimeSession receipts")

        # // 3.2 生成 RuntimeSession-bound lifecycle evidence
        evidence = self.batch_lifecycle_evidence(
            operation="transfer_stats",
            names=selected_names,
            handles=handles,
        )
        snapshot = {
            "transfer_state": "runtime_session_bound",
            "names": list(selected_names),
            "bytes": int(evidence.get("bytes", 0) or 0),
            "direct_chunks": int(evidence.get("direct_chunks", 0) or 0),
            "relay_chunks": int(evidence.get("relay_chunks", 0) or 0),
            "runtime_entrypoint": dict(evidence["runtime_entrypoint"]),
            "adapter_evidence_record": dict(
                evidence["runtime_entrypoint"]["adapter_evidence_record"]
            ),
            "receipt_contracts": list(evidence["receipt_contracts"]),
            "receipt_count": int(evidence["receipt_count"]),
            "receipt_ids": str(evidence["receipt_ids"]),
            "intent_ids": str(evidence["intent_ids"]),
            "decision_ids": str(evidence["decision_ids"]),
            "topology_snapshot_ids": str(evidence["topology_snapshot_ids"]),
            "ticket_ids": str(evidence["ticket_ids"]),
            "receipt_states": str(evidence["receipt_states"]),
            "direct_bytes": int(evidence["direct_bytes"]),
            "relay_bytes": int(evidence["relay_bytes"]),
            "route_policy_visible_to_adapter": False,
        }
        validate_adapter_transfer_stats_snapshot(snapshot)
        logger.info(
            "RuntimeSession 绑定 transfer stats 快照生成完成, receipts: %s",
            snapshot["receipt_count"],
        )
        return TransferStatsSnapshot(snapshot)

    def _raw_transfer_stats_many(self, names: Iterable[str]) -> TransferStats:
        return summarize_transfer_handles(
            block.last_handle
            for block in (self.block(name) for name in names)
            if block.last_handle is not None
        )

    def batch_lifecycle_evidence(
        self,
        *,
        operation: str,
        names: Iterable[str],
        handles: Iterable[object],
    ) -> dict[str, object]:
        # /*
        #  * ========================================================================
        #  * 步骤2：绑定 batch lifecycle evidence
        #  * ========================================================================
        #  * 数据源：adapter transfer handles 与 OffloadStore RuntimeSession client
        #  * 操作：
        #  *   1) 用 RuntimeSession entrypoint record 记录 batch receipt 对齐关系
        #  *   2) 返回 adapter 可消费的 evidence，不开放 route/relay/pool 选择
        #  */
        logger.info("开始绑定 batch lifecycle evidence...")

        # // 2.1 归一化 batch 名称和 handle
        selected_names = tuple(str(name) for name in names)
        selected_handles = tuple(handles)

        # // 2.2 基于真实 receipt 生成 RuntimeSession-bound evidence
        evidence = adapter_lifecycle_evidence_from_handles(
            evidence_id=(
                f"offload-batch-{self.transfer_context.session_id}-"
                f"{self.transfer_context.intent_prefix}-{operation}-"
                f"{'-'.join(selected_names)}"
            ),
            operation=str(operation),
            transfer_context=self.transfer_context,
            item_field="block_names",
            item_count_field="block_count",
            item_names=selected_names,
            handles=selected_handles,
            transfer_stats=self._raw_transfer_stats_many(selected_names).as_dict(),
            runtime_session=self.client,
            extra={
                "adapter": "offload_store_batch",
                "adapter_submit_source": "TurboBusRuntimeSession",
                "adapter_handle_source": "ReceiptTransferHandle",
                "batch_snapshot_source": "OffloadBatch.as_dict",
            },
        )

        # // 2.3 用 runtime/evidence 严格校验 RuntimeSession adapter record
        validate_adapter_lifecycle_evidence(evidence)
        logger.info(
            "batch lifecycle evidence 绑定完成, evidence_id: %s",
            evidence["evidence_id"],
        )
        return evidence

    def _handles_for_names(self, names: Iterable[str]) -> list[object]:
        handles = []
        seen = set()
        for name in names:
            handle = self.block(str(name)).last_handle
            if handle is None or id(handle) in seen:
                continue
            seen.add(id(handle))
            handles.append(handle)
        return handles

    def set_block_state(
        self,
        name: str,
        state: BlockState,
        *,
        clear_transfer_state: bool = False,
    ) -> OffloadBlock:
        block = self.block(name)
        block.state = state
        if clear_transfer_state:
            self.clear_block_transfer_state(name)
        return block

    def clear_block_transfer_state(self, name: str) -> OffloadBlock:
        block = self.block(name)
        block.last_prefetch = None
        block.last_evict = None
        block.last_handle = None
        block.last_operation = None
        return block

    def _mark_waited(self, block: OffloadBlock) -> None:
        if block.last_operation == "prefetch":
            block.state = BlockState.GPU
        elif block.last_operation == "evict":
            block.state = BlockState.CPU
        else:
            block.state = BlockState.UNKNOWN

    @staticmethod
    def _can_use_range_batch(blocks: list[OffloadBlock]) -> bool:
        first = blocks[0]
        if first.byte_count is None:
            return False
        return all(
            block.cpu_tensor is first.cpu_tensor
            and block.gpu_tensor is first.gpu_tensor
            and block.byte_count is not None
            for block in blocks
        )

    @staticmethod
    def _ranges(blocks: list[OffloadBlock], operation: str) -> list[dict]:
        ranges = []
        for block in blocks:
            if operation == "prefetch":
                src_offset = block.cpu_offset
                dst_offset = block.gpu_offset
            elif operation == "evict":
                src_offset = block.gpu_offset
                dst_offset = block.cpu_offset
            else:
                raise ValueError(f"unknown offload operation: {operation}")
            ranges.append(
                {
                    "src_offset": src_offset,
                    "dst_offset": dst_offset,
                    "bytes": block.bytes,
                }
            )
        return ranges

    @staticmethod
    def _record_many(
        blocks: list[OffloadBlock],
        handle,
        operation: str,
        state: BlockState,
    ) -> None:
        for block in blocks:
            if operation == "prefetch":
                block.last_prefetch = handle
            elif operation == "evict":
                block.last_evict = handle
            else:
                raise ValueError(f"unknown offload operation: {operation}")
            block.last_handle = handle
            block.last_operation = operation
            block.state = state

    def _submit_transfer(
        self,
        blocks: list[OffloadBlock],
        operation: str,
        ranges: Iterable[dict[str, int]],
    ) -> ReceiptTransferHandle:
        require_runtime_session_open(self.client)
        self.client.open_session()
        register_pending_buffers = getattr(self.client, "_register_pending_buffers", None)
        if callable(register_pending_buffers):
            register_pending_buffers()
        direction = _direction_for_operation(operation)
        ranges_tuple = tuple(dict(item) for item in ranges)
        total_bytes = sum(item["bytes"] for item in ranges_tuple)
        if direction == "h2d":
            source_buffer = self.transfer_context.cpu_buffer
            destination_buffer = self.transfer_context.gpu_buffer
        else:
            source_buffer = self.transfer_context.gpu_buffer
            destination_buffer = self.transfer_context.cpu_buffer
        metadata = {
            **self.transfer_context.metadata,
            "operation": operation,
            "block_names": [block.name for block in blocks],
        }
        intent_id = self._next_intent_id(operation)
        intent = TransferIntent(
            intent_id=intent_id,
            job_id=self.transfer_context.job_id,
            session_id=self.transfer_context.session_id,
            source_buffer_id=(
                self.transfer_context.cpu_buffer_id
                if direction == "h2d"
                else self.transfer_context.gpu_buffer_id
            ),
            destination_buffer_id=(
                self.transfer_context.gpu_buffer_id
                if direction == "h2d"
                else self.transfer_context.cpu_buffer_id
            ),
            direction=direction,
            total_bytes=total_bytes,
            ranges=ranges_tuple,
            workload_kind=self.transfer_context.workload_kind,
            priority=self.transfer_context.priority,
            policy_hints=self.transfer_context.policy_hints,
            metadata=metadata,
        )
        receipt = self.client.submit_transfer_intent(intent, wait=False)
        if not isinstance(receipt, TransferReceipt):
            raise TypeError("submit_transfer_intent must return a TransferReceipt")
        validate_adapter_receipt(
            receipt,
            intent,
            transfer_context=self.transfer_context,
        )
        return ReceiptTransferHandle(
            client=self.client,
            intent=intent,
            receipt=receipt,
            transfer_context=self.transfer_context,
            wait_timeout_seconds=self.transfer_context.wait_timeout_seconds,
        )

    def _next_intent_id(self, operation: str) -> str:
        self._intent_counter += 1
        return f"{self.transfer_context.intent_prefix}-{operation}-{self._intent_counter}"

    @staticmethod
    def _validate_name(name: str) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError("offload block name must be a non-empty string")

    @staticmethod
    def _validate_range_fields(
        cpu_offset: int,
        gpu_offset: int,
        byte_count: int | None,
    ) -> None:
        if cpu_offset < 0 or gpu_offset < 0:
            raise ValueError("block offsets must be non-negative")
        if byte_count is not None and byte_count <= 0:
            raise ValueError("byte_count must be positive")


def _direction_for_operation(operation: str) -> str:
    if operation == "prefetch":
        return "h2d"
    if operation == "evict":
        return "d2h"
    raise ValueError(f"unknown offload operation: {operation}")


def _require_runtime_session_client(
    client: TurboBusRuntimeSession,
    transfer_context: AdapterTransferContext,
) -> None:
    require_runtime_session_open(client)
    if str(client.job_id) != transfer_context.job_id:
        raise ValueError("offload context job_id must match the runtime session job_id")
    try:
        session_id = getattr(client, "session_id")
    except RuntimeError:
        session_id = client.open_session()
    except AttributeError as exc:
        raise TypeError(
            "OffloadStore client must expose a runtime session session_id"
        ) from exc
    if str(session_id) != transfer_context.session_id:
        raise ValueError(
            "offload context session_id must match the runtime session session_id"
        )


__all__ = [
    "OffloadBatch",
    "OffloadStore",
]
