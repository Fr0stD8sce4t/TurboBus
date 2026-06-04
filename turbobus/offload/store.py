from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Mapping

from ..runtime_session import TurboBusRuntimeSession
from ..schema import TransferIntent, TransferReceipt, WorkloadKind
from .blocks import BlockState, OffloadBlock, OffloadBlockInfo
from .context import AdapterTransferContext, require_runtime_session_open
from .handles import ReceiptTransferHandle, validate_adapter_receipt
from .stats import TransferStats, summarize_transfer_handles


@dataclass(frozen=True)
class OffloadBatch:
    operation: str
    names: tuple[str, ...]
    handles: tuple[object, ...]
    store: "OffloadStore" = field(repr=False, compare=False)

    def wait(self) -> None:
        self.store.wait_many(self.names)

    def transfer_stats(self) -> TransferStats:
        return self.store.transfer_stats_many(self.names)

    def block_infos(self) -> list[OffloadBlockInfo]:
        return self.store.block_infos(self.names)

    def as_dict(self) -> dict[str, object]:
        return {
            "operation": self.operation,
            "names": list(self.names),
            "transfer_stats": self.transfer_stats().as_dict(),
            "blocks": [info.as_dict() for info in self.block_infos()],
        }


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

    @classmethod
    def from_runtime_session(
        cls,
        runtime_session,
        cpu_buffer,
        gpu_buffer,
        *,
        workload_kind: WorkloadKind | str = WorkloadKind.GENERIC,
        priority: int = 0,
        policy_hints: Mapping[str, object] | None = None,
        metadata: Mapping[str, object] | None = None,
        intent_prefix: str | None = None,
        wait_timeout_seconds: float | None = None,
    ) -> "OffloadStore":
        context = AdapterTransferContext.from_runtime_session(
            runtime_session,
            cpu_buffer,
            gpu_buffer,
            workload_kind=workload_kind,
            priority=priority,
            policy_hints=policy_hints,
            metadata=metadata,
            intent_prefix=intent_prefix,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        return cls(runtime_session, context)

    def add(
        self,
        name: str,
        cpu_tensor,
        gpu_tensor=None,
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
        if gpu_tensor is None:
            gpu_tensor = object()
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

    def stats(self, name: str):
        return self.block(name).last_stats

    def transfer_stats(self, name: str) -> TransferStats | None:
        return self.block(name).last_transfer_stats

    def transfer_stats_many(self, names: Iterable[str]) -> TransferStats:
        return summarize_transfer_handles(
            block.last_handle
            for block in (self.block(name) for name in names)
            if block.last_handle is not None
        )

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
        direction = _direction_for_operation(operation)
        ranges_tuple = tuple(dict(item) for item in ranges)
        total_bytes = sum(item["bytes"] for item in ranges_tuple)
        if direction == "h2d":
            source_buffer_id = self.transfer_context.cpu_buffer_id
            destination_buffer_id = self.transfer_context.gpu_buffer_id
        else:
            source_buffer_id = self.transfer_context.gpu_buffer_id
            destination_buffer_id = self.transfer_context.cpu_buffer_id
        metadata = {
            **self.transfer_context.metadata,
            "operation": operation,
            "block_names": [block.name for block in blocks],
        }
        intent = TransferIntent(
            intent_id=self._next_intent_id(operation),
            job_id=self.transfer_context.job_id,
            session_id=self.transfer_context.session_id,
            source_buffer_id=source_buffer_id,
            destination_buffer_id=destination_buffer_id,
            direction=direction,
            total_bytes=total_bytes,
            ranges=ranges_tuple,
            workload_kind=self.transfer_context.workload_kind,
            priority=self.transfer_context.priority,
            policy_hints=self.transfer_context.policy_hints,
            metadata=metadata,
        )
        receipt = self.client.submit_transfer_intent(intent)
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
