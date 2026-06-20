from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from ..offload.blocks import BlockState, OffloadBlock, OffloadBlockInfo
from ..offload.context import TransferContext
from ..offload.stats import TransferStatsSnapshot
from ..offload.store import OffloadBatch, OffloadStore
from ..schema import WorkloadKind


@dataclass(frozen=True)
class InferenceKVSlot:
    """One inference-framework-owned KV block slot."""

    name: str
    block_id: object
    cpu_offset: int
    gpu_offset: int
    byte_count: int
    cpu_slot: object | None = None
    gpu_slot: object | None = None


class InferenceKVSlotBinding(OffloadStore):
    """Register framework KV slots and restore/save them through TurboBus."""

    def __init__(
        self,
        runtime_session,
        cpu_backing,
        gpu_kv_backing,
        *,
        workload_kind: WorkloadKind | str = WorkloadKind.KV_CACHE,
        priority: int = 0,
        metadata: Mapping[str, object] | None = None,
        intent_prefix: str | None = None,
        wait_timeout_seconds: float | None = None,
    ) -> None:
        transfer_context = runtime_session.make_transfer_context(
            cpu_backing,
            gpu_kv_backing,
            workload_kind=workload_kind,
            priority=priority,
            metadata=metadata,
            intent_prefix=intent_prefix,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        if not isinstance(transfer_context, TransferContext):
            raise TypeError(
                "runtime session transfer context factory must return a TransferContext"
            )
        self._init_from_transfer_context(
            runtime_session,
            transfer_context,
            cpu_backing,
            gpu_kv_backing,
        )

    @classmethod
    def _from_transfer_context(
        cls,
        client,
        transfer_context: TransferContext,
        cpu_backing,
        gpu_kv_backing,
    ) -> "InferenceKVSlotBinding":
        instance = cls.__new__(cls)
        instance._init_from_transfer_context(
            client,
            transfer_context,
            cpu_backing,
            gpu_kv_backing,
        )
        return instance

    def _init_from_transfer_context(
        self,
        client,
        transfer_context: TransferContext,
        cpu_backing,
        gpu_kv_backing,
    ) -> None:
        super().__init__(client, transfer_context)
        self.cpu_backing = cpu_backing
        self.gpu_kv_backing = gpu_kv_backing

    def register_slot(self, slot: InferenceKVSlot) -> OffloadBlock:
        if not isinstance(slot, InferenceKVSlot):
            raise TypeError("slot must be an InferenceKVSlot")
        return self.add(
            slot.name,
            self.cpu_backing,
            self.gpu_kv_backing,
            block_id=slot.block_id,
            cpu_slot=slot.cpu_slot,
            gpu_slot=slot.gpu_slot,
            cpu_offset=slot.cpu_offset,
            gpu_offset=slot.gpu_offset,
            byte_count=slot.byte_count,
        )

    def register_slots(self, slots: Iterable[InferenceKVSlot]) -> list[OffloadBlock]:
        registered: list[OffloadBlock] = []
        for slot in slots:
            registered.append(self.register_slot(slot))
        return registered

    def register_contiguous_slots(
        self,
        prefix: str,
        count: int,
        block_bytes: int,
        *,
        start_cpu_offset: int = 0,
        start_gpu_offset: int = 0,
        start_slot: int = 0,
    ) -> list[OffloadBlock]:
        return self.register_slots(
            make_contiguous_kv_slots(
                prefix,
                count,
                block_bytes,
                start_cpu_offset=start_cpu_offset,
                start_gpu_offset=start_gpu_offset,
                start_slot=start_slot,
            )
        )

    def slot(self, name: str) -> OffloadBlock:
        return self.block(name)

    def slot_info(self, name: str) -> OffloadBlockInfo:
        return self.block_info(name)

    def slot_infos(self, names: Iterable[str] | None = None) -> list[OffloadBlockInfo]:
        return self.block_infos(names)

    def restore_slot(self, name: str) -> OffloadBatch:
        return self.prefetch(name)

    def restore_prefix(self, names: Iterable[str]) -> OffloadBatch:
        return self._run_prefix_transfer(names, self.submit_restore_prefix)

    def restore_all(self) -> OffloadBatch:
        return self.restore_prefix(self.names())

    def save_slot(self, name: str) -> OffloadBatch:
        return self.evict(name)

    def save_prefix(self, names: Iterable[str]) -> OffloadBatch:
        return self._run_prefix_transfer(names, self.submit_save_prefix)

    def save_all(self) -> OffloadBatch:
        return self.save_prefix(self.names())

    def submit_restore_batch(self, names: Iterable[str]) -> OffloadBatch:
        names = list(names)
        return self.submit_prefetch_many(names)

    def submit_restore_prefix(self, names: Iterable[str]) -> OffloadBatch:
        return self.submit_restore_batch(names)

    def restore_batch(self, names: Iterable[str]) -> OffloadBatch:
        names = list(names)
        return self.submit_restore_batch(names)

    def submit_save_batch(self, names: Iterable[str]) -> OffloadBatch:
        names = list(names)
        return self.submit_evict_many(names)

    def submit_save_prefix(self, names: Iterable[str]) -> OffloadBatch:
        return self.submit_save_batch(names)

    def save_batch(self, names: Iterable[str]) -> OffloadBatch:
        names = list(names)
        return self.submit_save_batch(names)

    def wait_prefix(self, names: Iterable[str]) -> None:
        self.wait_many(names)

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

    def _run_prefix_transfer(self, names: Iterable[str], submit) -> OffloadBatch:
        batch = submit(names)
        batch.wait()
        return batch


def make_contiguous_kv_slots(
    prefix: str,
    count: int,
    block_bytes: int,
    *,
    start_cpu_offset: int = 0,
    start_gpu_offset: int = 0,
    start_slot: int = 0,
) -> list[InferenceKVSlot]:
    return [
        InferenceKVSlot(
            name=f"{prefix}{index}",
            block_id=start_slot + index,
            cpu_slot=start_slot + index,
            gpu_slot=start_slot + index,
            cpu_offset=start_cpu_offset + index * block_bytes,
            gpu_offset=start_gpu_offset + index * block_bytes,
            byte_count=block_bytes,
        )
        for index in range(count)
    ]


__all__ = [
    "InferenceKVSlot",
    "InferenceKVSlotBinding",
    "make_contiguous_kv_slots",
]

