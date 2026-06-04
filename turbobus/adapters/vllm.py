from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Iterable, Mapping

from ..client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer
from ..offload_store import AdapterTransferContext, TransferStats
from ..schema import WorkloadKind
from .inference import InferenceKVSlot, InferenceKVSlotAdapter


@dataclass(frozen=True)
class VllmKVBlockRef:
    """One vLLM KV block mapped to saved CPU backing and a GPU KV slot."""

    request_id: str
    group_id: int
    block_id: int
    cpu_slot: int
    gpu_slot: int
    lane_id: int | None = None
    cpu_offset: int | None = None
    gpu_offset: int | None = None
    byte_count: int | None = None


@dataclass(frozen=True)
class VllmKVGroup:
    """KV backing tensors and block size for one vLLM KV cache group."""

    group_id: int
    cpu_backing: object
    gpu_kv_backing: object
    block_bytes: int
    layer_id: int | None = None


class VllmKVSlotAdapter:
    """vLLM-shaped wrapper around TurboBus inference KV slot adapters."""

    def __init__(
        self,
        runtime_session,
        groups: Iterable[VllmKVGroup],
        *,
        workload_kind: WorkloadKind | str = WorkloadKind.KV_CACHE,
        priority: int = 0,
        metadata: Mapping[str, object] | None = None,
        intent_prefix: str | None = None,
        wait_timeout_seconds: float | None = None,
        gpu_buffer_id: str = "vllm-kv-gpu",
    ) -> None:
        _require_runtime_session_open(runtime_session)
        self.client = runtime_session
        self.groups: dict[int, VllmKVGroup] = {group.group_id: group for group in groups}
        self.runtime_session = runtime_session
        self.adapters = {}
        self.transfer_context: AdapterTransferContext | None = None
        for group in self.groups.values():
            cpu_buffer = runtime_session.register_cpu_buffer(
                _require_shared_cpu_buffer(group.cpu_backing)
            )
            gpu_buffer = runtime_session.register_cuda_buffer(
                _cuda_buffer_for_group(
                    runtime_session,
                    group,
                    buffer_id=_group_gpu_buffer_id(gpu_buffer_id, group),
                )
            )
            group_context = AdapterTransferContext(
                job_id=runtime_session.job_id,
                session_id=runtime_session.open_session(),
                cpu_buffer_id=cpu_buffer.buffer_id,
                gpu_buffer_id=gpu_buffer.buffer_id,
                workload_kind=workload_kind,
                priority=priority,
                metadata=_group_metadata(metadata, group.group_id),
                intent_prefix=_group_intent_prefix(intent_prefix, group.group_id),
                wait_timeout_seconds=wait_timeout_seconds,
            )
            adapter = InferenceKVSlotAdapter._from_transfer_context(
                runtime_session,
                group_context,
                cpu_buffer,
                gpu_buffer,
            )
            if self.transfer_context is None:
                self.transfer_context = group_context
            self.adapters[group.group_id] = adapter
        if self.transfer_context is None:
            raise ValueError("vLLM adapter requires at least one group")
        self._registered_names: set[str] = set()

    @classmethod
    def from_runtime_session(
        cls,
        runtime_session,
        groups: Iterable[VllmKVGroup],
        *,
        workload_kind: WorkloadKind | str = WorkloadKind.KV_CACHE,
        priority: int = 0,
        metadata: Mapping[str, object] | None = None,
        intent_prefix: str | None = None,
        wait_timeout_seconds: float | None = None,
        gpu_buffer_id: str = "vllm-kv-gpu",
    ) -> "VllmKVSlotAdapter":
        resolved_groups = tuple(groups)
        if not resolved_groups:
            raise ValueError("vLLM runtime session adapter requires at least one group")
        return cls(
            runtime_session,
            resolved_groups,
            workload_kind=workload_kind,
            priority=priority,
            metadata=metadata,
            intent_prefix=intent_prefix,
            wait_timeout_seconds=wait_timeout_seconds,
            gpu_buffer_id=gpu_buffer_id,
        )

    def register_blocks(self, refs: Iterable[VllmKVBlockRef]) -> list[str]:
        slots_by_group: dict[int, list[InferenceKVSlot]] = {}
        names = []
        for ref in refs:
            group = self.groups[ref.group_id]
            name = vllm_block_name(ref)
            names.append(name)
            if name in self._registered_names:
                continue
            slots_by_group.setdefault(ref.group_id, []).append(
                InferenceKVSlot(
                    name=name,
                    block_id=(ref.request_id, ref.group_id, ref.block_id),
                    cpu_slot=ref.cpu_slot,
                    gpu_slot=ref.gpu_slot,
                    cpu_offset=(
                        ref.cpu_offset
                        if ref.cpu_offset is not None
                        else ref.cpu_slot * group.block_bytes
                    ),
                    gpu_offset=(
                        ref.gpu_offset
                        if ref.gpu_offset is not None
                        else ref.gpu_slot * group.block_bytes
                    ),
                    byte_count=(
                        ref.byte_count
                        if ref.byte_count is not None
                        else group.block_bytes
                    ),
                )
            )

        for group_id, slots in slots_by_group.items():
            self.adapters[group_id].register_slots(slots)
            for slot in slots:
                self._registered_names.add(slot.name)
        return names

    def restore_prefix(self, refs: Iterable[VllmKVBlockRef]) -> list:
        return self._transfer_prefix(refs, "restore")

    def save_prefix(self, refs: Iterable[VllmKVBlockRef]) -> list:
        return self._transfer_prefix(refs, "save")

    def transfer_stats(self, refs: Iterable[VllmKVBlockRef]) -> TransferStats:
        names_by_group = self._register_and_group(refs)
        total = TransferStats()
        for group_id, names in names_by_group.items():
            total = self._sum_transfer_stats(total, self.adapters[group_id].transfer_stats(names))
        return total

    def _transfer_prefix(self, refs: Iterable[VllmKVBlockRef], operation: str) -> list:
        refs = list(refs)
        names_by_group = self._register_and_group(refs)
        handles = []
        submitted = []
        submit_method = "submit_restore_prefix" if operation == "restore" else "submit_save_prefix"
        for group_id, names in names_by_group.items():
            submit = getattr(self.adapters[group_id], submit_method)
            names, group_handles = submit(names)
            submitted.append((group_id, names))
            handles.extend(group_handles)
        for group_id, names in submitted:
            self.adapters[group_id].wait_prefix(names)
        return handles

    @staticmethod
    def _sum_transfer_stats(total: TransferStats, stats: TransferStats) -> TransferStats:
        return TransferStats(
            bytes=total.bytes + stats.bytes,
            direct_chunks=total.direct_chunks + stats.direct_chunks,
            relay_chunks=total.relay_chunks + stats.relay_chunks,
        )

    def _register_and_group(
        self,
        refs: Iterable[VllmKVBlockRef],
    ) -> Mapping[int, list[str]]:
        refs = list(refs)
        self.register_blocks(refs)
        names_by_group: dict[int, list[str]] = {}
        for ref in refs:
            names_by_group.setdefault(ref.group_id, []).append(vllm_block_name(ref))
        return names_by_group

    def _batch_size(self, refs: Iterable[VllmKVBlockRef]) -> tuple[int, int]:
        total_bytes = 0
        total_chunks = 0
        chunk_bytes = max(
            1,
            int(self.transfer_context.metadata.get("chunk_bytes", 16 * 1024 * 1024)),
        )
        for ref in refs:
            group = self.groups[ref.group_id]
            byte_count = ref.byte_count if ref.byte_count is not None else group.block_bytes
            total_bytes += int(byte_count)
            total_chunks += max(1, math.ceil(int(byte_count) / chunk_bytes))
        return total_bytes, total_chunks

def _group_metadata(
    metadata: Mapping[str, object] | None,
    group_id: int,
) -> dict[str, object]:
    metadata = {} if metadata is None else dict(metadata)
    metadata["group_id"] = int(group_id)
    return metadata


def _group_intent_prefix(intent_prefix: str | None, group_id: int) -> str | None:
    if intent_prefix is None:
        return None
    return f"{intent_prefix}-g{int(group_id)}"


def _group_gpu_buffer_id(prefix: str, group: VllmKVGroup) -> str:
    return f"{prefix}-g{int(group.group_id)}"


def _require_shared_cpu_buffer(value) -> SharedPinnedCpuBuffer:
    if not isinstance(value, SharedPinnedCpuBuffer):
        raise TypeError(
            "runtime session vLLM transfers require SharedPinnedCpuBuffer CPU backing"
        )
    return value


def _cuda_buffer_for_group(
    runtime_session,
    group: VllmKVGroup,
    *,
    buffer_id: str,
) -> CudaIpcDeviceBuffer:
    value = group.gpu_kv_backing
    if isinstance(value, CudaIpcDeviceBuffer):
        return value
    data_ptr = getattr(value, "data_ptr", None)
    if not callable(data_ptr):
        raise TypeError("runtime session vLLM transfers require CUDA tensor backings")
    ptr = int(data_ptr())
    return CudaIpcDeviceBuffer.from_device_pointer(
        buffer_id=f"{buffer_id}-{ptr:x}",
        job_id=runtime_session.job_id,
        device_index=_tensor_device_index(value),
        size_bytes=_tensor_nbytes(value),
        device_ptr=ptr,
    )


def _tensor_device_index(tensor) -> int:
    device = getattr(tensor, "device", None)
    index = getattr(device, "index", None)
    if index is not None:
        return int(index)
    getter = getattr(tensor, "get_device", None)
    if callable(getter):
        return int(getter())
    return 0


def _tensor_nbytes(tensor) -> int:
    nbytes = getattr(tensor, "nbytes", None)
    if nbytes is not None:
        return int(nbytes)
    return int(tensor.numel() * tensor.element_size())


def _require_runtime_session_open(runtime_session) -> None:
    if bool(getattr(runtime_session, "closed", False)):
        raise RuntimeError("runtime session is closed")


def vllm_block_name(ref: VllmKVBlockRef) -> str:
    lane = "" if ref.lane_id is None else f":l{ref.lane_id}"
    byte_count = "" if ref.byte_count is None else f":bytes{ref.byte_count}"
    return f"{ref.request_id}:g{ref.group_id}:b{ref.block_id}{lane}{byte_count}"


def make_vllm_block_refs_from_ids(
    request_id: str,
    group_id: int,
    block_ids: Iterable[int],
    cpu_slot_start: int = 0,
) -> list[VllmKVBlockRef]:
    refs = []
    for index, block_id in enumerate(block_ids):
        refs.append(
            VllmKVBlockRef(
                request_id=request_id,
                group_id=group_id,
                block_id=int(block_id),
                cpu_slot=cpu_slot_start + index,
                gpu_slot=int(block_id),
            )
        )
    return refs


def block_bytes_from_vllm_kv_tensor(tensor) -> int:
    """Return bytes for one vLLM KV block in a tensor shaped like [*, blocks, ...]."""

    if len(tensor.shape) < 2:
        raise ValueError("vLLM KV tensor must have at least two dimensions")
    return int(tensor.stride(1) * tensor.element_size())


def make_vllm_layer_groups_from_kv_caches(
    cpu_backings: Iterable,
    kv_caches: Iterable,
    *,
    group_id_start: int = 0,
) -> list[VllmKVGroup]:
    """Create one TurboBus group for each vLLM layer KV cache tensor."""

    groups = []
    for layer_offset, (cpu_backing, kv_cache) in enumerate(zip(cpu_backings, kv_caches)):
        groups.append(
            VllmKVGroup(
                group_id=group_id_start + layer_offset,
                layer_id=layer_offset,
                cpu_backing=cpu_backing,
                gpu_kv_backing=kv_cache,
                block_bytes=block_bytes_from_vllm_kv_tensor(kv_cache),
            )
        )
    return groups


def make_vllm_layer_block_refs_from_ids(
    request_id: str,
    block_ids: Iterable[int],
    layer_count: int,
    cpu_slot_start: int = 0,
) -> list[VllmKVBlockRef]:
    refs = []
    block_ids = [int(block_id) for block_id in block_ids]
    for layer_id in range(layer_count):
        for index, block_id in enumerate(block_ids):
            refs.append(
                VllmKVBlockRef(
                    request_id=request_id,
                    group_id=layer_id,
                    block_id=block_id,
                    cpu_slot=cpu_slot_start + index,
                    gpu_slot=block_id,
                )
            )
    return refs


def make_vllm_layer_range_refs_from_ids(
    request_id: str,
    block_ids: Iterable[int],
    kv_caches: Iterable,
    cpu_slot_start: int = 0,
) -> list[VllmKVBlockRef]:
    """Create byte-range refs for vLLM tensors shaped like [kv, blocks, ...].

    vLLM commonly stores K and V in dimension 0 and block id in dimension 1.
    The K and V ranges for the same block are not necessarily contiguous, so a
    logical KV block can become more than one TurboBus byte range.
    """

    block_ids = [int(block_id) for block_id in block_ids]
    refs = []
    for layer_id, kv_cache in enumerate(kv_caches):
        lane_count = int(kv_cache.shape[0]) if len(kv_cache.shape) >= 3 else 1
        block_bytes = block_bytes_from_vllm_kv_tensor(kv_cache)
        for lane_id in range(lane_count):
            for start_index, run in _contiguous_runs(block_ids):
                block_id = run[0]
                run_blocks = len(run)
                cpu_slot = cpu_slot_start + lane_id * len(block_ids) + start_index
                cpu_offset = cpu_slot * block_bytes
                if lane_count == 1:
                    gpu_offset = block_id * block_bytes
                    lane = None
                else:
                    gpu_offset = int(
                        (lane_id * kv_cache.stride(0) + block_id * kv_cache.stride(1))
                        * kv_cache.element_size()
                    )
                    lane = lane_id
                refs.append(
                    VllmKVBlockRef(
                        request_id=request_id,
                        group_id=layer_id,
                        block_id=block_id,
                        cpu_slot=cpu_slot,
                        gpu_slot=block_id,
                        lane_id=lane,
                        cpu_offset=cpu_offset,
                        gpu_offset=gpu_offset,
                        byte_count=run_blocks * block_bytes,
                    )
                )
    return refs


def _contiguous_runs(block_ids: list[int]) -> list[tuple[int, list[int]]]:
    if not block_ids:
        return []
    runs = []
    start_index = 0
    current = [block_ids[0]]
    for index, block_id in enumerate(block_ids[1:], start=1):
        if block_id == current[-1] + 1:
            current.append(block_id)
            continue
        runs.append((start_index, current))
        start_index = index
        current = [block_id]
    runs.append((start_index, current))
    return runs
