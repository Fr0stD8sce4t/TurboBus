from __future__ import annotations

from dataclasses import dataclass
import logging
import math
from typing import Iterable, Mapping

from ..client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer
from ..offload.context import forbidden_physical_policy_keys
from ..offload.stats import TransferStats, TransferStatsSnapshot
from ..runtime.evidence import validate_transfer_stats_collection
from ..runtime_session import TurboBusRuntimeSession
from ..schema import WorkloadKind
from .inference import InferenceKVSlot, InferenceKVSlotBinding
from ..offload.store import OffloadBatch

logger = logging.getLogger(__name__)


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


class VllmKVSlotBinding:
    """vLLM-shaped wrapper around TurboBus inference KV slot bindings."""

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
        self.slot_bindings = {}
        self.transfer_context: TransferContext | None = None
        created_groups: list[dict[str, object]] = []
        try:
            for group in self.groups.values():
                cpu_buffer = _require_shared_cpu_buffer(group.cpu_backing)
                gpu_buffer = _cuda_buffer_for_group(
                    runtime_session,
                    group,
                    buffer_id=_group_gpu_buffer_id(gpu_buffer_id, group),
                )
                existing_buffers = getattr(runtime_session, "_buffers", {})
                group_resources = {
                    "cpu_buffer_id": cpu_buffer.buffer_id,
                    "cpu_buffer_was_registered": cpu_buffer.buffer_id
                    in existing_buffers,
                    "cpu_buffer": cpu_buffer,
                    "gpu_buffer_id": gpu_buffer.buffer_id,
                    "gpu_buffer_was_registered": gpu_buffer.buffer_id
                    in existing_buffers,
                }
                created_groups.append(group_resources)
                cpu_buffer = runtime_session.register_cpu_buffer(cpu_buffer)
                gpu_buffer = runtime_session.register_cuda_buffer(gpu_buffer)
                group_context = runtime_session.make_transfer_context(
                    cpu_buffer,
                    gpu_buffer,
                    workload_kind=workload_kind,
                    priority=priority,
                    policy_hints={
                        "chunk_bytes": int(runtime_session.runtime_options.chunk_bytes),
                    },
                    metadata=_group_metadata(metadata, group.group_id),
                    intent_prefix=_group_intent_prefix(intent_prefix, group.group_id),
                    wait_timeout_seconds=wait_timeout_seconds,
                )
                slot_binding = InferenceKVSlotBinding._from_transfer_context(
                    runtime_session,
                    group_context,
                    cpu_buffer,
                    gpu_buffer,
                )
                if self.transfer_context is None:
                    self.transfer_context = group_context
                self.slot_bindings[group.group_id] = slot_binding
        except Exception:
            _rollback_group_initialization(runtime_session, created_groups)
            raise
        if self.transfer_context is None:
            raise ValueError("vLLM KV binding requires at least one group")
        self._registered_names: set[str] = set()
        self._request_group_names: dict[str, dict[int, tuple[str, ...]]] = {}

    def lifecycle_group_bindings(self) -> list[dict[str, object]]:
        bindings: list[dict[str, object]] = []
        for group_id, group in sorted(self.groups.items()):
            slot_binding = self.slot_bindings[group_id]
            transfer_context = slot_binding.transfer_context
            bindings.append(
                {
                    "group_id": int(group_id),
                    "layer_id": None if group.layer_id is None else int(group.layer_id),
                    "block_bytes": int(group.block_bytes),
                    "cpu_buffer_id": transfer_context.cpu_buffer_id,
                    "gpu_buffer_id": transfer_context.gpu_buffer_id,
                    "job_id": transfer_context.job_id,
                    "session_id": transfer_context.session_id,
                    "workload_kind": str(transfer_context.workload_kind.value),
                    "intent_prefix": transfer_context.intent_prefix,
                    "policy_source": "daemon_scheduler",
                    "metadata": dict(transfer_context.metadata),
                }
            )
        return bindings

    def register_blocks(self, refs: Iterable[VllmKVBlockRef]) -> list[str]:
        slots_by_group: dict[int, list[InferenceKVSlot]] = {}
        request_group_names: dict[str, dict[int, list[str]]] = {}
        names = []
        for ref in refs:
            group = self.groups[ref.group_id]
            name = vllm_block_name(ref)
            names.append(name)
            request_group_names.setdefault(str(ref.request_id), {}).setdefault(
                int(ref.group_id),
                [],
            ).append(name)
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
            self.slot_bindings[group_id].register_slots(slots)
            for slot in slots:
                self._registered_names.add(slot.name)
        self._merge_request_group_names(request_group_names)
        return names

    def request_ids(self) -> list[str]:
        return sorted(self._request_group_names)

    def block_names_for_request(self, request_id: str) -> list[str]:
        return [
            name
            for _, names in sorted(self._group_names_for_request(request_id).items())
            for name in names
        ]

    def register_request(self, refs: Iterable[VllmKVBlockRef]) -> list[str]:
        return self.register_blocks(refs)

    def restore_prefix(self, refs: Iterable[VllmKVBlockRef]) -> list[OffloadBatch]:
        return self._transfer_prefix(refs, "restore")

    def save_prefix(self, refs: Iterable[VllmKVBlockRef]) -> list[OffloadBatch]:
        return self._transfer_prefix(refs, "save")

    def submit_restore_prefix(self, refs: Iterable[VllmKVBlockRef]) -> list[OffloadBatch]:
        return self._submit_prefix_transfer(refs, "restore")

    def submit_save_prefix(self, refs: Iterable[VllmKVBlockRef]) -> list[OffloadBatch]:
        return self._submit_prefix_transfer(refs, "save")

    def restore_request(self, request_id: str) -> list[OffloadBatch]:
        return self._run_submitted_batches(self.submit_restore_request(request_id))

    def save_request(self, request_id: str) -> list[OffloadBatch]:
        return self._run_submitted_batches(self.submit_save_request(request_id))

    def submit_restore_request(self, request_id: str) -> list[OffloadBatch]:
        return self._submit_request_transfer(request_id, "restore")

    def submit_save_request(self, request_id: str) -> list[OffloadBatch]:
        return self._submit_request_transfer(request_id, "save")

    def _submit_restore_request_evidence_handles(self, request_id: str) -> list:
        return self._submit_request_transfer(request_id, "restore", public=False)

    def _submit_save_request_evidence_handles(self, request_id: str) -> list:
        return self._submit_request_transfer(request_id, "save", public=False)

    def transfer_stats(self, refs: Iterable[VllmKVBlockRef]) -> TransferStatsSnapshot:
        names_by_group = self._register_and_group(refs)
        return self._transfer_stats_snapshot_from_group_names(names_by_group)

    def transfer_stats_for_request(self, request_id: str) -> TransferStatsSnapshot:
        return self._transfer_stats_snapshot_from_group_names(
            self._group_names_for_request(request_id),
        )

    def forget_request(self, request_id: str) -> tuple[str, ...]:
        grouped_names = self._request_group_names.pop(str(request_id), None)
        if grouped_names is None:
            return ()
        removed: list[str] = []
        for group_id, names in sorted(grouped_names.items()):
            slot_binding = self.slot_bindings.get(group_id)
            if slot_binding is None:
                continue
            for name in names:
                try:
                    slot_binding.remove(name)
                except KeyError:
                    pass
                self._registered_names.discard(name)
                removed.append(name)
        return tuple(removed)

    def _transfer_prefix(
        self,
        refs: Iterable[VllmKVBlockRef],
        operation: str,
    ) -> list[OffloadBatch]:
        batches = self._submit_prefix_transfer(refs, operation)
        self._wait_batches(batches)
        return batches

    def _submit_prefix_transfer(
        self,
        refs: Iterable[VllmKVBlockRef],
        operation: str,
    ) -> list[OffloadBatch]:
        refs = list(refs)
        names_by_group = self._register_and_group(refs)
        batches = []
        submit_method = "submit_restore_prefix" if operation == "restore" else "submit_save_prefix"
        for group_id, names in names_by_group.items():
            submit = getattr(self.slot_bindings[group_id], submit_method)
            batch = submit(names)
            batches.append(batch)
        return batches

    def _submit_request_transfer(
        self,
        request_id: str,
        operation: str,
        *,
        public: bool = True,
    ) -> list[OffloadBatch]:
        batches = []
        for group_id, names in self._group_names_for_request(request_id).items():
            if public:
                submit_method = (
                    "submit_restore_prefix"
                    if operation == "restore"
                    else "submit_save_prefix"
                )
                submit = getattr(self.slot_bindings[group_id], submit_method)
                batch = submit(names)
            else:
                batch_method = (
                    "submit_restore_batch"
                    if operation == "restore"
                    else "submit_save_batch"
                )
                batch = getattr(self.slot_bindings[group_id], batch_method)(names)
            batches.append(batch)
        return batches

    @staticmethod
    def _wait_batches(batches: Iterable[OffloadBatch]) -> None:
        seen = set()
        for batch in batches:
            batch_id = id(batch)
            if batch_id in seen:
                continue
            seen.add(batch_id)
            waiter = getattr(batch, "wait", None)
            if not callable(waiter):
                raise TypeError("vLLM TurboBus prefix batch must expose wait()")
            waiter()

    @staticmethod
    def _run_submitted_batches(batches: Iterable[OffloadBatch]) -> list[OffloadBatch]:
        resolved = list(batches)
        VllmKVSlotBinding._wait_batches(resolved)
        return resolved

    @staticmethod
    def _sum_transfer_stats(total: TransferStats, stats: TransferStats) -> TransferStats:
        return TransferStats(
            bytes=total.bytes + stats.bytes,
            direct_chunks=total.direct_chunks + stats.direct_chunks,
            relay_chunks=total.relay_chunks + stats.relay_chunks,
        )

    def _transfer_stats_snapshot_from_group_names(
        self,
        names_by_group: Mapping[int, Iterable[str]],
    ) -> TransferStatsSnapshot:
        # /*
        #  * ========================================================================
        #  * 步骤1：聚合 RuntimeSession 绑定 vLLM stats 快照
        #  * ========================================================================
        #  * 数据源：每个 group 的 InferenceKVSlotBinding transfer stats snapshot
        #  * 操作：
        #  *   1) 收集每个 group 的 RuntimeSession transfer evidence
        #  *   2) 只聚合已绑定 evidence 的统计摘要，不开放 route policy
        #  */
        logger.info("开始聚合 RuntimeSession 绑定 vLLM stats 快照...")

        # // 1.1 读取 group-level evidence-bound stats
        group_snapshots: list[dict[str, object]] = []
        total = TransferStats()
        for group_id, names in names_by_group.items():
            stats = self.slot_bindings[int(group_id)].transfer_stats(tuple(names))
            group_snapshot = stats.as_dict()
            group_snapshot["group_id"] = int(group_id)
            group_snapshots.append(group_snapshot)
            total = self._sum_transfer_stats(total, stats)

        # // 1.2 构造 vLLM 聚合快照
        snapshot = {
            "transfer_state": "runtime_session_bound",
            "binding": "vllm_kv_slot_binding",
            "group_count": len(group_snapshots),
            "groups": group_snapshots,
            "bytes": int(total.bytes),
            "direct_chunks": int(total.direct_chunks),
            "relay_chunks": int(total.relay_chunks),
            "receipt_count": sum(
                int(item.get("receipt_count", 0) or 0) for item in group_snapshots
            ),
            "receipt_states": _join_snapshot_csv(group_snapshots, "receipt_states"),
            "direct_bytes": sum(
                int(item.get("direct_bytes", 0) or 0) for item in group_snapshots
            ),
            "relay_bytes": sum(
                int(item.get("relay_bytes", 0) or 0) for item in group_snapshots
            ),
            "route_policy_visible_to_transfer": False,
        }
        logger.info(
            "RuntimeSession 绑定 vLLM stats 快照聚合完成, receipts: %s",
            snapshot["receipt_count"],
        )
        validate_transfer_stats_collection(snapshot)
        return TransferStatsSnapshot(snapshot)

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

    def _group_names_for_request(self, request_id: str) -> dict[int, tuple[str, ...]]:
        grouped_names = self._request_group_names.get(str(request_id))
        if grouped_names is None:
            raise KeyError(f"unknown vLLM request: {request_id}")
        return grouped_names

    def _merge_request_group_names(
        self,
        request_group_names: Mapping[str, Mapping[int, list[str]]],
    ) -> None:
        for request_id, grouped_names in request_group_names.items():
            existing = self._request_group_names.get(str(request_id), {})
            merged: dict[int, tuple[str, ...]] = dict(existing)
            for group_id, names in grouped_names.items():
                ordered: list[str] = []
                seen = set()
                for name in (
                    *existing.get(int(group_id), ()),
                    *(str(item) for item in names),
                ):
                    if name in seen:
                        continue
                    seen.add(name)
                    ordered.append(name)
                merged[int(group_id)] = tuple(ordered)
            self._request_group_names[str(request_id)] = merged

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
    invalid_keys = forbidden_physical_policy_keys(metadata)
    if invalid_keys:
        raise ValueError(
            "vLLM connector metadata must not choose physical paths: "
            + ", ".join(str(key) for key in invalid_keys)
        )
    metadata["group_id"] = int(group_id)
    return metadata


def _join_snapshot_csv(
    snapshots: Iterable[Mapping[str, object]],
    field_name: str,
) -> str:
    # /*
    #  * ========================================================================
    #  * 步骤2：聚合 stats 快照标识字段
    #  * ========================================================================
    #  * 数据源：group-level RuntimeSession stats snapshots
    #  * 操作：
    #  *   1) 读取每个 group 的 CSV 标识字段
    #  *   2) 保持去重顺序，用于 vLLM 聚合 evidence 摘要
    #  */
    logger.info("开始聚合 stats 快照标识字段...")

    # // 2.1 顺序去重拼接 CSV 字段
    seen: set[str] = set()
    ordered: list[str] = []
    for snapshot in snapshots:
        for value in str(snapshot.get(field_name, "")).split(","):
            normalized = value.strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            ordered.append(normalized)
    result = ",".join(ordered)
    logger.info("stats 快照标识字段聚合完成, field: %s", field_name)
    return result


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


def _rollback_group_initialization(
    runtime_session,
    created_groups: list[dict[str, object]],
) -> None:
    for group_resources in reversed(created_groups):
        _rollback_registered_buffer(
            runtime_session,
            group_resources["cpu_buffer"],
            was_registered=bool(group_resources["cpu_buffer_was_registered"]),
        )
        _rollback_registered_buffer(
            runtime_session,
            group_resources["gpu_buffer_id"],
            was_registered=bool(group_resources["gpu_buffer_was_registered"]),
        )


def _rollback_registered_buffer(
    runtime_session,
    buffer,
    *,
    was_registered: bool,
) -> None:
    buffer_id = str(getattr(buffer, "buffer_id", buffer))
    if was_registered:
        try:
            if not bool(getattr(runtime_session, "closed", False)):
                runtime_session.cleanup_buffer(
                    buffer_id,
                    reason="runtime_vllm_binding_creation_failed",
                    force=True,
                )
                return
        except Exception:
            pass
    _discard_runtime_session_buffer(runtime_session, buffer_id)
    if isinstance(buffer, SharedPinnedCpuBuffer) and bool(getattr(buffer, "owner", False)):
        try:
            buffer.release()
        except Exception:
            pass


def _discard_runtime_session_buffer(runtime_session, buffer_id: str) -> None:
    buffers = getattr(runtime_session, "_buffers", None)
    if isinstance(buffers, dict):
        buffers.pop(buffer_id, None)
    registered_ids = getattr(runtime_session, "_registered_buffer_ids", None)
    if isinstance(registered_ids, set):
        registered_ids.discard(buffer_id)
    fingerprints = getattr(runtime_session, "_registered_buffer_fingerprints", None)
    if isinstance(fingerprints, dict):
        fingerprints.pop(buffer_id, None)
    owned_ids = getattr(runtime_session, "_owned_cpu_buffer_ids", None)
    if isinstance(owned_ids, set):
        owned_ids.discard(buffer_id)


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
    if not isinstance(runtime_session, TurboBusRuntimeSession):
        raise TypeError("vLLM KV binding requires a TurboBusRuntimeSession")
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

