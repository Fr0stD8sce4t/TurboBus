from __future__ import annotations

import functools
import itertools
from dataclasses import dataclass, field
from typing import Callable, Iterable, Mapping

from ..client import SharedPinnedCpuBuffer
from ..runtime_session import TurboBusRuntimeSession
from ..schema import WorkloadKind
from .vllm import (
    VllmKVBlockRef,
    VllmKVSlotAdapter,
    block_bytes_from_vllm_kv_tensor,
    make_vllm_layer_groups_from_kv_caches,
    make_vllm_layer_range_refs_from_ids,
)


@dataclass(frozen=True)
class VllmAllocationEvent:
    """Block ids that vLLM allocated for one request."""

    request_id: str
    block_ids_by_group: tuple[tuple[int, ...], ...]
    event_count: int = 1

    @property
    def block_ids(self) -> tuple[int, ...]:
        seen = set()
        ordered = []
        for group_ids in self.block_ids_by_group:
            for block_id in group_ids:
                if block_id not in seen:
                    seen.add(block_id)
                    ordered.append(block_id)
        return tuple(ordered)

    def merge(self, other: "VllmAllocationEvent") -> "VllmAllocationEvent":
        if other.request_id != self.request_id:
            raise ValueError("cannot merge allocation events for different requests")
        group_count = max(len(self.block_ids_by_group), len(other.block_ids_by_group))
        merged = []
        for group_index in range(group_count):
            left = (
                self.block_ids_by_group[group_index]
                if group_index < len(self.block_ids_by_group)
                else tuple()
            )
            right = (
                other.block_ids_by_group[group_index]
                if group_index < len(other.block_ids_by_group)
                else tuple()
            )
            seen = set()
            ordered = []
            for block_id in (*left, *right):
                if block_id not in seen:
                    seen.add(block_id)
                    ordered.append(block_id)
            merged.append(tuple(ordered))
        return VllmAllocationEvent(
            self.request_id,
            tuple(merged),
            event_count=self.event_count + other.event_count,
        )


AllocationCallback = Callable[
    ["VllmTurboBusIntegration", object, object, VllmAllocationEvent],
    None,
]


@dataclass
class VllmIntegrationState:
    """Runtime state observed from a real vLLM process."""

    kv_cache_config: object | None = None
    kv_caches: list[object] = field(default_factory=list)
    allocations: dict[str, VllmAllocationEvent] = field(default_factory=dict)
    request_cpu_slot_starts: dict[str, int] = field(default_factory=dict)
    adapter: VllmKVSlotAdapter | None = None


class VllmTurboBusIntegration:
    """Narrow TurboBus data-path hook for vLLM-owned KV cache slots.

    vLLM still owns scheduling, request state, and GPU KV allocation. This hook
    observes the real vLLM KV tensors and block ids, then maps those slots to
    TurboBus restore/save operations.
    """

    def __init__(
        self,
        runtime_session,
        cpu_backings: Iterable | None = None,
        *,
        workload_kind: WorkloadKind | str = WorkloadKind.KV_CACHE,
        priority: int = 0,
        metadata: Mapping[str, object] | None = None,
        intent_prefix: str | None = None,
        wait_timeout_seconds: float | None = None,
        cpu_buffer_id: str = "vllm-kv-cpu",
        gpu_buffer_id: str = "vllm-kv-gpu",
    ) -> None:
        _require_runtime_session_open(runtime_session)
        self.runtime_session = runtime_session
        self.state = VllmIntegrationState()
        self._cpu_backings = list(cpu_backings) if cpu_backings is not None else None
        self._next_cpu_backing_id = itertools.count(1)
        self._cpu_buffer_id_prefix = str(cpu_buffer_id)
        self._runtime_adapter_options = {
            "workload_kind": workload_kind,
            "priority": int(priority),
            "metadata": {} if metadata is None else dict(metadata),
            "intent_prefix": intent_prefix,
            "wait_timeout_seconds": wait_timeout_seconds,
            "gpu_buffer_id": str(gpu_buffer_id),
        }
        self._allocation_callback: AllocationCallback | None = None

    def install(self) -> None:
        """Install hooks into the imported vLLM V1 classes."""

        from vllm.v1.core import kv_cache_manager as manager_module
        from vllm.v1.worker import gpu_model_runner as runner_module

        self.install_on_classes(
            runner_module.GPUModelRunner,
            manager_module.KVCacheManager,
        )

    def install_on_classes(self, runner_cls, manager_cls) -> None:
        """Install hooks on explicit classes.

        This method exists so tests and version-specific integration code can
        patch the exact classes used by the active vLLM build.
        """

        runner_cls._turbobus_integration = self
        manager_cls._turbobus_integration = self

        if not hasattr(runner_cls, "_turbobus_original_initialize_kv_cache"):
            runner_cls._turbobus_original_initialize_kv_cache = runner_cls.initialize_kv_cache
            original_initialize = runner_cls.initialize_kv_cache

            @functools.wraps(original_initialize)
            def wrapped_initialize(runner, kv_cache_config, *args, **kwargs):
                result = original_initialize(runner, kv_cache_config, *args, **kwargs)
                integration = getattr(type(runner), "_turbobus_integration", None)
                if integration is not None:
                    integration.bind_runner(runner, kv_cache_config)
                return result

            runner_cls.initialize_kv_cache = wrapped_initialize

        if not hasattr(manager_cls, "_turbobus_original_allocate_slots"):
            manager_cls._turbobus_original_allocate_slots = manager_cls.allocate_slots
            original_allocate = manager_cls.allocate_slots

            @functools.wraps(original_allocate)
            def wrapped_allocate(manager, request, *args, **kwargs):
                result = original_allocate(manager, request, *args, **kwargs)
                integration = getattr(type(manager), "_turbobus_integration", None)
                if integration is not None:
                    integration.handle_allocation(request, result)
                return result

            manager_cls.allocate_slots = wrapped_allocate

    def bind_runner(self, runner, kv_cache_config=None) -> None:
        kv_caches = list(getattr(runner, "kv_caches", []) or [])
        self.bind_kv_caches(kv_caches, kv_cache_config)

    def bind_kv_caches(self, kv_caches: Iterable, kv_cache_config=None) -> None:
        self.state.kv_cache_config = kv_cache_config
        self.state.kv_caches = list(kv_caches)
        self._refresh_adapter()

    def set_cpu_backings(self, cpu_backings: Iterable) -> None:
        self._cpu_backings = list(cpu_backings)
        self._refresh_adapter()

    def set_allocation_callback(self, callback: AllocationCallback | None) -> None:
        self._allocation_callback = callback

    def allocate_cpu_backings(self, slots_per_layer: int, *, pin_memory: bool = True) -> list:
        """Allocate shared CPU buffers for the observed vLLM layer caches."""

        if not pin_memory:
            raise ValueError("runtime session vLLM backings must be shared pinned buffers")

        backings = []
        for kv_cache in self.state.kv_caches:
            block_bytes = block_bytes_from_vllm_kv_tensor(kv_cache)
            backings.append(
                SharedPinnedCpuBuffer.allocate(
                    buffer_id=(
                        f"{self._cpu_buffer_id_prefix}-"
                        f"{next(self._next_cpu_backing_id)}"
                    ),
                    job_id=self.runtime_session.job_id,
                    size_bytes=slots_per_layer * block_bytes,
                    name_prefix="turbobus-vllm",
                )
            )
        self.set_cpu_backings(backings)
        return backings

    def record_allocation(self, request, blocks) -> VllmAllocationEvent | None:
        request_id = str(getattr(request, "request_id", "unknown"))
        block_ids_by_group = extract_vllm_block_ids(blocks)
        if not block_ids_by_group:
            return None
        event = VllmAllocationEvent(request_id, block_ids_by_group)
        return self._store_allocation_event(event)

    def record_request_blocks(
        self,
        request_id: str,
        block_ids: Iterable[int],
    ) -> VllmAllocationEvent | None:
        normalized = tuple(int(block_id) for block_id in block_ids)
        if not normalized:
            return None
        event = VllmAllocationEvent(str(request_id), (normalized,))
        return self._store_allocation_event(event)

    def _store_allocation_event(self, event: VllmAllocationEvent) -> VllmAllocationEvent:
        previous = self.state.allocations.get(event.request_id)
        if previous is not None:
            event = previous.merge(event)
        self.state.allocations[event.request_id] = event
        return event

    def handle_allocation(self, request, blocks) -> VllmAllocationEvent | None:
        event = self.record_allocation(request, blocks)
        if event is not None and self._allocation_callback is not None:
            self._allocation_callback(self, request, blocks, event)
        return event

    def block_ids_for_request(self, request_id: str) -> tuple[int, ...]:
        event = self._allocation_for_request(request_id)
        return event.block_ids

    def block_ids_by_group_for_request(self, request_id: str) -> tuple[tuple[int, ...], ...]:
        event = self._allocation_for_request(request_id)
        return event.block_ids_by_group

    def request_ids(self) -> list[str]:
        return sorted(self.state.allocations)

    def register_request(
        self,
        request_id: str,
        *,
        cpu_slot_start: int = 0,
    ) -> list[str]:
        request_id = str(request_id)
        slot_start = self._resolve_cpu_slot_start(request_id, cpu_slot_start)
        refs = self.make_refs_for_request(request_id, cpu_slot_start=slot_start)
        names = self.require_adapter().register_request(refs)
        self.state.request_cpu_slot_starts[request_id] = slot_start
        return names

    def lifecycle_request_binding(
        self,
        request_id: str,
        *,
        cpu_slot_start: int = 0,
    ) -> dict[str, object]:
        request_id = str(request_id)
        slot_start = self._resolve_cpu_slot_start(request_id, cpu_slot_start)
        refs = self.make_refs_for_request(request_id, cpu_slot_start=slot_start)
        adapter = self.require_adapter()
        return {
            "request_id": request_id,
            "cpu_slot_start": int(slot_start),
            "block_ids": list(self.block_ids_for_request(request_id)),
            "block_ids_by_group": [
                list(group) for group in self.block_ids_by_group_for_request(request_id)
            ],
            "kv_cache_count": len(self.state.kv_caches),
            "cpu_backing_count": (
                0 if self._cpu_backings is None else len(self._cpu_backings)
            ),
            "range_refs": [_vllm_ref_lifecycle(ref) for ref in refs],
            "registered_block_names": adapter.block_names_for_request(request_id),
            "adapter_group_bindings": adapter.lifecycle_group_bindings(),
        }

    def block_names_for_request(
        self,
        request_id: str,
        *,
        cpu_slot_start: int = 0,
    ) -> list[str]:
        self.register_request(request_id, cpu_slot_start=cpu_slot_start)
        return self.require_adapter().block_names_for_request(str(request_id))

    def transfer_stats_for_request(
        self,
        request_id: str,
        *,
        cpu_slot_start: int = 0,
    ):
        self.register_request(request_id, cpu_slot_start=cpu_slot_start)
        return self.require_adapter().transfer_stats_for_request(str(request_id))

    def forget_request(self, request_id: str) -> tuple[str, ...]:
        request_id = str(request_id)
        self.state.allocations.pop(request_id, None)
        self.state.request_cpu_slot_starts.pop(request_id, None)
        adapter = self.state.adapter
        if adapter is None:
            return ()
        return adapter.forget_request(request_id)

    def make_refs_for_request(
        self,
        request_id: str,
        *,
        cpu_slot_start: int = 0,
    ) -> list[VllmKVBlockRef]:
        if not self.state.kv_caches:
            raise RuntimeError("vLLM KV caches must be bound before request restore/save")
        return make_vllm_layer_range_refs_from_ids(
            str(request_id),
            self.block_ids_for_request(request_id),
            self.state.kv_caches,
            cpu_slot_start=cpu_slot_start,
        )

    def restore_request_prefix(self, request_id: str, *, cpu_slot_start: int = 0) -> list:
        self.register_request(request_id, cpu_slot_start=cpu_slot_start)
        return self.require_adapter().restore_request(str(request_id))

    def submit_restore_request_prefix(
        self,
        request_id: str,
        *,
        cpu_slot_start: int = 0,
    ) -> list:
        self.register_request(request_id, cpu_slot_start=cpu_slot_start)
        return self.require_adapter().submit_restore_request(str(request_id))

    def save_request_prefix(self, request_id: str, *, cpu_slot_start: int = 0) -> list:
        self.register_request(request_id, cpu_slot_start=cpu_slot_start)
        return self.require_adapter().save_request(str(request_id))

    def submit_save_request_prefix(
        self,
        request_id: str,
        *,
        cpu_slot_start: int = 0,
    ) -> list:
        self.register_request(request_id, cpu_slot_start=cpu_slot_start)
        return self.require_adapter().submit_save_request(str(request_id))

    def _submit_restore_request_evidence_handles(
        self,
        request_id: str,
        *,
        cpu_slot_start: int = 0,
    ) -> list:
        self.register_request(request_id, cpu_slot_start=cpu_slot_start)
        return self.require_adapter()._submit_restore_request_evidence_handles(
            str(request_id)
        )

    def _submit_save_request_evidence_handles(
        self,
        request_id: str,
        *,
        cpu_slot_start: int = 0,
    ) -> list:
        self.register_request(request_id, cpu_slot_start=cpu_slot_start)
        return self.require_adapter()._submit_save_request_evidence_handles(
            str(request_id)
        )

    def _refresh_adapter(self) -> None:
        if not self.state.kv_caches or self._cpu_backings is None:
            self.state.adapter = None
            return
        if len(self._cpu_backings) != len(self.state.kv_caches):
            raise ValueError("cpu_backings must match the number of vLLM KV cache tensors")
        groups = make_vllm_layer_groups_from_kv_caches(
            self._cpu_backings,
            self.state.kv_caches,
        )
        self.state.adapter = self.runtime_session.make_vllm_kv_slot_adapter(
            groups,
            **self._runtime_adapter_options,
        )

    def require_adapter(self) -> VllmKVSlotAdapter:
        if self.state.adapter is None:
            raise RuntimeError("vLLM KV caches and CPU backings must be bound before restore/save")
        return self.state.adapter

    _require_adapter = require_adapter

    def _allocation_for_request(self, request_id: str) -> VllmAllocationEvent:
        request_id = str(request_id)
        try:
            return self.state.allocations[request_id]
        except KeyError as exc:
            raise KeyError(f"unknown vLLM request: {request_id}") from exc

    def _resolve_cpu_slot_start(self, request_id: str, cpu_slot_start: int) -> int:
        request_id = str(request_id)
        requested = int(cpu_slot_start)
        existing = self.state.request_cpu_slot_starts.get(request_id)
        if existing is None:
            return requested
        if requested not in (0, existing):
            raise ValueError(
                f"vLLM request {request_id} is already registered with "
                f"cpu_slot_start={existing}"
            )
        return existing


def extract_vllm_block_ids(blocks) -> tuple[tuple[int, ...], ...]:
    if blocks is None:
        return tuple()
    get_block_ids = getattr(blocks, "get_block_ids", None)
    if get_block_ids is None:
        raw = getattr(blocks, "block_ids", blocks)
    else:
        try:
            raw = get_block_ids(allow_none=True)
        except TypeError:
            raw = get_block_ids()
    return _normalize_block_id_groups(raw)


def _normalize_block_id_groups(raw) -> tuple[tuple[int, ...], ...]:
    if raw is None:
        return tuple()
    if isinstance(raw, tuple):
        if all(isinstance(item, int) or item is None for item in raw):
            return (tuple(int(item) for item in raw if item is not None),)
        return tuple(_normalize_block_id_group(group) for group in raw)
    if isinstance(raw, list):
        if not raw:
            return tuple()
        if all(isinstance(item, int) or item is None for item in raw):
            return (tuple(int(item) for item in raw if item is not None),)
        return tuple(_normalize_block_id_group(group) for group in raw)
    return tuple(_normalize_block_id_group(raw),) if raw is not None else tuple()


def _normalize_block_id_group(group) -> tuple[int, ...]:
    if group is None:
        return tuple()
    if isinstance(group, int):
        return (int(group),)
    return tuple(int(block_id) for block_id in group if block_id is not None)


def _require_runtime_session_open(runtime_session) -> None:
    if not isinstance(runtime_session, TurboBusRuntimeSession):
        raise TypeError("vLLM integration requires a TurboBusRuntimeSession")
    if bool(getattr(runtime_session, "closed", False)):
        raise RuntimeError("runtime session is closed")


def _vllm_ref_lifecycle(ref: VllmKVBlockRef) -> dict[str, object]:
    return {
        "request_id": ref.request_id,
        "group_id": int(ref.group_id),
        "block_id": int(ref.block_id),
        "cpu_slot": int(ref.cpu_slot),
        "gpu_slot": int(ref.gpu_slot),
        "lane_id": None if ref.lane_id is None else int(ref.lane_id),
        "cpu_offset": None if ref.cpu_offset is None else int(ref.cpu_offset),
        "gpu_offset": None if ref.gpu_offset is None else int(ref.gpu_offset),
        "byte_count": None if ref.byte_count is None else int(ref.byte_count),
    }
