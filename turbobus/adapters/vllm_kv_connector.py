from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Any, Mapping

from ..offload.context import forbidden_physical_policy_keys
from ..offload.stats import TransferStats
from ..offload.lifecycle import runtime_session_receipt_trace_from_receipts
from ..runtime_session import TurboBusRuntimeSession
from ..runtime_options import RuntimeOptions
from ..schema import TransferReceipt, WorkloadKind
from .vllm_backing_pool import TurboBusCPUBackingPool
from .vllm_config import TurboBusConnectorConfig
from .vllm_events import (
    clear_connector_events,
    emit_event as _emit_event,
    get_connector_events,
)
from .vllm_integration import VllmTurboBusIntegration, extract_vllm_block_ids
from .vllm_prefix_store import (
    TurboBusPrefixStore,
    TurboBusRequestMetadata,
    TurboBusSavedPrefix,
    _clear_saved_prefixes_for_connector,
    _get_saved_prefix_for_connector,
    _remove_saved_prefix_for_connector,
    _store_saved_prefix_for_connector,
    get_saved_prefix,
)

try:  # pragma: no cover - depends on an installed vLLM build
    from vllm.distributed.kv_transfer.kv_connector.v1.base import (
        KVConnectorBase_V1,
        KVConnectorMetadata,
        KVConnectorRole,
    )
    try:
        from vllm.distributed.kv_transfer.kv_connector.v1.base import SupportsHMA
    except ImportError:
        SupportsHMA = object
except ImportError:  # pragma: no cover - lets unit tests import without vLLM
    class KVConnectorMetadata:
        pass

    class KVConnectorBase_V1:
        def __init__(self, vllm_config, role, kv_cache_config=None):
            self._vllm_config = vllm_config
            self._role = role
            self._connector_metadata = None

        def bind_connector_metadata(self, metadata):
            self._connector_metadata = metadata

        def clear_connector_metadata(self):
            self._connector_metadata = None

        def has_connector_metadata(self):
            return self._connector_metadata is not None

    class KVConnectorRole:
        SCHEDULER = "scheduler"
        WORKER = "worker"

    SupportsHMA = object


@dataclass
class _ScheduledRequestView:
    req_id: str
    new_block_ids: Any
    kv_transfer_params: dict[str, Any]


@dataclass
class _LayerSaveContext:
    request: TurboBusRequestMetadata
    cpu_backings: list[Any]
    kv_caches: list[Any]
    integration: VllmTurboBusIntegration
    reused_backing: bool
    total_start: float
    client_init_ms: float = 0.0
    cpu_alloc_ms: float = 0.0
    group_ms: float = 0.0
    adapter_ms: float = 0.0
    refs_ms: float = 0.0
    transfer_ms: float = 0.0
    bytes: int = 0
    direct_chunks: int = 0
    relay_chunks: int = 0
    direct_bytes: int = 0
    relay_bytes: int = 0
    receipt_ids: list[str] = field(default_factory=list)
    decision_ids: list[str] = field(default_factory=list)
    topology_snapshot_ids: list[str] = field(default_factory=list)
    ticket_ids: list[str] = field(default_factory=list)
    fallback_reasons: list[str] = field(default_factory=list)
    ranges: int = 0
    ready_layers: set[int] = field(default_factory=set)
    request_binding: dict[str, Any] = field(default_factory=dict)


class TurboBusConnectorMetadata(KVConnectorMetadata):
    def __init__(self) -> None:
        super().__init__()
        self.requests: list[TurboBusRequestMetadata] = []
        self.save_requests: list[TurboBusRequestMetadata] = []

    def add_request(self, request: TurboBusRequestMetadata) -> None:
        self.requests.append(request)

    def add_save_request(self, request: TurboBusRequestMetadata) -> None:
        self.save_requests.append(request)

    def __len__(self) -> int:
        return len(self.requests) + len(self.save_requests)


@dataclass
class TurboBusKVConnectorState:
    kv_caches: dict[str, Any] = field(default_factory=dict)
    pending_loads: dict[str, TurboBusRequestMetadata] = field(default_factory=dict)
    pending_saves: dict[str, TurboBusRequestMetadata] = field(default_factory=dict)
    save_params_by_request_id: dict[str, dict[str, Any]] = field(default_factory=dict)
    save_request_ids: set[str] = field(default_factory=set)
    saved_request_ids: set[str] = field(default_factory=set)
    finished_sending: set[str] = field(default_factory=set)
    finished_recving: set[str] = field(default_factory=set)
    events: list[dict[str, Any]] = field(default_factory=list)


class TurboBusConnector(KVConnectorBase_V1, SupportsHMA):
    """vLLM KV connector entry point for TurboBus prefix restore.

    This uses vLLM's KV-transfer connector lifecycle instead of replacing the
    scheduler. A request opts in with `kv_transfer_params`:

    {
      "turbobus.do_restore": true,
      "turbobus.matched_tokens": 128
    }
    """

    @classmethod
    def requires_piecewise_for_cudagraph(cls, extra_config: dict[str, Any]) -> bool:
        return True

    def __init__(
        self,
        vllm_config,
        role: KVConnectorRole,
        kv_cache_config=None,
    ) -> None:
        try:
            super().__init__(
                vllm_config=vllm_config,
                role=role,
                kv_cache_config=kv_cache_config,
            )
        except TypeError:
            super().__init__(vllm_config=vllm_config, role=role)
        self.state = TurboBusKVConnectorState()
        self.vllm_block_size = int(getattr(vllm_config.cache_config, "block_size", 16))
        self.config = TurboBusConnectorConfig.from_vllm_config(vllm_config)
        self.restore_block_limit = self.config.restore_block_limit
        self.restore_enabled = self.config.restore_enabled
        self.connector_session_id = self.config.session_id
        self.job_id = self.config.job_id
        self.max_saved_prefixes = self.config.max_saved_prefixes
        self.runtime_session = TurboBusRuntimeSession.open_production_socket(
            job_id=self.job_id,
            runtime_options=RuntimeOptions(
                chunk_bytes=int(self.config.chunk_bytes),
                profile_bytes=int(self.config.profile_bytes),
                min_pool_bytes=int(self.config.min_pool_bytes),
                min_chunks_for_relay=int(self.config.min_chunks_for_relay),
                relay_min_effective_bw_gbps=float(
                    self.config.relay_min_effective_bw_gbps
                ),
                relay_min_direct_ratio=float(self.config.relay_min_direct_ratio),
                enable_dynamic_weights=bool(self.config.enable_dynamic_weights),
                dynamic_weight_alpha=float(self.config.dynamic_weight_alpha),
                worker_runtime_cache_entries=int(self.config.runtime_cache_entries),
                worker_terminal_history_entries=int(
                    self.config.terminal_history_entries
                ),
                clear_relay_staging_on_chunk=bool(
                    self.config.clear_relay_staging_on_chunk
                ),
                daemon_socket_path=self.config.daemon_socket_path,
                worker_socket_path=self.config.worker_socket_path,
            ),
        )
        self.session_id = self.connector_session_id
        self._layer_save_contexts: dict[str, _LayerSaveContext] = {}
        self._backing_pool = TurboBusCPUBackingPool(
            job_id=self.job_id,
            buffer_id_prefix=self.config.cpu_buffer_id,
        )
        self._prefix_store = TurboBusPrefixStore(max_prefixes=self.max_saved_prefixes)
        self._closed = False
        _emit_event(
            "init",
            role=str(role),
            job_id=self.job_id,
            session_id=self.session_id,
            connector_session_id=self.connector_session_id,
            restore_enabled=self.restore_enabled,
            restore_block_limit=self.restore_block_limit,
            max_saved_prefixes=self.max_saved_prefixes,
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        closed_prefixes = self._close_saved_prefixes()
        closed_pending_saves = self._close_pending_save_contexts()
        closed_pending_metadata = self._close_pending_metadata()
        _clear_saved_prefixes_for_connector(self.session_id, job_id=self.job_id)
        runtime_close_response = self.runtime_session.close()
        runtime_close_entrypoint = _runtime_close_entrypoint_for_connector_close(
            runtime_close_response.payload
        )
        free_backing_cleanup = self._backing_pool.close(
            runtime_close_entrypoint=runtime_close_entrypoint
        )
        clear_metadata = getattr(self, "clear_connector_metadata", None)
        if callable(clear_metadata):
            clear_metadata()
        events = list(self.state.events)
        events.append(
            {
                "event": "close",
                "job_id": self.job_id,
                "session_id": self.session_id,
                "connector_session_id": self.connector_session_id,
                "prefixes": closed_prefixes["count"],
                "prefix_cleanup_mutation_ids": closed_prefixes["cleanup_mutation_ids"],
                "adapter_evidence_records": closed_prefixes[
                    "adapter_evidence_records"
                ],
                "free_backing_cleanup": free_backing_cleanup,
                "runtime_close_entrypoint": runtime_close_entrypoint,
                "route_policy_visible_to_adapter": False,
                "pending_saves": closed_pending_saves,
                **closed_pending_metadata,
            }
        )
        self.state = TurboBusKVConnectorState(events=events)
        _emit_event(
            "close",
            job_id=self.job_id,
            session_id=self.session_id,
            connector_session_id=self.connector_session_id,
            prefixes=closed_prefixes["count"],
            prefix_cleanup_mutation_ids=",".join(closed_prefixes["cleanup_mutation_ids"]),
            adapter_evidence_records=closed_prefixes["adapter_evidence_records"],
            free_backing_cleanup_groups=len(free_backing_cleanup),
            runtime_close_entrypoint=runtime_close_entrypoint,
            route_policy_visible_to_adapter=False,
            pending_saves=closed_pending_saves,
            **closed_pending_metadata,
        )

    def shutdown(self) -> None:
        self.close()

    def __enter__(self) -> "TurboBusConnector":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def register_kv_caches(self, kv_caches: dict[str, Any]) -> None:
        self.state.kv_caches = dict(kv_caches)
        self.state.events.append(
            {
                "event": "register_kv_caches",
                "layers": len(self.state.kv_caches),
            }
        )
        _emit_event("register_kv_caches", layers=len(self.state.kv_caches))

    def get_num_new_matched_tokens(
        self,
        request,
        num_computed_tokens: int,
    ) -> tuple[int | None, bool]:
        params = _request_params(request)
        if not params.get("turbobus.do_restore"):
            return 0, False
        if not self.restore_enabled:
            self.state.events.append(
                {
                    "event": "match_skipped",
                    "request_id": str(getattr(request, "request_id", "unknown")),
                    "restore_enabled": False,
                }
            )
            _emit_event(
                "match_skipped",
                request_id=str(getattr(request, "request_id", "unknown")),
                restore_enabled=False,
            )
            return 0, False
        prefix_key = _request_prefix_key(params)
        saved = _get_saved_prefix_for_connector(
            prefix_key,
            self.session_id,
            job_id=self.job_id,
        )
        if saved is None:
            self.state.events.append(
                {
                    "event": "match_miss",
                    "request_id": str(getattr(request, "request_id", "unknown")),
                    "prefix_key": prefix_key,
                    "session_id": self.session_id,
                }
            )
            _emit_event(
                "match_miss",
                request_id=str(getattr(request, "request_id", "unknown")),
                prefix_key=prefix_key,
                session_id=self.session_id,
            )
            return 0, False
        matched_tokens = int(params.get("turbobus.matched_tokens", 0))
        if matched_tokens <= 0:
            matched_tokens = saved.matched_tokens
        matched_tokens = min(matched_tokens, saved.matched_tokens)
        if matched_tokens <= num_computed_tokens:
            return 0, False
        available = matched_tokens - int(num_computed_tokens)
        if available == matched_tokens and available == int(getattr(request, "num_tokens", 0)):
            available -= 1
        self.state.events.append(
            {
                "event": "match",
                "request_id": str(getattr(request, "request_id", "unknown")),
                "prefix_key": prefix_key,
                "session_id": self.session_id,
                "matched_tokens": matched_tokens,
                "num_computed_tokens": int(num_computed_tokens),
                "available_tokens": max(0, available),
            }
        )
        _emit_event(
            "match",
            request_id=str(getattr(request, "request_id", "unknown")),
            prefix_key=prefix_key,
            session_id=self.session_id,
            matched_tokens=matched_tokens,
            num_computed_tokens=int(num_computed_tokens),
            available_tokens=max(0, available),
        )
        return max(0, available), available > 0

    def update_state_after_alloc(self, request, blocks, num_external_tokens: int) -> None:
        params = _request_params(request)
        if params.get("turbobus.do_save"):
            self._update_save_state_after_alloc(request, blocks, params)
        if num_external_tokens <= 0:
            return
        prefix_key = _request_prefix_key(params)
        saved = _get_saved_prefix_for_connector(
            prefix_key,
            self.session_id,
            job_id=self.job_id,
        )
        if saved is None:
            _emit_event(
                "alloc_miss",
                request_id=str(getattr(request, "request_id", "unknown")),
                prefix_key=prefix_key,
                session_id=self.session_id,
            )
            return
        block_ids = _flatten_block_ids(extract_vllm_block_ids(blocks))
        if not block_ids:
            return
        block_count = _block_count_for_tokens(num_external_tokens, self.vllm_block_size)
        if self.restore_block_limit > 0:
            block_count = min(block_count, self.restore_block_limit)
        block_count = min(block_count, saved.block_count)
        block_ids = block_ids[:block_count]
        request_id = str(getattr(request, "request_id", "unknown"))
        meta = TurboBusRequestMetadata(
            request_id=request_id,
            prefix_key=prefix_key,
            block_ids=tuple(block_ids),
            matched_tokens=int(num_external_tokens),
            block_count=len(block_ids),
        )
        self.state.pending_loads[request_id] = meta
        self.state.events.append(
            {
                "event": "alloc",
                "request_id": request_id,
                "prefix_key": prefix_key,
                "session_id": self.session_id,
                "matched_tokens": int(num_external_tokens),
                "block_count": len(block_ids),
            }
        )
        _emit_event(
            "alloc",
            request_id=request_id,
            prefix_key=prefix_key,
            session_id=self.session_id,
            matched_tokens=int(num_external_tokens),
            block_count=len(block_ids),
        )

    def build_connector_meta(self, scheduler_output) -> TurboBusConnectorMetadata:
        self._collect_save_requests_from_scheduler_output(scheduler_output)
        metadata = TurboBusConnectorMetadata()
        for request_id in sorted(self.state.pending_loads):
            metadata.add_request(self.state.pending_loads[request_id])
        for request_id in sorted(self.state.pending_saves):
            metadata.add_save_request(self.state.pending_saves[request_id])
        if len(metadata) > 0:
            _emit_event(
                "build_connector_meta",
                requests=len(metadata),
                loads=len(metadata.requests),
                saves=len(metadata.save_requests),
            )
        self.state.pending_loads.clear()
        self.state.pending_saves.clear()
        return metadata

    def start_load_kv(self, forward_context, **kwargs) -> None:
        metadata = self._get_connector_metadata()
        if not isinstance(metadata, TurboBusConnectorMetadata) or not metadata.requests:
            return
        start = time.perf_counter()
        if not self.restore_enabled:
            for request in metadata.requests:
                self.state.finished_recving.add(request.request_id)
                self.state.events.append(
                    {
                        "event": "load_ready",
                        "request_id": request.request_id,
                        "block_count": len(request.block_ids),
                        "restore_enabled": False,
                    }
                )
                _emit_event(
                    "load_ready",
                    request_id=request.request_id,
                    block_count=len(request.block_ids),
                    restore_enabled=False,
                )
        else:
            for request in metadata.requests:
                self._restore_request(request)
                self.state.finished_recving.add(request.request_id)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        _emit_event(
            "start_load_done",
            requests=len(metadata.requests),
            restore_enabled=self.restore_enabled,
            elapsed_ms=f"{elapsed_ms:.3f}",
        )

    def wait_for_layer_load(self, layer_name: str) -> None:
        return None

    def save_kv_layer(self, layer_name: str, kv_layer, attn_metadata, **kwargs) -> None:
        metadata = self._get_connector_metadata()
        if not isinstance(metadata, TurboBusConnectorMetadata) or not metadata.save_requests:
            return None
        if not self.state.kv_caches:
            raise RuntimeError("vLLM did not register KV caches for TurboBus")

        kv_items = list(self.state.kv_caches.items())
        layer_index = _layer_index(layer_name, kv_layer, kv_items)
        kv_caches = [item[1] for item in kv_items]
        for request in metadata.save_requests:
            context = self._layer_save_contexts.get(request.request_id)
            if context is None:
                context = self._start_layer_save_context(request, kv_caches)
            if layer_index in context.ready_layers:
                continue
            self._save_request_layer(context, layer_name, layer_index, kv_layer)
        return None

    def wait_for_save(self) -> None:
        metadata = self._get_connector_metadata()
        if not isinstance(metadata, TurboBusConnectorMetadata) or not metadata.save_requests:
            return None
        completed = 0
        for request in metadata.save_requests:
            context = self._layer_save_contexts.pop(request.request_id, None)
            if context is None or not context.ready_layers:
                raise RuntimeError(
                    "vLLM did not call save_kv_layer before wait_for_save "
                    f"for request {request.request_id}"
                )
            if len(context.ready_layers) != len(context.kv_caches):
                raise RuntimeError(
                    f"observed {len(context.ready_layers)} of {len(context.kv_caches)} "
                    f"KV layers for request {request.request_id}"
                )
            try:
                self._finish_layer_save_context(context)
            except Exception:
                self._backing_pool.close_backings(context.cpu_backings)
                raise
            completed += 1
        self.state.events.append(
            {
                "event": "wait_for_save_done",
                "requests": completed,
            }
        )
        _emit_event("wait_for_save_done", requests=completed)
        return None

    def get_finished(self, finished_req_ids: set[str]):
        finished_sending = self.state.finished_sending
        if finished_req_ids:
            finished_sending = finished_sending | (
                self.state.saved_request_ids & set(finished_req_ids)
            )
            self.state.saved_request_ids -= finished_sending
        self.state.finished_sending -= finished_sending
        finished_recving = self.state.finished_recving
        self.state.finished_recving = set()
        return finished_sending or None, finished_recving or None

    def request_finished(self, request, block_ids: list[int]):
        params = _request_params(request)
        if not params.get("turbobus.do_save"):
            return False, None
        request_id = str(getattr(request, "request_id", "unknown"))
        if request_id not in self.state.save_request_ids:
            return False, None
        prefix_key = _request_prefix_key(params)
        matched_tokens = _matched_tokens_for_save(params, self.vllm_block_size)
        return True, {
            "turbobus.prefix_key": prefix_key,
            "turbobus.matched_tokens": matched_tokens,
        }

    def request_finished_all_groups(self, request, block_ids: tuple[list[int], ...]):
        flat_block_ids = [block_id for group in block_ids for block_id in group]
        return self.request_finished(request, flat_block_ids)

    def _get_connector_metadata(self):
        return getattr(self, "_connector_metadata", None)

    def _update_save_state_after_alloc(
        self,
        request,
        blocks,
        params: dict[str, Any],
    ) -> None:
        request_id = str(getattr(request, "request_id", "unknown"))
        if request_id in self.state.save_request_ids:
            return
        self.state.save_params_by_request_id[request_id] = dict(params)
        block_ids = _flatten_block_ids(extract_vllm_block_ids(blocks))
        if not block_ids:
            return
        requested_blocks = _save_block_count(params, self.vllm_block_size)
        if requested_blocks <= 0:
            requested_blocks = len(block_ids)
        if len(block_ids) < requested_blocks:
            self.state.events.append(
                {
                    "event": "save_waiting",
                    "request_id": request_id,
                    "available_blocks": len(block_ids),
                    "requested_blocks": requested_blocks,
                }
            )
            _emit_event(
                "save_waiting",
                request_id=request_id,
                available_blocks=len(block_ids),
                requested_blocks=requested_blocks,
            )
            return
        block_ids = block_ids[:requested_blocks]
        meta = TurboBusRequestMetadata(
            request_id=request_id,
            prefix_key=_request_prefix_key(params),
            block_ids=tuple(block_ids),
            matched_tokens=_matched_tokens_for_save(params, self.vllm_block_size),
            block_count=len(block_ids),
        )
        self.state.pending_saves[request_id] = meta
        self.state.save_request_ids.add(request_id)
        self.state.save_params_by_request_id.pop(request_id, None)
        self.state.events.append(
            {
                "event": "save_alloc",
                "request_id": request_id,
                "prefix_key": meta.prefix_key,
                "matched_tokens": meta.matched_tokens,
                "block_count": meta.block_count,
            }
        )
        _emit_event(
            "save_alloc",
            request_id=request_id,
            prefix_key=meta.prefix_key,
            matched_tokens=meta.matched_tokens,
            block_count=meta.block_count,
        )

    def _collect_save_requests_from_scheduler_output(self, scheduler_output) -> None:
        for request in _iter_scheduled_requests(scheduler_output):
            params = _request_params(request)
            request_id = _scheduled_request_id(request)
            if params.get("turbobus.do_save"):
                self.state.save_params_by_request_id[request_id] = dict(params)
            else:
                params = self.state.save_params_by_request_id.get(request_id, {})
            if not params.get("turbobus.do_save"):
                continue
            if request_id in self.state.save_request_ids:
                continue
            block_ids = _scheduled_request_block_ids(request)
            if not block_ids:
                continue
            requested_blocks = _save_block_count(params, self.vllm_block_size)
            if requested_blocks <= 0:
                requested_blocks = len(block_ids)
            if len(block_ids) < requested_blocks:
                self.state.events.append(
                    {
                        "event": "save_waiting",
                        "request_id": request_id,
                        "available_blocks": len(block_ids),
                        "requested_blocks": requested_blocks,
                    }
                )
                _emit_event(
                    "save_waiting",
                    request_id=request_id,
                    available_blocks=len(block_ids),
                    requested_blocks=requested_blocks,
                )
                continue
            block_ids = block_ids[:requested_blocks]
            meta = TurboBusRequestMetadata(
                request_id=request_id,
                prefix_key=_request_prefix_key(params),
                block_ids=tuple(block_ids),
                matched_tokens=_matched_tokens_for_save(params, self.vllm_block_size),
                block_count=len(block_ids),
            )
            self.state.pending_saves[request_id] = meta
            self.state.save_request_ids.add(request_id)
            self.state.save_params_by_request_id.pop(request_id, None)
            self.state.events.append(
                {
                    "event": "save_schedule",
                    "request_id": request_id,
                    "prefix_key": meta.prefix_key,
                    "matched_tokens": meta.matched_tokens,
                    "block_count": meta.block_count,
                }
            )
            _emit_event(
                "save_schedule",
                request_id=request_id,
                prefix_key=meta.prefix_key,
                matched_tokens=meta.matched_tokens,
                block_count=meta.block_count,
            )

    def _integration_for_saved_prefix(
        self,
        saved: TurboBusSavedPrefix,
        request: TurboBusRequestMetadata,
    ) -> tuple[VllmTurboBusIntegration, list[Any], float]:
        if not self.state.kv_caches:
            raise RuntimeError("vLLM did not register KV caches for TurboBus")

        kv_caches = list(self.state.kv_caches.values())
        if len(saved.cpu_backings) != len(kv_caches):
            raise RuntimeError(
                f"saved prefix {saved.key!r} has {len(saved.cpu_backings)} backing tensors, "
                f"but vLLM registered {len(kv_caches)} KV cache tensors"
            )
        integration, _, client_init_ms = self._make_integration(
            saved.cpu_backings,
            workload_kind=WorkloadKind.KV_CACHE,
            metadata=self._adapter_metadata(
                {
                    "prefix_key": saved.key,
                    "request_id": request.request_id,
                    "vllm_operation": "restore",
                    "vllm_lifecycle": "start_load_kv",
                    "matched_tokens": request.matched_tokens,
                    "block_count": request.block_count,
                    "block_ids": list(request.block_ids),
                    "source_request_id": saved.source_request_id,
                }
            ),
            intent_prefix=f"vllm-kv-restore-{saved.key}",
        )
        return integration, kv_caches, client_init_ms

    def _restore_request(self, request: TurboBusRequestMetadata) -> None:
        total_start = time.perf_counter()
        saved = _get_saved_prefix_for_connector(
            request.prefix_key,
            self.session_id,
            job_id=self.job_id,
        )
        if saved is None:
            raise RuntimeError(f"saved prefix {request.prefix_key!r} is not registered")
        prepare_start = time.perf_counter()
        integration, kv_caches, client_init_ms = self._integration_for_saved_prefix(
            saved,
            request,
        )
        register_start = time.perf_counter()
        if integration.record_request_blocks(request.request_id, request.block_ids) is None:
            raise RuntimeError(f"restore request {request.request_id} has no block ids")
        range_names = integration.register_request(
            request.request_id,
            cpu_slot_start=request.cpu_slot_start,
        )
        request_binding = integration.lifecycle_request_binding(
            request.request_id,
            cpu_slot_start=request.cpu_slot_start,
        )
        register_ms = (time.perf_counter() - register_start) * 1000.0
        prepare_ms = (time.perf_counter() - prepare_start) * 1000.0
        try:
            transfer_start = time.perf_counter()
            handles = integration.submit_restore_request_prefix(
                request.request_id,
                cpu_slot_start=request.cpu_slot_start,
            )
            _wait_transfer_handles(handles)
            transfer_ms = (time.perf_counter() - transfer_start) * 1000.0
            total_ms = (time.perf_counter() - total_start) * 1000.0
            stats = integration.transfer_stats_for_request(
                request.request_id,
                cpu_slot_start=request.cpu_slot_start,
            ).as_dict()
            evidence_id = _vllm_kv_lifecycle_evidence_id(
                "restore",
                self.session_id,
                request,
            )
            receipt_trace = _receipt_trace_from_handles(
                handles,
                self.runtime_session,
                evidence_id=evidence_id,
                operation="restore",
            )
        finally:
            integration.forget_request(request.request_id)
        lifecycle_evidence = _vllm_kv_lifecycle_evidence(
            evidence_id=evidence_id,
            operation="restore",
            request=request,
            job_id=self.job_id,
            session_id=self.session_id,
            source_request_id=saved.source_request_id,
            block_count=len(request.block_ids),
            matched_tokens=request.matched_tokens,
            layer_count=len(kv_caches),
            range_count=len(range_names),
            receipt_trace=receipt_trace,
            request_binding=request_binding,
        )
        saved.last_restore_lifecycle_evidence = lifecycle_evidence
        saved.daemon_recovery_evidence = {
            "operation": "restore",
            "request_id": request.request_id,
            "runtime_entrypoint": dict(lifecycle_evidence["runtime_entrypoint"]),
            "adapter_evidence_record": _adapter_evidence_record_for_event(
                lifecycle_evidence
            ),
            "daemon_recovery": list(receipt_trace.get("daemon_recovery", [])),
            "daemon_recovery_count": int(
                receipt_trace.get("daemon_recovery_count", 0) or 0
            ),
            "daemon_recovery_sources": str(
                receipt_trace.get("daemon_recovery_sources", "")
            ),
            "route_policy_visible_to_adapter": False,
        }
        self.state.events.append(
            {
                "event": "restore",
                "request_id": request.request_id,
                "prefix_key": request.prefix_key,
                "session_id": self.session_id,
                "source_request_id": saved.source_request_id,
                "block_count": len(request.block_ids),
                "matched_tokens": request.matched_tokens,
                "elapsed_ms": transfer_ms,
                "client_init_ms": client_init_ms,
                "register_ms": register_ms,
                "prepare_ms": prepare_ms,
                "transfer_ms": transfer_ms,
                "total_ms": total_ms,
                "layers": len(kv_caches),
                "ranges": len(range_names),
                "lifecycle_evidence": lifecycle_evidence,
                "adapter_evidence_record": _adapter_evidence_record_for_event(
                    lifecycle_evidence
                ),
                "route_policy_visible_to_adapter": False,
                **stats,
                **receipt_trace,
            }
        )
        _emit_event(
            "restore",
            request_id=request.request_id,
            prefix_key=request.prefix_key,
            session_id=self.session_id,
            source_request_id=saved.source_request_id,
            block_count=len(request.block_ids),
            matched_tokens=request.matched_tokens,
            elapsed_ms=f"{transfer_ms:.3f}",
            client_init_ms=f"{client_init_ms:.3f}",
            register_ms=f"{register_ms:.3f}",
            prepare_ms=f"{prepare_ms:.3f}",
            transfer_ms=f"{transfer_ms:.3f}",
            total_ms=f"{total_ms:.3f}",
            layers=len(kv_caches),
            ranges=len(range_names),
            lifecycle_evidence_id=lifecycle_evidence["evidence_id"],
            adapter_evidence_record=_adapter_evidence_record_for_event(
                lifecycle_evidence
            ),
            **stats,
            **_event_receipt_trace_fields(receipt_trace),
        )

    def _start_layer_save_context(
        self,
        request: TurboBusRequestMetadata,
        kv_caches: list[Any],
    ) -> _LayerSaveContext:
        total_start = time.perf_counter()
        alloc_start = time.perf_counter()
        cpu_backings, reused_backing = self._backing_pool.acquire(
            request.block_count,
            kv_caches,
        )
        cpu_alloc_ms = (time.perf_counter() - alloc_start) * 1000.0
        try:
            integration, _, client_init_ms = self._make_integration(
                cpu_backings,
                workload_kind=WorkloadKind.KV_CACHE,
                metadata=self._adapter_metadata(
                    {
                        "prefix_key": request.prefix_key,
                        "request_id": request.request_id,
                        "vllm_operation": "save",
                        "vllm_lifecycle": "save_kv_request",
                        "matched_tokens": request.matched_tokens,
                        "block_count": request.block_count,
                        "block_ids": list(request.block_ids),
                    }
                ),
                intent_prefix=f"vllm-kv-save-{request.prefix_key}",
            )
            register_start = time.perf_counter()
            if integration.record_request_blocks(request.request_id, request.block_ids) is None:
                raise RuntimeError(f"save request {request.request_id} has no block ids")
            integration.register_request(
                request.request_id,
                cpu_slot_start=request.cpu_slot_start,
            )
            request_binding = integration.lifecycle_request_binding(
                request.request_id,
                cpu_slot_start=request.cpu_slot_start,
            )
            refs_ms = (time.perf_counter() - register_start) * 1000.0
        except Exception:
            self._backing_pool.release(
                request.block_count,
                kv_caches,
                cpu_backings,
            )
            raise
        context = _LayerSaveContext(
            request=request,
            cpu_backings=cpu_backings,
            kv_caches=list(kv_caches),
            integration=integration,
            reused_backing=reused_backing,
            total_start=total_start,
            client_init_ms=client_init_ms,
            cpu_alloc_ms=cpu_alloc_ms,
            refs_ms=refs_ms,
            request_binding=request_binding,
        )
        self._layer_save_contexts[request.request_id] = context
        return context

    def _save_request_layer(
        self,
        context: _LayerSaveContext,
        layer_name: str,
        layer_index: int,
        kv_layer,
    ) -> None:
        request = context.request
        layer_ready_start = time.perf_counter()
        context.ready_layers.add(layer_index)
        ready_ms = (time.perf_counter() - layer_ready_start) * 1000.0
        context.group_ms += ready_ms
        self.state.events.append(
            {
                "event": "save_layer_ready",
                "request_id": request.request_id,
                "prefix_key": request.prefix_key,
                "session_id": self.session_id,
                "layer_name": str(layer_name),
                "layer_index": layer_index,
                "ready_layers": len(context.ready_layers),
                "elapsed_ms": ready_ms,
            }
        )
        _emit_event(
            "save_layer_ready",
            request_id=request.request_id,
            prefix_key=request.prefix_key,
            session_id=self.session_id,
            layer_name=str(layer_name),
            layer_index=layer_index,
            ready_layers=len(context.ready_layers),
            elapsed_ms=f"{ready_ms:.3f}",
        )

    def _finish_layer_save_context(self, context: _LayerSaveContext) -> None:
        request = context.request
        try:
            transfer_start = time.perf_counter()
            range_names = context.integration.block_names_for_request(
                request.request_id,
                cpu_slot_start=request.cpu_slot_start,
            )
            handles = context.integration.submit_save_request_prefix(
                request.request_id,
                cpu_slot_start=request.cpu_slot_start,
            )
            _wait_transfer_handles(handles)
            context.transfer_ms = (time.perf_counter() - transfer_start) * 1000.0
            stats = context.integration.transfer_stats_for_request(
                request.request_id,
                cpu_slot_start=request.cpu_slot_start,
            )
            evidence_id = _vllm_kv_lifecycle_evidence_id(
                "save",
                self.session_id,
                request,
            )
            receipt_trace = _receipt_trace_from_handles(
                handles,
                self.runtime_session,
                evidence_id=evidence_id,
                operation="save",
            )
        finally:
            context.integration.forget_request(request.request_id)
        context.bytes = stats.bytes
        context.direct_chunks = stats.direct_chunks
        context.relay_chunks = stats.relay_chunks
        context.direct_bytes = int(receipt_trace["direct_bytes"])
        context.relay_bytes = int(receipt_trace["relay_bytes"])
        context.receipt_ids.extend(_csv_values(receipt_trace["receipt_ids"]))
        context.decision_ids.extend(_csv_values(receipt_trace["decision_ids"]))
        context.topology_snapshot_ids.extend(_csv_values(receipt_trace["topology_snapshot_ids"]))
        context.ticket_ids.extend(_csv_values(receipt_trace["ticket_ids"]))
        context.fallback_reasons.extend(_csv_values(receipt_trace["fallback_reason"]))
        context.ranges = len(range_names)
        register_start = time.perf_counter()
        lifecycle_evidence = _vllm_kv_lifecycle_evidence(
            evidence_id=evidence_id,
            operation="save",
            request=request,
            job_id=self.job_id,
            session_id=self.session_id,
            source_request_id=request.request_id,
            block_count=request.block_count,
            matched_tokens=request.matched_tokens,
            layer_count=len(context.kv_caches),
            range_count=context.ranges,
            receipt_trace=receipt_trace,
            request_binding=context.request_binding,
        )
        prefix = TurboBusSavedPrefix(
            key=request.prefix_key,
            cpu_backings=context.cpu_backings,
            block_count=request.block_count,
            matched_tokens=request.matched_tokens,
            job_id=self.job_id,
            session_id=self.session_id,
            source_request_id=request.request_id,
            elapsed_ms=context.transfer_ms,
            client_init_ms=context.client_init_ms,
            prepare_ms=(
                context.cpu_alloc_ms
                + context.client_init_ms
                + context.group_ms
                + context.adapter_ms
                + context.refs_ms
            ),
            cpu_alloc_ms=context.cpu_alloc_ms,
            reused_backing=context.reused_backing,
            group_ms=context.group_ms,
            adapter_ms=context.adapter_ms,
            refs_ms=context.refs_ms,
            transfer_ms=context.transfer_ms,
            bytes=context.bytes,
            direct_chunks=context.direct_chunks,
            relay_chunks=context.relay_chunks,
            direct_bytes=context.direct_bytes,
            relay_bytes=context.relay_bytes,
            receipt_ids=_join_unique(context.receipt_ids),
            decision_ids=_join_unique(context.decision_ids),
            topology_snapshot_ids=_join_unique(context.topology_snapshot_ids),
            ticket_ids=_join_unique(context.ticket_ids),
            fallback_reason=_join_unique(context.fallback_reasons),
            save_layer_count=len(context.ready_layers),
            save_layer_ranges=context.ranges,
            save_lifecycle_evidence=lifecycle_evidence,
            daemon_recovery_evidence={
                "operation": "save",
                "request_id": request.request_id,
                "runtime_entrypoint": dict(lifecycle_evidence["runtime_entrypoint"]),
                "adapter_evidence_record": _adapter_evidence_record_for_event(
                    lifecycle_evidence
                ),
                "daemon_recovery": list(receipt_trace.get("daemon_recovery", [])),
                "daemon_recovery_count": int(
                    receipt_trace.get("daemon_recovery_count", 0) or 0
                ),
                "daemon_recovery_sources": str(
                    receipt_trace.get("daemon_recovery_sources", "")
                ),
                "route_policy_visible_to_adapter": False,
            },
        )
        mutation = self._store_prefix(prefix)
        _store_saved_prefix_for_connector(prefix)
        for removal in mutation.removals:
            removed = removal.prefix
            if removed.key != prefix.key:
                _remove_saved_prefix_for_connector(
                    removed.key,
                    removed.session_id,
                    job_id=removed.job_id,
                )
        _emit_event(
            "register_saved_prefix",
            prefix_key=request.prefix_key,
            session_id=self.session_id,
            block_count=request.block_count,
            matched_tokens=request.matched_tokens,
            source_request_id=request.request_id,
            layers=len(context.cpu_backings),
            store_mutation_id=mutation.evidence["mutation_id"],
            adapter_evidence_record=_adapter_evidence_record_for_event(
                mutation.evidence
            ),
            removed_prefixes=len(mutation.removals),
        )
        self.state.saved_request_ids.add(request.request_id)
        register_ms = (time.perf_counter() - register_start) * 1000.0
        total_ms = (time.perf_counter() - context.total_start) * 1000.0
        saved = _get_saved_prefix_for_connector(
            request.prefix_key,
            self.session_id,
            job_id=self.job_id,
        )
        if saved is not None:
            saved.register_ms = register_ms
            saved.total_ms = total_ms
        stats = TransferStats(
            bytes=context.bytes,
            direct_chunks=context.direct_chunks,
            relay_chunks=context.relay_chunks,
        ).as_dict()
        receipt_trace = {
            "direct_bytes": context.direct_bytes,
            "relay_bytes": context.relay_bytes,
            "receipt_ids": _join_unique(context.receipt_ids),
            "decision_ids": _join_unique(context.decision_ids),
            "topology_snapshot_ids": _join_unique(context.topology_snapshot_ids),
            "ticket_ids": _join_unique(context.ticket_ids),
            "fallback_reason": _join_unique(context.fallback_reasons),
            "receipt_count": lifecycle_evidence["receipt_count"],
            "receipt_states": lifecycle_evidence["receipt_states"],
            "completion_sources": lifecycle_evidence["completion_sources"],
            "transfer_ids": lifecycle_evidence["transfer_ids"],
        }
        self.state.events.append(
            {
                "event": "save",
                "request_id": request.request_id,
                "prefix_key": request.prefix_key,
                "session_id": self.session_id,
                "block_count": len(request.block_ids),
                "matched_tokens": request.matched_tokens,
                "elapsed_ms": context.transfer_ms,
                "client_init_ms": context.client_init_ms,
                "prepare_ms": saved.prepare_ms if saved is not None else 0.0,
                "cpu_alloc_ms": context.cpu_alloc_ms,
                "reused_backing": context.reused_backing,
                "group_ms": context.group_ms,
                "adapter_ms": context.adapter_ms,
                "refs_ms": context.refs_ms,
                "transfer_ms": context.transfer_ms,
                "register_ms": register_ms,
                "total_ms": total_ms,
                "layers": len(context.kv_caches),
                "ranges": context.ranges,
                "lifecycle_evidence": lifecycle_evidence,
                "store_lifecycle_evidence": mutation.evidence,
                "adapter_evidence_record": _adapter_evidence_record_for_event(
                    lifecycle_evidence
                ),
                "route_policy_visible_to_adapter": False,
                **stats,
                **receipt_trace,
            }
        )
        _emit_event(
            "save",
            request_id=request.request_id,
            prefix_key=request.prefix_key,
            session_id=self.session_id,
            block_count=len(request.block_ids),
            matched_tokens=request.matched_tokens,
            elapsed_ms=f"{context.transfer_ms:.3f}",
            client_init_ms=f"{context.client_init_ms:.3f}",
            prepare_ms=f"{(saved.prepare_ms if saved is not None else 0.0):.3f}",
            cpu_alloc_ms=f"{context.cpu_alloc_ms:.3f}",
            reused_backing=context.reused_backing,
            group_ms=f"{context.group_ms:.3f}",
            adapter_ms=f"{context.adapter_ms:.3f}",
            refs_ms=f"{context.refs_ms:.3f}",
            transfer_ms=f"{context.transfer_ms:.3f}",
            register_ms=f"{register_ms:.3f}",
            total_ms=f"{total_ms:.3f}",
            layers=len(context.kv_caches),
            ranges=context.ranges,
            lifecycle_evidence_id=lifecycle_evidence["evidence_id"],
            store_mutation_id=mutation.evidence["mutation_id"],
            adapter_evidence_record=_adapter_evidence_record_for_event(
                lifecycle_evidence
            ),
            **stats,
            **_event_receipt_trace_fields(receipt_trace),
        )

    def _make_integration(
        self,
        cpu_backings: list[Any],
        *,
        workload_kind: WorkloadKind | str,
        metadata: dict[str, Any],
        intent_prefix: str,
    ) -> tuple[VllmTurboBusIntegration, list[Any], float]:
        if not self.state.kv_caches:
            raise RuntimeError("vLLM did not register KV caches for TurboBus")
        kv_caches = list(self.state.kv_caches.values())
        client_init_start = time.perf_counter()
        integration = self.runtime_session.make_vllm_turbobus_integration(
            cpu_backings,
            workload_kind=workload_kind,
            metadata=metadata,
            intent_prefix=intent_prefix,
            wait_timeout_seconds=self.config.wait_timeout_seconds,
            cpu_buffer_id=self.config.cpu_buffer_id,
            gpu_buffer_id=self.config.gpu_buffer_id,
        )
        integration.bind_kv_caches(kv_caches)
        client_init_ms = (time.perf_counter() - client_init_start) * 1000.0
        return integration, kv_caches, client_init_ms

    def _allocate_cpu_backings(self, block_count: int, kv_caches: list[Any]) -> list[Any]:
        return self._backing_pool._allocate_for_pool(block_count, kv_caches)

    def _store_prefix(self, prefix: TurboBusSavedPrefix):
        if prefix.job_id == "default":
            prefix.job_id = self.job_id
        elif prefix.job_id != self.job_id:
            raise ValueError("saved prefix job_id must match connector job_id")
        mutation = self._prefix_store.put_with_lifecycle(
            prefix,
            reason="vllm_kv_save",
        )
        kv_caches = list(self.state.kv_caches.values())
        for removal in mutation.removals:
            removed = removal.prefix
            backing_evidence = self._backing_pool.release_prefix(removed, kv_caches)
            removal.cleanup_evidence["backing_lifecycle"] = backing_evidence
            self.state.events.append(
                {
                    "event": "evict_prefix",
                    "prefix_key": removed.key,
                    "session_id": removed.session_id,
                    "block_count": removed.block_count,
                    "source_request_id": removed.source_request_id,
                    "reason": removal.reason,
                    "cleanup_lifecycle_evidence": removal.cleanup_evidence,
                    "backing_lifecycle": backing_evidence,
                    "adapter_evidence_record": _adapter_evidence_record_for_event(
                        removal.cleanup_evidence
                    ),
                    "route_policy_visible_to_adapter": False,
                }
            )
            _emit_event(
                "evict_prefix",
                prefix_key=removed.key,
                session_id=removed.session_id,
                block_count=removed.block_count,
                source_request_id=removed.source_request_id,
                reason=removal.reason,
                cleanup_mutation_id=removal.cleanup_evidence["mutation_id"],
                adapter_evidence_record=_adapter_evidence_record_for_event(
                    removal.cleanup_evidence
                ),
                backing_lifecycle_action=backing_evidence["action"],
                route_policy_visible_to_adapter=False,
            )
        return mutation

    def _close_saved_prefixes(self) -> dict[str, object]:
        drained = self._prefix_store.drain_with_lifecycle(
            session_id=self.session_id,
            job_id=self.job_id,
            reason="connector_close",
        )
        cleanup_mutation_ids: list[str] = []
        adapter_evidence_records: list[dict[str, Any]] = []
        for removal in drained.removals:
            prefix = removal.prefix
            backing_evidence = self._backing_pool.close_prefix(prefix)
            removal.cleanup_evidence["backing_lifecycle"] = backing_evidence
            cleanup_mutation_ids.append(str(removal.cleanup_evidence["mutation_id"]))
            adapter_evidence_records.append(
                _adapter_evidence_record_for_event(removal.cleanup_evidence)
            )
            self.state.events.append(
                {
                    "event": "close_prefix",
                    "prefix_key": prefix.key,
                    "session_id": prefix.session_id,
                    "block_count": prefix.block_count,
                    "source_request_id": prefix.source_request_id,
                    "reason": removal.reason,
                    "cleanup_lifecycle_evidence": removal.cleanup_evidence,
                    "backing_lifecycle": backing_evidence,
                    "adapter_evidence_record": _adapter_evidence_record_for_event(
                        removal.cleanup_evidence
                    ),
                    "route_policy_visible_to_adapter": False,
                }
            )
            _emit_event(
                "close_prefix",
                prefix_key=prefix.key,
                session_id=prefix.session_id,
                block_count=prefix.block_count,
                source_request_id=prefix.source_request_id,
                reason=removal.reason,
                cleanup_mutation_id=removal.cleanup_evidence["mutation_id"],
                adapter_evidence_record=_adapter_evidence_record_for_event(
                    removal.cleanup_evidence
                ),
                backing_lifecycle_action=backing_evidence["action"],
                route_policy_visible_to_adapter=False,
            )
        return {
            "count": len(drained.prefixes),
            "cleanup_mutation_ids": cleanup_mutation_ids,
            "adapter_evidence_records": adapter_evidence_records,
            "drain_mutation_id": drained.evidence["mutation_id"],
        }

    def _close_pending_save_contexts(self) -> int:
        contexts = list(self._layer_save_contexts.values())
        self._layer_save_contexts.clear()
        for context in contexts:
            try:
                context.integration.forget_request(context.request.request_id)
            except Exception:
                pass
            self._backing_pool.close_backings(context.cpu_backings)
        return len(contexts)

    def _close_pending_metadata(self) -> dict[str, int]:
        closed = {
            "pending_loads": len(self.state.pending_loads),
            "pending_save_metadata": len(self.state.pending_saves),
            "pending_save_params": len(self.state.save_params_by_request_id),
            "save_request_ids": len(self.state.save_request_ids),
            "saved_request_ids": len(self.state.saved_request_ids),
            "finished_sending": len(self.state.finished_sending),
            "finished_recving": len(self.state.finished_recving),
        }
        self.state.pending_loads.clear()
        self.state.pending_saves.clear()
        self.state.save_params_by_request_id.clear()
        self.state.save_request_ids.clear()
        self.state.saved_request_ids.clear()
        self.state.finished_sending.clear()
        self.state.finished_recving.clear()
        return closed

    def _adapter_metadata(self, metadata: dict[str, Any]) -> dict[str, Any]:
        return {
            **metadata,
            "connector": "vllm_kv",
            "connector_session_id": self.connector_session_id,
            "chunk_bytes": self.config.chunk_bytes,
        }


def _request_params(request) -> dict[str, Any]:
    params = getattr(request, "kv_transfer_params", None)
    if isinstance(params, dict):
        return _validate_vllm_kv_request_params(params)
    sampling_params = getattr(request, "sampling_params", None)
    extra_args = getattr(sampling_params, "extra_args", None)
    if isinstance(extra_args, dict):
        params = extra_args.get("kv_transfer_params")
        if isinstance(params, dict):
            return _validate_vllm_kv_request_params(params)
    return {}


def _validate_vllm_kv_request_params(params: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(params)
    invalid_keys = forbidden_physical_policy_keys(normalized)
    if invalid_keys:
        raise ValueError(
            "vLLM TurboBus kv_transfer_params must not choose physical paths: "
            + ", ".join(str(key) for key in invalid_keys)
        )
    return normalized

def _wait_transfer_handles(handles) -> list[object]:
    resolved = list(handles)
    seen = set()
    for handle in resolved:
        handle_id = id(handle)
        if handle_id in seen:
            continue
        seen.add(handle_id)
        waiter = getattr(handle, "wait", None)
        if not callable(waiter):
            raise TypeError("vLLM TurboBus transfer handle must expose wait()")
        waiter()
    return resolved


def _receipt_trace_from_handles(
    handles,
    runtime_session: TurboBusRuntimeSession,
    *,
    evidence_id: str,
    operation: str,
) -> dict[str, Any]:
    receipts: list[TransferReceipt] = []
    seen = set()
    handles = list(handles)
    if not handles:
        raise RuntimeError("vLLM TurboBus transfer produced no handles")
    for index, handle in enumerate(handles):
        receipt = getattr(handle, "receipt", None)
        if not isinstance(receipt, TransferReceipt):
            raise TypeError(
                "vLLM TurboBus transfer handle "
                f"{index} did not expose a TransferReceipt"
            )
        if receipt.receipt_id in seen:
            continue
        seen.add(receipt.receipt_id)
        receipts.append(receipt)
    if not receipts:
        raise RuntimeError("vLLM TurboBus transfer produced no receipts")
    return _receipt_trace_from_receipts(
        receipts,
        runtime_session,
        evidence_id=evidence_id,
        operation=operation,
    )


def _receipt_trace_from_receipts(
    receipts: list[TransferReceipt],
    runtime_session: TurboBusRuntimeSession,
    *,
    evidence_id: str,
    operation: str,
) -> dict[str, Any]:
    return runtime_session_receipt_trace_from_receipts(
        receipts,
        runtime_session,
        evidence_id=evidence_id,
        operation=operation,
    )


def _vllm_kv_lifecycle_evidence_id(
    operation: str,
    session_id: str,
    request: TurboBusRequestMetadata,
) -> str:
    return (
        f"vllm-kv-{operation}-{session_id}-{request.prefix_key}-"
        f"{request.request_id}"
    )


def _event_receipt_trace_fields(receipt_trace: Mapping[str, Any]) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 步骤1：构造 adapter event receipt 摘要
    #  * ========================================================================
    #  * 数据源：RuntimeSession receipt trace
    #  * 操作：
    #  *   1) 只保留控制台事件需要的标量摘要
    #  *   2) 显式拒绝在 event 输出中暴露 route policy 或大块证据对象
    #  */
    logger.info("开始构造 adapter event receipt 摘要...")

    # // 1.1 保留标量 receipt trace 字段
    allowed_keys = (
        "direct_bytes",
        "relay_bytes",
        "receipt_ids",
        "decision_ids",
        "topology_snapshot_ids",
        "ticket_ids",
        "fallback_reason",
        "receipt_count",
        "receipt_states",
        "completion_sources",
        "transfer_ids",
    )
    event_fields = {key: receipt_trace.get(key) for key in allowed_keys}

    # // 1.2 写入 route policy 拒绝字段
    event_fields["route_policy_visible_to_adapter"] = False
    logger.info("adapter event receipt 摘要构造完成, fields: %s", len(event_fields))
    return event_fields


def _adapter_evidence_record_for_event(
    lifecycle_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 步骤2：提取 adapter event 证据记录
    #  * ========================================================================
    #  * 数据源：adapter lifecycle evidence
    #  * 操作：
    #  *   1) 从 lifecycle runtime_entrypoint 提取 adapter evidence record
    #  *   2) 拒绝缺失 RuntimeSession 证据链的 adapter event
    #  */
    logger.info("开始提取 adapter event 证据记录...")

    # // 2.1 读取 RuntimeSession entrypoint 合约
    runtime_entrypoint = lifecycle_evidence.get("runtime_entrypoint")
    if not isinstance(runtime_entrypoint, Mapping):
        raise ValueError("adapter event missing RuntimeSession entrypoint")

    # // 2.2 提取 adapter evidence record
    adapter_record = runtime_entrypoint.get("adapter_evidence_record")
    if not isinstance(adapter_record, Mapping):
        raise ValueError("adapter event missing adapter evidence record")

    # // 2.3 返回事件摘要
    result = dict(adapter_record)
    logger.info("adapter event 证据记录提取完成, evidence_id: %s", result.get("evidence_id"))
    return result


def _vllm_kv_lifecycle_evidence(
    *,
    evidence_id: str,
    operation: str,
    request: TurboBusRequestMetadata,
    job_id: str,
    session_id: str,
    source_request_id: str,
    block_count: int,
    matched_tokens: int,
    layer_count: int,
    range_count: int,
    receipt_trace: Mapping[str, Any],
    request_binding: Mapping[str, Any],
) -> dict[str, Any]:
    receipt_contracts = receipt_trace.get("receipt_contracts")
    runtime_buffer_bindings = receipt_trace.get("runtime_buffer_bindings")
    daemon_recovery = receipt_trace.get("daemon_recovery")
    runtime_entrypoint = _runtime_entrypoint_for_vllm_kv_lifecycle(
        receipt_trace,
        evidence_id=str(evidence_id),
        receipt_contracts=receipt_contracts,
    )
    request_binding_evidence = _request_binding_for_vllm_kv_lifecycle(
        request_binding,
        runtime_entrypoint=runtime_entrypoint,
        evidence_id=str(evidence_id),
    )
    return {
        "evidence_id": str(evidence_id),
        "operation": str(operation),
        "request_id": request.request_id,
        "source_request_id": str(source_request_id),
        "prefix_key": request.prefix_key,
        "job_id": str(job_id),
        "session_id": str(session_id),
        "block_count": int(block_count),
        "matched_tokens": int(matched_tokens),
        "layer_count": int(layer_count),
        "range_count": int(range_count),
        "buffer_registration_source": "TurboBusRuntimeSession",
        "intent_source": "TransferIntent",
        "receipt_source": "TransferReceipt",
        "adapter_submit_source": "TurboBusRuntimeSession",
        "adapter_handle_source": "ReceiptTransferHandle",
        "policy_source": "daemon_scheduler",
        "route_policy_visible_to_adapter": False,
        "physical_route_source": "daemon_scheduler",
        "intent_ids": str(receipt_trace.get("intent_ids", "")),
        "receipt_count": int(receipt_trace.get("receipt_count", 0) or 0),
        "receipt_ids": str(receipt_trace.get("receipt_ids", "")),
        "receipt_states": str(receipt_trace.get("receipt_states", "")),
        "decision_ids": str(receipt_trace.get("decision_ids", "")),
        "topology_snapshot_ids": str(receipt_trace.get("topology_snapshot_ids", "")),
        "ticket_ids": str(receipt_trace.get("ticket_ids", "")),
        "transfer_ids": str(receipt_trace.get("transfer_ids", "")),
        "completion_sources": str(receipt_trace.get("completion_sources", "")),
        "direct_bytes": int(receipt_trace.get("direct_bytes", 0) or 0),
        "relay_bytes": int(receipt_trace.get("relay_bytes", 0) or 0),
        "fallback_reason": str(receipt_trace.get("fallback_reason", "")),
        "request_binding": request_binding_evidence,
        "block_ids": list(request.block_ids),
        "runtime_buffer_bindings": (
            [dict(item) for item in runtime_buffer_bindings if isinstance(item, Mapping)]
            if isinstance(runtime_buffer_bindings, list)
            else []
        ),
        "receipt_contracts": (
            [dict(item) for item in receipt_contracts if isinstance(item, Mapping)]
            if isinstance(receipt_contracts, list)
            else []
        ),
        "daemon_recovery": (
            [dict(item) for item in daemon_recovery if isinstance(item, Mapping)]
            if isinstance(daemon_recovery, list)
            else []
        ),
        "runtime_entrypoint": runtime_entrypoint,
        "daemon_recovery_count": int(
            receipt_trace.get("daemon_recovery_count", 0) or 0
        ),
        "daemon_recovery_sources": str(
            receipt_trace.get("daemon_recovery_sources", "")
        ),
    }


def _runtime_close_entrypoint_for_connector_close(
    close_payload: Mapping[str, Any],
) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 步骤1：提取 connector close 的 RuntimeSession entrypoint
    #  * ========================================================================
    #  * 数据源：TurboBusRuntimeSession.close response payload
    #  * 操作：
    #  *   1) 拒绝缺失 RuntimeSession close snapshot
    #  *   2) 拒绝 close 证据暴露 route policy
    #  */
    logger.info("开始提取 connector close RuntimeSession entrypoint...")

    # // 1.1 读取 close payload 中的 RuntimeSession entrypoint
    if not isinstance(close_payload, Mapping):
        raise ValueError("connector close requires RuntimeSession close payload")
    runtime_entrypoint = close_payload.get("runtime_entrypoint")
    if not isinstance(runtime_entrypoint, Mapping):
        raise ValueError("connector close missing RuntimeSession entrypoint")
    contract = dict(runtime_entrypoint)

    # // 1.2 校验生产入口和 route policy 边界
    if str(contract.get("entrypoint")) != "TurboBusRuntimeSession":
        raise ValueError("connector close RuntimeSession entrypoint mismatch")
    if str(contract.get("plan_source")) != "daemon_scheduler":
        raise ValueError("connector close RuntimeSession plan_source mismatch")
    if bool(contract.get("route_policy_visible_to_adapter", True)):
        raise ValueError("connector close exposes route policy to adapter")
    if bool(contract.get("route_policy_visible_to_application", True)):
        raise ValueError("connector close exposes route policy to application")

    # // 1.3 校验 close record 已写入 entrypoint
    close_record = contract.get("close")
    if not isinstance(close_record, Mapping):
        raise ValueError("connector close missing RuntimeSession close record")
    if str(close_record.get("entrypoint")) != "TurboBusRuntimeSession.close":
        raise ValueError("connector close record mismatch")
    if bool(close_record.get("route_policy_visible_to_adapter", True)):
        raise ValueError("connector close record exposes route policy")

    # // 1.4 返回隔离副本供 cleanup evidence 消费
    contract["close"] = dict(close_record)
    logger.info("connector close RuntimeSession entrypoint 提取完成")
    return contract


def _request_binding_for_vllm_kv_lifecycle(
    request_binding: Mapping[str, Any],
    *,
    runtime_entrypoint: Mapping[str, Any],
    evidence_id: str,
) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 步骤1：绑定 vLLM KV request evidence
    #  * ========================================================================
    #  * 数据源：integration request binding 与 RuntimeSession entrypoint
    #  * 操作：
    #  *   1) 复制 integration request binding
    #  *   2) 绑定 RuntimeSession adapter evidence record 和 route policy 拒绝字段
    #  */
    logger.info("开始绑定 vLLM KV request evidence...")

    # // 1.1 复制 integration request binding
    binding = dict(request_binding)

    # // 1.2 校验 RuntimeSession adapter evidence record
    adapter_record = runtime_entrypoint.get("adapter_evidence_record")
    if not isinstance(adapter_record, Mapping):
        raise ValueError("vLLM KV request binding missing adapter evidence record")
    if str(adapter_record.get("evidence_id")) != str(evidence_id):
        raise ValueError("vLLM KV request binding adapter evidence_id mismatch")

    # // 1.3 写入 RuntimeSession 边界字段
    binding["runtime_entrypoint"] = dict(runtime_entrypoint)
    binding["adapter_evidence_record"] = dict(adapter_record)
    binding["route_policy_visible_to_adapter"] = False
    logger.info("vLLM KV request evidence 绑定完成, evidence_id: %s", evidence_id)
    return binding


def _runtime_entrypoint_for_vllm_kv_lifecycle(
    receipt_trace: Mapping[str, Any],
    *,
    evidence_id: str,
    receipt_contracts: object,
) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 步骤1：提取 vLLM KV RuntimeSession 合约
    #  * ========================================================================
    #  * 数据源：receipt_trace.runtime_entrypoint
    #  * 操作：
    #  *   1) 拒绝缺失 RuntimeSession entrypoint 合约的 vLLM KV evidence
    #  *   2) 拒绝缺失 adapter evidence 记录明细或暴露 route policy 的合约
    #  */
    logger.info("开始提取 vLLM KV RuntimeSession 合约...")

    # // 1.1 读取 RuntimeSession entrypoint 合约
    runtime_entrypoint = receipt_trace.get("runtime_entrypoint")
    if not isinstance(runtime_entrypoint, Mapping):
        raise ValueError("vLLM KV evidence missing RuntimeSession entrypoint")
    contract = dict(runtime_entrypoint)

    # // 1.2 拒绝 route policy 暴露
    if bool(contract.get("route_policy_visible_to_adapter", True)):
        raise ValueError("vLLM KV evidence exposes route policy to adapter")
    if bool(contract.get("route_policy_visible_to_application", True)):
        raise ValueError("vLLM KV evidence exposes route policy to application")

    # // 1.3 校验 adapter evidence 记录明细
    adapter_record = contract.get("adapter_evidence_record")
    if not isinstance(adapter_record, Mapping):
        raise ValueError("vLLM KV evidence missing adapter evidence record")
    if str(adapter_record.get("evidence_id")) != str(evidence_id):
        raise ValueError("vLLM KV evidence adapter evidence_id mismatch")
    if not bool(adapter_record.get("intents_recorded", False)):
        raise ValueError("vLLM KV evidence adapter intents were not recorded")
    if not bool(adapter_record.get("receipts_recorded", False)):
        raise ValueError("vLLM KV evidence adapter receipts were not recorded")

    # // 1.4 核对 receipt contracts 与 RuntimeSession 记录
    _require_vllm_kv_adapter_record_receipts(
        adapter_record,
        receipt_contracts=receipt_contracts,
    )

    # // 1.5 保留 RuntimeSession 记录摘要
    contract["adapter_evidence_record"] = dict(adapter_record)
    logger.info("vLLM KV RuntimeSession 合约提取完成, evidence_id: %s", evidence_id)
    return contract


def _require_vllm_kv_adapter_record_receipts(
    adapter_record: Mapping[str, object],
    *,
    receipt_contracts: object,
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤2：核对 vLLM KV receipt 记录
    #  * ========================================================================
    #  * 数据源：receipt contracts 与 RuntimeSession adapter evidence record
    #  * 操作：
    #  *   1) 从 receipt contracts 提取 intent_id 和 receipt_id
    #  *   2) 确认 RuntimeSession adapter evidence record 包含这些标识
    #  */
    logger.info("开始核对 vLLM KV receipt 记录...")

    # // 2.1 提取 receipt contract 标识
    expected_intent_ids, expected_receipt_ids = _vllm_kv_receipt_contract_ids(
        receipt_contracts
    )

    # // 2.2 提取 RuntimeSession adapter evidence 标识
    recorded_intent_ids = _vllm_kv_string_set(adapter_record.get("intent_ids"))
    recorded_receipt_ids = _vllm_kv_string_set(adapter_record.get("receipt_ids"))

    # // 2.3 核对 receipt contract 是否都进入 RuntimeSession 记录
    if not expected_intent_ids.issubset(recorded_intent_ids):
        raise ValueError("vLLM KV evidence adapter intent_ids mismatch")
    if not expected_receipt_ids.issubset(recorded_receipt_ids):
        raise ValueError("vLLM KV evidence adapter receipt_ids mismatch")
    logger.info("vLLM KV receipt 记录核对完成, receipts: %s", len(expected_receipt_ids))


def _vllm_kv_receipt_contract_ids(
    receipt_contracts: object,
) -> tuple[set[str], set[str]]:
    # /*
    #  * ========================================================================
    #  * 步骤3：提取 vLLM KV receipt contract 标识
    #  * ========================================================================
    #  * 数据源：receipt_trace.receipt_contracts
    #  * 操作：
    #  *   1) 校验 receipt_contracts 为列表
    #  *   2) 提取 intent_id 和 receipt_id 集合
    #  */
    logger.info("开始提取 vLLM KV receipt contract 标识...")

    # // 3.1 校验 receipt_contracts 结构
    if not isinstance(receipt_contracts, list):
        raise ValueError("vLLM KV evidence missing receipt contracts")

    # // 3.2 收集 intent_id 与 receipt_id
    intent_ids: set[str] = set()
    receipt_ids: set[str] = set()
    for index, contract in enumerate(receipt_contracts):
        if not isinstance(contract, Mapping):
            raise TypeError(
                "vLLM KV receipt contracts must be mappings; "
                f"item {index} is {type(contract).__name__}"
            )
        intent_id = contract.get("intent_id")
        receipt_id = contract.get("receipt_id")
        if intent_id is None or receipt_id is None:
            raise ValueError("vLLM KV receipt contract missing identity fields")
        intent_ids.add(str(intent_id))
        receipt_ids.add(str(receipt_id))

    # // 3.3 拒绝空 receipt contract
    if not receipt_ids:
        raise ValueError("vLLM KV evidence contains no receipt contracts")
    logger.info("vLLM KV receipt contract 标识提取完成, receipts: %s", len(receipt_ids))
    return intent_ids, receipt_ids


def _vllm_kv_string_set(value: object) -> set[str]:
    # /*
    #  * ========================================================================
    #  * 步骤4：归一化 vLLM KV 字符串集合
    #  * ========================================================================
    #  * 数据源：RuntimeSession adapter evidence record
    #  * 操作：
    #  *   1) 字符串按单个标识处理
    #  *   2) 列表和元组转为字符串集合
    #  */
    logger.info("开始归一化 vLLM KV 字符串集合...")

    # // 4.1 字符串按单值处理
    if isinstance(value, str):
        result = {value}
        logger.info("vLLM KV 字符串集合归一化完成, count: %s", len(result))
        return result

    # // 4.2 列表和元组转为字符串集合
    if isinstance(value, list | tuple):
        result = {str(item) for item in value}
        logger.info("vLLM KV 字符串集合归一化完成, count: %s", len(result))
        return result

    # // 4.3 非序列值返回空集合
    logger.info("vLLM KV 字符串集合归一化完成, count: %s", 0)
    return set()


def _csv_values(value: object) -> list[str]:
    if value is None:
        return []
    return [item for item in str(value).split(",") if item]


def _join_unique(values) -> str:
    seen = set()
    ordered = []
    for value in values:
        text = str(value)
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ",".join(ordered)


def _layer_index(layer_name: str, kv_layer, kv_items: list[tuple[str, Any]]) -> int:
    layer_name = str(layer_name)
    for index, (registered_name, registered_layer) in enumerate(kv_items):
        if layer_name == str(registered_name) or kv_layer is registered_layer:
            return index
    raise KeyError(f"unknown vLLM KV layer: {layer_name}")


def _request_prefix_key(params: dict[str, Any]) -> str:
    return str(params.get("turbobus.prefix_key", "default"))


def _flatten_block_ids(groups: tuple[tuple[int, ...], ...]) -> tuple[int, ...]:
    seen = set()
    ordered = []
    for group in groups:
        for block_id in group:
            if block_id not in seen:
                seen.add(block_id)
                ordered.append(block_id)
    return tuple(ordered)


def _iter_scheduled_requests(scheduler_output) -> list[Any]:
    requests = []
    for request in getattr(scheduler_output, "scheduled_new_reqs", []) or []:
        requests.append(request)
    cached = getattr(scheduler_output, "scheduled_cached_reqs", None)
    if isinstance(cached, list):
        requests.extend(cached)
    elif cached is not None:
        req_ids = list(getattr(cached, "req_ids", []) or [])
        new_block_ids = list(getattr(cached, "new_block_ids", []) or [])
        for index, req_id in enumerate(req_ids):
            request = getattr(cached, "requests", {}).get(req_id, None)
            params = _request_params(request) if request is not None else {}
            requests.append(
                _ScheduledRequestView(
                    req_id=str(req_id),
                    new_block_ids=new_block_ids[index] if index < len(new_block_ids) else [],
                    kv_transfer_params=params,
                )
            )
    return requests


def _scheduled_request_id(request) -> str:
    return str(
        getattr(
            request,
            "request_id",
            getattr(request, "req_id", "unknown"),
        )
    )


def _scheduled_request_block_ids(request) -> tuple[int, ...]:
    raw = getattr(request, "new_block_ids", None)
    if raw is None:
        raw = getattr(request, "block_ids", None)
    return _flatten_block_ids(_normalize_block_id_groups(raw))


def _normalize_block_id_groups(raw) -> tuple[tuple[int, ...], ...]:
    if raw is None:
        return tuple()
    if hasattr(raw, "get_block_ids"):
        return extract_vllm_block_ids(raw)
    if isinstance(raw, tuple):
        return tuple(tuple(int(block_id) for block_id in group) for group in raw)
    if isinstance(raw, list):
        if not raw:
            return tuple()
        if all(isinstance(item, int) for item in raw):
            return (tuple(int(item) for item in raw),)
        groups = []
        for group in raw:
            if group is None:
                groups.append(tuple())
            elif isinstance(group, int):
                groups.append((int(group),))
            else:
                groups.append(tuple(int(block_id) for block_id in group))
        return tuple(groups)
    return tuple()


def _block_count_for_tokens(token_count: int, block_size: int) -> int:
    if token_count <= 0:
        return 0
    return (int(token_count) + int(block_size) - 1) // int(block_size)


def _save_block_count(params: dict[str, Any], block_size: int) -> int:
    if "turbobus.save_blocks" in params:
        return int(params.get("turbobus.save_blocks", 0) or 0)
    return _block_count_for_tokens(
        int(params.get("turbobus.matched_tokens", 0) or 0),
        block_size,
    )


def _matched_tokens_for_save(params: dict[str, Any], block_size: int) -> int:
    matched_tokens = int(params.get("turbobus.matched_tokens", 0) or 0)
    if matched_tokens > 0:
        return matched_tokens
    return _save_block_count(params, block_size) * int(block_size)


__all__ = [
    "TurboBusConnector",
    "TurboBusConnectorConfig",
    "TurboBusConnectorMetadata",
    "TurboBusRequestMetadata",
    "TurboBusSavedPrefix",
    "clear_connector_events",
    "get_connector_events",
    "get_saved_prefix",
]
