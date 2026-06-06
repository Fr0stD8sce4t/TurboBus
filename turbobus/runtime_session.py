from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, fields

from .backends.cuda import default_cuda_backend
from .buffer_registration import (
    ExecutableBuffer,
    ranges_or_full_buffer,
    register_executable_buffer,
)
from .client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer
from .daemon.client import (
    TurboBusDaemonClient,
    TurboBusDaemonExecutionClient,
    TurboBusDaemonProfileClient,
    TurboBusDaemonRuntimeClient,
)
from .intent_executor import WorkerIntentTransferExecutor
from .intent_execution_support import require_ok
from .ranges import TransferRange, range_as_dict
from .profiling.bootstrap import bootstrap_daemon_profile
from .runtime.buffers import (
    buffer_registration_fingerprint,
    runtime_session_buffer_metadata,
    validate_runtime_buffer_backing,
    validate_intent_uses_runtime_buffers,
)
from .runtime.daemon_view import RuntimeExecutionDaemonView
from .runtime.session_state import (
    clear_runtime_session_state,
    normalize_runtime_session_config,
    resolve_runtime_role_clients,
)
from .runtime.validation import validate_runtime_receipt
from .runtime_options import RuntimeOptions
from .schema import (
    DaemonResponse,
    TransferIntent,
    TransferReceipt,
    WorkloadKind,
)
from .worker.socket_client import WorkerServiceSocketClient


@dataclass
class TurboBusRuntimeSession:
    daemon_client: object
    job_id: str
    user_id: str | None = None
    runtime_daemon_client: object | None = None
    execution_daemon_client: object | None = None
    profile_daemon_client: object | None = None
    worker_client: object | None = None
    max_inflight_chunks: int = 8
    backend: object = default_cuda_backend
    runtime_options: RuntimeOptions = field(default_factory=RuntimeOptions)
    _target_gpu: int | None = field(default=None, init=False, repr=False)
    _relay_gpus: tuple[int, ...] | None = field(default=None, init=False, repr=False)
    _session_id: str | None = field(default=None, init=False, repr=False)
    _buffers: dict[str, ExecutableBuffer] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _registered_buffer_ids: set[str] = field(
        default_factory=set,
        init=False,
        repr=False,
    )
    _registered_buffer_fingerprints: dict[str, tuple[object, ...]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _owned_cpu_buffer_ids: set[str] = field(
        default_factory=set,
        init=False,
        repr=False,
    )
    _submitted_intent_ids: set[str] = field(
        default_factory=set,
        init=False,
        repr=False,
    )
    _transfer_executor: WorkerIntentTransferExecutor | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _profile_bootstrapped: bool = field(default=False, init=False, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        normalize_runtime_session_config(self)
        resolve_runtime_role_clients(
            self,
            runtime_client_factory=TurboBusDaemonRuntimeClient,
            execution_client_factory=TurboBusDaemonExecutionClient,
            profile_client_factory=TurboBusDaemonProfileClient,
        )

    @classmethod
    def open(
        cls,
        daemon_client,
        *,
        job_id: str,
        user_id: str | None = None,
        runtime_daemon_client: object | None = None,
        execution_daemon_client: object | None = None,
        profile_daemon_client: object | None = None,
        worker_client: object | None = None,
        max_inflight_chunks: int = 8,
        backend=default_cuda_backend,
        runtime_options: RuntimeOptions | None = None,
    ) -> "TurboBusRuntimeSession":
        session = cls(
            daemon_client=daemon_client,
            job_id=str(job_id),
            user_id=user_id,
            runtime_daemon_client=runtime_daemon_client,
            execution_daemon_client=execution_daemon_client,
            profile_daemon_client=profile_daemon_client,
            worker_client=worker_client,
            max_inflight_chunks=int(max_inflight_chunks),
            backend=backend,
            runtime_options=runtime_options or RuntimeOptions(),
        )
        return session

    @classmethod
    def open_socket(
        cls,
        *,
        daemon_socket_path: str,
        job_id: str,
        worker_socket_path: str | None = None,
        user_id: str | None = None,
        max_inflight_chunks: int = 8,
        backend=default_cuda_backend,
        runtime_options: RuntimeOptions | None = None,
    ) -> "TurboBusRuntimeSession":
        if not str(daemon_socket_path).strip():
            raise ValueError("daemon_socket_path must be non-empty")
        worker_client = None
        if worker_socket_path is not None:
            if not str(worker_socket_path).strip():
                raise ValueError("worker_socket_path must be non-empty")
            worker_client = WorkerServiceSocketClient(str(worker_socket_path))
        daemon_client = TurboBusDaemonClient(str(daemon_socket_path))
        return cls.open(
            daemon_client,
            job_id=job_id,
            user_id=user_id,
            runtime_daemon_client=TurboBusDaemonRuntimeClient(str(daemon_socket_path)),
            execution_daemon_client=TurboBusDaemonExecutionClient(
                str(daemon_socket_path)
            ),
            profile_daemon_client=TurboBusDaemonProfileClient(str(daemon_socket_path)),
            worker_client=worker_client,
            max_inflight_chunks=max_inflight_chunks,
            backend=backend,
            runtime_options=runtime_options,
        )

    @classmethod
    def open_production_socket(
        cls,
        *,
        daemon_socket_path: str | None = None,
        worker_socket_path: str | None = None,
        job_id: str,
        user_id: str | None = None,
        max_inflight_chunks: int = 8,
        backend=default_cuda_backend,
        runtime_options: RuntimeOptions | None = None,
    ) -> "TurboBusRuntimeSession":
        options = runtime_options or RuntimeOptions()
        resolved_daemon_socket = (
            daemon_socket_path
            if daemon_socket_path is not None
            else options.daemon_socket_path
        )
        resolved_worker_socket = (
            worker_socket_path
            if worker_socket_path is not None
            else options.worker_socket_path
        )
        if resolved_daemon_socket is None or not str(resolved_daemon_socket).strip():
            raise ValueError("daemon_socket_path must be non-empty")
        if resolved_worker_socket is None or not str(resolved_worker_socket).strip():
            raise ValueError("worker_socket_path must be non-empty")
        return cls.open_socket(
            daemon_socket_path=str(resolved_daemon_socket),
            worker_socket_path=str(resolved_worker_socket),
            job_id=job_id,
            user_id=user_id,
            max_inflight_chunks=max_inflight_chunks,
            backend=backend,
            runtime_options=options,
        )

    @property
    def session_id(self) -> str:
        if self._session_id is None:
            raise RuntimeError("TurboBus runtime session is not open")
        return self._session_id

    @property
    def target_gpu(self) -> int | None:
        return self._target_gpu

    @property
    def relay_gpus(self) -> Sequence[int] | None:
        return self._relay_gpus

    @property
    def closed(self) -> bool:
        return self._closed

    def open_session(self) -> str:
        self._require_open()
        if self._session_id is not None:
            return self._session_id
        if self._target_gpu is None:
            raise RuntimeError(
                "target GPU is not known; register a CUDA buffer before opening "
                "the daemon session"
            )
        session_id: str | None = None
        relay_gpus: tuple[int, ...] = ()
        try:
            response = self._runtime_daemon_client().register_session(
                int(self._target_gpu),
                int(self.max_inflight_chunks),
            )
            require_ok(response, "daemon session registration failed")
            session_payload = response.payload["session"]
            session_id = str(session_payload["session_id"])
            relay_gpus = tuple(
                int(gpu) for gpu in session_payload.get("relay_gpus", ()) or ()
            )
            require_ok(
                self._runtime_daemon_client().register_job(
                    job_id=self.job_id,
                    user_id=self.user_id,
                    session_id=session_id,
                ),
                "daemon job registration failed",
            )
            if bool(self.runtime_options.profile_on_first_transfer):
                self._bootstrap_daemon_profile(relay_gpus, force=False)
        except Exception:
            if session_id is not None:
                try:
                    self._runtime_daemon_client().close_session(session_id)
                except Exception:
                    pass
            self._relay_gpus = None
            self._profile_bootstrapped = False
            raise
        self._relay_gpus = relay_gpus
        self._session_id = session_id
        return session_id

    def register_cpu_buffer(
        self,
        buffer: SharedPinnedCpuBuffer,
        *,
        runtime_owned: bool = False,
    ) -> SharedPinnedCpuBuffer:
        self._require_open()
        if not isinstance(buffer, SharedPinnedCpuBuffer):
            raise TypeError("buffer must be a SharedPinnedCpuBuffer")
        if runtime_owned and not buffer.owner:
            raise ValueError("runtime-owned CPU buffers must own their shared memory")
        owned_added = False
        if runtime_owned:
            owned_added = buffer.buffer_id not in self._owned_cpu_buffer_ids
            self._owned_cpu_buffer_ids.add(buffer.buffer_id)
        try:
            self._register_buffer(buffer)
        except Exception:
            if owned_added:
                self._owned_cpu_buffer_ids.discard(buffer.buffer_id)
            if runtime_owned or getattr(buffer, "owner", False):
                try:
                    buffer.release()
                except Exception:
                    pass
            raise
        return buffer

    def allocate_cpu_buffer(
        self,
        buffer_id: str,
        size_bytes: int,
        *,
        name_prefix: str = "turbobus-runtime",
    ) -> SharedPinnedCpuBuffer:
        self._require_open()
        buffer = SharedPinnedCpuBuffer.allocate(
            buffer_id=str(buffer_id),
            job_id=self.job_id,
            size_bytes=int(size_bytes),
            name_prefix=str(name_prefix),
        )
        try:
            return self.register_cpu_buffer(buffer, runtime_owned=True)
        except Exception:
            buffer.release()
            raise

    def register_cuda_buffer(
        self,
        buffer: CudaIpcDeviceBuffer,
    ) -> CudaIpcDeviceBuffer:
        self._require_open()
        if not isinstance(buffer, CudaIpcDeviceBuffer):
            raise TypeError("buffer must be a CudaIpcDeviceBuffer")
        self._register_buffer(buffer)
        return buffer

    def fetch_h2d(
        self,
        source: SharedPinnedCpuBuffer,
        target: CudaIpcDeviceBuffer,
        *,
        ranges: Iterable[TransferRange | tuple[int, int, int] | dict] | None = None,
        chunk_bytes: int | None = None,
        workload_kind: WorkloadKind | str = WorkloadKind.GENERIC,
        priority: int = 0,
        metadata: Mapping[str, object] | None = None,
        policy_hints: Mapping[str, object] | None = None,
        intent_id: str | None = None,
    ) -> TransferReceipt:
        return self._submit_transfer_intent(
            source,
            target,
            direction="h2d",
            ranges=ranges,
            chunk_bytes=chunk_bytes,
            workload_kind=workload_kind,
            priority=priority,
            metadata=metadata,
            policy_hints=policy_hints,
            intent_id=intent_id,
        )

    def offload_d2h(
        self,
        source: CudaIpcDeviceBuffer,
        target: SharedPinnedCpuBuffer,
        *,
        ranges: Iterable[TransferRange | tuple[int, int, int] | dict] | None = None,
        chunk_bytes: int | None = None,
        workload_kind: WorkloadKind | str = WorkloadKind.GENERIC,
        priority: int = 0,
        metadata: Mapping[str, object] | None = None,
        policy_hints: Mapping[str, object] | None = None,
        intent_id: str | None = None,
    ) -> TransferReceipt:
        return self._submit_transfer_intent(
            source,
            target,
            direction="d2h",
            ranges=ranges,
            chunk_bytes=chunk_bytes,
            workload_kind=workload_kind,
            priority=priority,
            metadata=metadata,
            policy_hints=policy_hints,
            intent_id=intent_id,
        )

    def submit_transfer_intent(self, intent: TransferIntent) -> TransferReceipt:
        return self._submit_runtime_intent(intent)

    def wait_transfer_receipt(
        self,
        intent_id: str,
        timeout_seconds: float | None = None,
    ) -> TransferReceipt:
        self._require_open()
        normalized_intent_id = str(intent_id)
        if normalized_intent_id not in self._submitted_intent_ids:
            raise ValueError(
                "runtime session can only wait for intents submitted through it"
            )
        waiter = getattr(self.daemon_client, "wait_transfer_receipt", None)
        if not callable(waiter):
            raise TypeError("daemon client must support wait_transfer_receipt")
        response = waiter(
            normalized_intent_id,
            timeout_seconds=timeout_seconds,
        )
        receipt = _receipt_from_daemon_response(
            response,
            expected_intent_id=normalized_intent_id,
        )
        validate_runtime_receipt(
            receipt,
            intent_id=normalized_intent_id,
            job_id=self.job_id,
            session_id=self.session_id,
        )
        return receipt

    def bootstrap_profile(self, *, force: bool = False):
        self._require_open()
        self.open_session()
        relays = self._relay_gpus_for_session()
        if self._profile_bootstrapped and not force:
            return DaemonResponse(
                ok=True,
                payload={"bootstrapped": True, "already_bootstrapped": True},
            )
        return self._bootstrap_daemon_profile(relays, force=force)

    def close(self) -> DaemonResponse:
        if self._closed:
            return DaemonResponse(
                ok=True,
                payload={"closed": False, "already_closed": True},
            )
        if self._session_id is None:
            release_evidence = self._release_owned_cpu_buffers(
                reason="runtime_session_close_without_daemon_session"
            )
            clear_runtime_session_state(self)
            self._closed = True
            return DaemonResponse(
                ok=True,
                payload={
                    "closed": False,
                    "owned_cpu_buffer_release": release_evidence,
                },
            )
        cleanup_errors: list[dict[str, object]] = []
        cleanup_evidence: list[dict[str, object]] = []
        release_evidence: list[dict[str, object]] = []
        for buffer_id in tuple(self._registered_buffer_ids):
            try:
                cleanup_response = self.cleanup_buffer(
                    buffer_id,
                    reason="runtime_session_close",
                    force=True,
                )
                cleanup_record: dict[str, object] = {
                    "buffer_id": buffer_id,
                    "ok": bool(cleanup_response.ok),
                }
                if cleanup_response.error:
                    cleanup_record["error"] = cleanup_response.error
                if isinstance(cleanup_response.payload, Mapping):
                    cleanup_record["payload"] = dict(cleanup_response.payload)
                cleanup_evidence.append(cleanup_record)
            except Exception as exc:
                cleanup_errors.append(
                    {
                        "buffer_id": buffer_id,
                        "error": str(exc),
                    }
                )
        try:
            response = self._runtime_daemon_client().close_session(self._session_id)
        except Exception as exc:
            response = DaemonResponse(ok=False, error=str(exc))
        finally:
            release_evidence = self._release_owned_cpu_buffers(
                reason="runtime_session_close"
            )
            clear_runtime_session_state(self)
            self._closed = True
        payload = {}
        if isinstance(response.payload, Mapping):
            payload.update(dict(response.payload))
        if cleanup_evidence:
            payload["buffer_cleanup_evidence"] = cleanup_evidence
        if release_evidence:
            payload["owned_cpu_buffer_release"] = release_evidence
        if cleanup_errors:
            payload["buffer_cleanup_errors"] = cleanup_errors
            return DaemonResponse(
                ok=response.ok,
                error=response.error,
                payload=payload,
            )
        if payload != response.payload:
            return DaemonResponse(ok=response.ok, error=response.error, payload=payload)
        return response

    def __enter__(self) -> "TurboBusRuntimeSession":
        self._require_open()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def _register_buffer(self, buffer: ExecutableBuffer) -> None:
        self._require_open()
        validate_runtime_buffer_backing(buffer)
        if buffer.job_id != self.job_id:
            raise ValueError("buffer job_id must match the runtime session job_id")
        existing = self._buffers.get(buffer.buffer_id)
        if (
            existing is not None
            and existing is not buffer
            and buffer.buffer_id in self._owned_cpu_buffer_ids
        ):
            raise ValueError(
                "runtime-owned CPU buffer must be cleaned up before reusing buffer_id"
            )
        if isinstance(buffer, CudaIpcDeviceBuffer):
            self._bind_target_gpu(buffer.device_index)
        self._buffers[buffer.buffer_id] = buffer
        try:
            if self._target_gpu is None:
                return
            self.open_session()
            self._register_pending_buffers()
        except Exception:
            if existing is None:
                self._buffers.pop(buffer.buffer_id, None)
            else:
                self._buffers[buffer.buffer_id] = existing
            raise

    def _submit_transfer_intent(
        self,
        source: ExecutableBuffer,
        target: ExecutableBuffer,
        *,
        direction: str,
        ranges: Iterable[TransferRange | tuple[int, int, int] | dict] | None,
        chunk_bytes: int | None,
        workload_kind: WorkloadKind | str,
        priority: int,
        metadata: Mapping[str, object] | None,
        policy_hints: Mapping[str, object] | None = None,
        intent_id: str | None = None,
    ) -> TransferReceipt:
        self._require_open()
        self._ensure_transfer_buffers(source, target)
        normalized_ranges = tuple(
            range_as_dict(item)
            for item in ranges_or_full_buffer(ranges, source.size_bytes, target.size_bytes)
        )
        total_bytes = sum(int(item["bytes"]) for item in normalized_ranges)
        resolved_chunk_bytes = (
            int(self.runtime_options.chunk_bytes)
            if chunk_bytes is None
            else int(chunk_bytes)
        )
        resolved_policy_hints = dict({} if policy_hints is None else policy_hints)
        resolved_policy_hints["chunk_bytes"] = resolved_chunk_bytes
        intent = TransferIntent(
            intent_id=(
                f"intent-{uuid.uuid4().hex}"
                if intent_id is None
                else str(intent_id)
            ),
            job_id=self.job_id,
            session_id=self.session_id,
            source_buffer_id=source.buffer_id,
            destination_buffer_id=target.buffer_id,
            direction=direction,
            total_bytes=total_bytes,
            ranges=normalized_ranges,
            workload_kind=workload_kind,
            priority=int(priority),
            policy_hints=resolved_policy_hints,
            metadata={} if metadata is None else dict(metadata),
        )
        return self._submit_runtime_intent(intent)

    def make_adapter_transfer_context(
        self,
        cpu_buffer,
        gpu_buffer,
        *,
        workload_kind: WorkloadKind | str = WorkloadKind.GENERIC,
        priority: int = 0,
        policy_hints: Mapping[str, object] | None = None,
        metadata: Mapping[str, object] | None = None,
        intent_prefix: str | None = None,
        wait_timeout_seconds: float | None = None,
    ):
        self._require_open()
        from .offload.context import AdapterTransferContext

        session_was_open = self._session_id is not None
        cpu_buffer_id = str(getattr(cpu_buffer, "buffer_id", ""))
        gpu_buffer_id = str(getattr(gpu_buffer, "buffer_id", ""))
        cpu_buffer_was_registered = cpu_buffer_id in self._buffers
        gpu_buffer_was_registered = gpu_buffer_id in self._buffers
        session_id: str | None = None
        try:
            gpu_buffer = self.register_cuda_buffer(gpu_buffer)
            session_id = self.open_session()
            cpu_buffer = self.register_cpu_buffer(cpu_buffer)
            resolved_policy_hints = {} if policy_hints is None else dict(policy_hints)
            if "chunk_bytes" not in resolved_policy_hints:
                resolved_policy_hints["chunk_bytes"] = int(
                    getattr(self.runtime_options, "chunk_bytes", 16 * 1024 * 1024)
                )
            return AdapterTransferContext(
                job_id=self.job_id,
                session_id=session_id,
                cpu_buffer_id=cpu_buffer.buffer_id,
                gpu_buffer_id=gpu_buffer.buffer_id,
                cpu_buffer=cpu_buffer,
                gpu_buffer=gpu_buffer,
                workload_kind=workload_kind,
                priority=priority,
                policy_hints=resolved_policy_hints,
                metadata={} if metadata is None else metadata,
                intent_prefix=intent_prefix,
                wait_timeout_seconds=wait_timeout_seconds,
            )
        except Exception:
            self._rollback_adapter_transfer_context_buffer(
                cpu_buffer_id,
                was_registered=cpu_buffer_was_registered,
            )
            self._rollback_adapter_transfer_context_buffer(
                gpu_buffer_id,
                was_registered=gpu_buffer_was_registered,
            )
            if not session_was_open and self._session_id is not None:
                try:
                    self._runtime_daemon_client().close_session(self._session_id)
                except Exception:
                    pass
                self._session_id = None
                self._relay_gpus = None
                self._profile_bootstrapped = False
            raise

    def make_offload_store(
        self,
        cpu_buffer,
        gpu_buffer,
        *,
        workload_kind: WorkloadKind | str = WorkloadKind.GENERIC,
        priority: int = 0,
        policy_hints: Mapping[str, object] | None = None,
        metadata: Mapping[str, object] | None = None,
        intent_prefix: str | None = None,
        wait_timeout_seconds: float | None = None,
    ):
        from .offload.store import OffloadStore

        context = self.make_adapter_transfer_context(
            cpu_buffer,
            gpu_buffer,
            workload_kind=workload_kind,
            priority=priority,
            policy_hints=policy_hints,
            metadata=metadata,
            intent_prefix=intent_prefix,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        return OffloadStore(self, context)

    def make_training_offload_manager(
        self,
        cpu_buffer,
        gpu_buffer,
        *,
        workload_kind: WorkloadKind | str = WorkloadKind.TRAINING_STATE,
        priority: int = 0,
        metadata: Mapping[str, object] | None = None,
        intent_prefix: str | None = None,
        wait_timeout_seconds: float | None = None,
    ):
        from .adapters.training_offload import TrainingOffloadManager

        context = self.make_adapter_transfer_context(
            cpu_buffer,
            gpu_buffer,
            workload_kind=workload_kind,
            priority=priority,
            metadata=metadata,
            intent_prefix=intent_prefix,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        return TrainingOffloadManager._from_transfer_context(
            self,
            context,
            cpu_buffer,
            gpu_buffer,
        )

    def make_model_weight_loader(
        self,
        cpu_buffer,
        gpu_buffer,
        *,
        priority: int = 0,
        metadata: Mapping[str, object] | None = None,
        intent_prefix: str | None = None,
        wait_timeout_seconds: float | None = None,
    ):
        from .adapters.model_loading import ModelWeightLoader

        context = self.make_adapter_transfer_context(
            cpu_buffer,
            gpu_buffer,
            workload_kind=WorkloadKind.MODEL_WEIGHTS,
            priority=priority,
            metadata=metadata,
            intent_prefix=intent_prefix,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        return ModelWeightLoader._from_transfer_context(
            self,
            context,
            cpu_buffer,
            gpu_buffer,
        )

    def make_inference_kv_slot_adapter(
        self,
        cpu_backing,
        gpu_kv_backing,
        *,
        workload_kind: WorkloadKind | str = WorkloadKind.KV_CACHE,
        priority: int = 0,
        metadata: Mapping[str, object] | None = None,
        intent_prefix: str | None = None,
        wait_timeout_seconds: float | None = None,
    ):
        from .adapters.inference import InferenceKVSlotAdapter

        context = self.make_adapter_transfer_context(
            cpu_backing,
            gpu_kv_backing,
            workload_kind=workload_kind,
            priority=priority,
            metadata=metadata,
            intent_prefix=intent_prefix,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        return InferenceKVSlotAdapter._from_transfer_context(
            self,
            context,
            cpu_backing,
            gpu_kv_backing,
        )

    def make_vllm_kv_slot_adapter(
        self,
        groups,
        *,
        workload_kind: WorkloadKind | str = WorkloadKind.KV_CACHE,
        priority: int = 0,
        metadata: Mapping[str, object] | None = None,
        intent_prefix: str | None = None,
        wait_timeout_seconds: float | None = None,
        gpu_buffer_id: str = "vllm-kv-gpu",
    ):
        from .adapters.vllm import VllmKVSlotAdapter

        return VllmKVSlotAdapter(
            self,
            groups,
            workload_kind=workload_kind,
            priority=priority,
            metadata=metadata,
            intent_prefix=intent_prefix,
            wait_timeout_seconds=wait_timeout_seconds,
            gpu_buffer_id=gpu_buffer_id,
        )

    def make_vllm_turbobus_integration(
        self,
        cpu_backings: Iterable | None = None,
        *,
        workload_kind: WorkloadKind | str = WorkloadKind.KV_CACHE,
        priority: int = 0,
        metadata: Mapping[str, object] | None = None,
        intent_prefix: str | None = None,
        wait_timeout_seconds: float | None = None,
        cpu_buffer_id: str = "vllm-kv-cpu",
        gpu_buffer_id: str = "vllm-kv-gpu",
    ):
        from .adapters.vllm_integration import VllmTurboBusIntegration

        return VllmTurboBusIntegration(
            self,
            cpu_backings,
            workload_kind=workload_kind,
            priority=priority,
            metadata=metadata,
            intent_prefix=intent_prefix,
            wait_timeout_seconds=wait_timeout_seconds,
            cpu_buffer_id=cpu_buffer_id,
            gpu_buffer_id=gpu_buffer_id,
        )

    def _ensure_transfer_buffers(
        self,
        source: ExecutableBuffer,
        target: ExecutableBuffer,
    ) -> None:
        self._require_open()
        if source.job_id != self.job_id or target.job_id != self.job_id:
            raise ValueError("transfer buffers must match the runtime session job_id")
        if source.buffer_id not in self._buffers:
            self._register_buffer(source)
        if target.buffer_id not in self._buffers:
            self._register_buffer(target)
        self.open_session()
        self._register_pending_buffers()

    def _submit_runtime_intent(self, intent: TransferIntent) -> TransferReceipt:
        self._prepare_runtime_intent(intent)
        receipt = self._execute_intent_through_daemon(intent)
        validate_runtime_receipt(
            receipt,
            intent_id=intent.intent_id,
            job_id=self.job_id,
            session_id=self.session_id,
        )
        self._submitted_intent_ids.add(intent.intent_id)
        return receipt

    def _prepare_runtime_intent(self, intent: TransferIntent) -> None:
        self._require_open()
        if not isinstance(intent, TransferIntent):
            raise TypeError("intent must be a TransferIntent")
        if intent.job_id != self.job_id:
            raise ValueError("intent job_id must match the runtime session job_id")
        self.open_session()
        if intent.session_id != self.session_id:
            raise ValueError("intent session_id must match the runtime session_id")
        if intent.source_buffer_id not in self._buffers:
            raise ValueError("intent source buffer is not registered with the session")
        if intent.destination_buffer_id not in self._buffers:
            raise ValueError("intent destination buffer is not registered with the session")
        self._validate_intent_uses_runtime_buffers(intent)
        self._register_pending_buffers()
        self._bootstrap_profile_if_enabled()

    def _execute_intent_through_daemon(self, intent: TransferIntent) -> TransferReceipt:
        self._require_open()
        response = self.daemon_client.submit_transfer_intent(intent)
        execution_view = RuntimeExecutionDaemonView(
            intent_daemon=self.daemon_client,
            execution_daemon=self._execution_daemon_client(),
        )
        return self.make_worker_intent_transfer_executor().execute_transfer_intent(
            intent,
            response,
            execution_view,
        )

    def make_worker_intent_transfer_executor(self) -> WorkerIntentTransferExecutor:
        """Return the session-owned worker intent executor."""
        self._require_open()
        if self._transfer_executor is not None:
            return self._transfer_executor
        self._transfer_executor = WorkerIntentTransferExecutor(
            buffers=self._buffers,
            worker_client=self.worker_client,
            backend=self.backend,
            runtime_options=self.runtime_options,
        )
        return self._transfer_executor

    _runtime_transfer_executor = make_worker_intent_transfer_executor

    def _register_pending_buffers(self) -> None:
        self._require_open()
        self.session_id
        registered_now: list[str] = []
        try:
            for buffer_id, buffer in tuple(self._buffers.items()):
                runtime_owned = buffer_id in self._owned_cpu_buffer_ids
                fingerprint = (
                    buffer_registration_fingerprint(buffer),
                    self.session_id,
                    runtime_owned,
                )
                if self._registered_buffer_fingerprints.get(buffer_id) == fingerprint:
                    continue
                metadata = runtime_session_buffer_metadata(
                    buffer,
                    session_id=self.session_id,
                    runtime_owned=runtime_owned,
                )
                register_executable_buffer(
                    self._runtime_daemon_client(),
                    buffer,
                    metadata=metadata,
                )
                registered_now.append(buffer_id)
                self._registered_buffer_ids.add(buffer_id)
                self._registered_buffer_fingerprints[buffer_id] = fingerprint
        except Exception:
            for buffer_id in reversed(registered_now):
                try:
                    self.cleanup_buffer(
                        buffer_id,
                        reason="runtime_buffer_registration_failed",
                        force=True,
                    )
                except Exception:
                    pass
            raise


    def cleanup_buffer(
        self,
        buffer_id: str,
        *,
        reason: str = "runtime_buffer_released",
        force: bool = False,
    ) -> DaemonResponse:
        self._require_open()
        normalized_id = str(buffer_id)
        buffer = self._buffers.get(normalized_id)
        response = DaemonResponse(ok=True, payload={"cleanup_skipped": True})
        cleanup_error: Exception | None = None
        release_error: Exception | None = None
        release_evidence: dict[str, object] | None = None
        if normalized_id in self._registered_buffer_ids:
            try:
                response = self._execution_daemon_client().cleanup(
                    target_kind="buffer",
                    target_id=normalized_id,
                    reason=reason,
                    force=force,
                )
                require_ok(response, "daemon buffer cleanup failed")
            except Exception as exc:
                cleanup_error = exc
        self._registered_buffer_ids.discard(normalized_id)
        self._registered_buffer_fingerprints.pop(normalized_id, None)
        self._buffers.pop(normalized_id, None)
        if normalized_id in self._owned_cpu_buffer_ids:
            self._owned_cpu_buffer_ids.discard(normalized_id)
            if isinstance(buffer, SharedPinnedCpuBuffer):
                release_evidence = self._release_owned_cpu_buffer(
                    normalized_id,
                    buffer,
                    reason=reason,
                )
                if not bool(release_evidence.get("ok", False)):
                    release_error = RuntimeError(
                        str(release_evidence.get("error") or "local buffer release failed")
                    )
        if cleanup_error is not None:
            if release_error is not None:
                raise RuntimeError(
                    f"{cleanup_error}; local buffer release failed: {release_error}"
                ) from cleanup_error
            raise cleanup_error
        if release_error is not None:
            raise release_error
        payload = dict(response.payload) if isinstance(response.payload, Mapping) else {}
        if release_evidence is not None:
            payload["owned_cpu_buffer_release"] = release_evidence
        return DaemonResponse(ok=response.ok, error=response.error, payload=payload)

    def _rollback_adapter_transfer_context_buffer(
        self,
        buffer_id: str,
        *,
        was_registered: bool,
    ) -> None:
        normalized_id = str(buffer_id)
        if not normalized_id or was_registered:
            return
        if self._session_id is not None and normalized_id in self._registered_buffer_ids:
            try:
                self.cleanup_buffer(
                    normalized_id,
                    reason="runtime_adapter_context_creation_failed",
                    force=True,
                )
                return
            except Exception:
                pass
        buffer = self._buffers.pop(normalized_id, None)
        self._registered_buffer_ids.discard(normalized_id)
        self._registered_buffer_fingerprints.pop(normalized_id, None)
        if normalized_id in self._owned_cpu_buffer_ids:
            self._owned_cpu_buffer_ids.discard(normalized_id)
            if isinstance(buffer, SharedPinnedCpuBuffer):
                self._release_owned_cpu_buffer(
                    normalized_id,
                    buffer,
                    reason="runtime_adapter_context_creation_failed",
                )

    def _release_owned_cpu_buffers(self, *, reason: str) -> list[dict[str, object]]:
        evidence: list[dict[str, object]] = []
        for buffer_id in tuple(self._owned_cpu_buffer_ids):
            buffer = self._buffers.get(buffer_id)
            self._owned_cpu_buffer_ids.discard(buffer_id)
            if isinstance(buffer, SharedPinnedCpuBuffer):
                evidence.append(
                    self._release_owned_cpu_buffer(
                        buffer_id,
                        buffer,
                        reason=reason,
                    )
                )
        return evidence

    def _release_owned_cpu_buffer(
        self,
        buffer_id: str,
        buffer: SharedPinnedCpuBuffer,
        *,
        reason: str,
    ) -> dict[str, object]:
        evidence = {
            "buffer_id": str(buffer_id),
            "job_id": buffer.job_id,
            "reason": str(reason),
            "runtime_owned": True,
            "owner": bool(buffer.owner),
            "shared_memory_name": buffer.shared_memory_name,
            "closed_before_release": buffer.closed,
            "unlinked_before_release": bool(getattr(buffer, "_unlinked", False)),
            "ok": False,
        }
        try:
            buffer.release()
        except Exception as exc:
            evidence["error"] = str(exc) or exc.__class__.__name__
            evidence["closed_after_release"] = buffer.closed
            evidence["unlinked_after_release"] = bool(getattr(buffer, "_unlinked", False))
            return evidence
        evidence["ok"] = True
        evidence["closed_after_release"] = buffer.closed
        evidence["unlinked_after_release"] = bool(getattr(buffer, "_unlinked", False))
        return evidence

    def _runtime_daemon_client(self):
        self._require_open()
        if self.runtime_daemon_client is None:
            raise RuntimeError("runtime daemon client is not configured")
        return self.runtime_daemon_client

    def _execution_daemon_client(self):
        self._require_open()
        if self.execution_daemon_client is None:
            raise RuntimeError("execution daemon client is not configured")
        return self.execution_daemon_client

    def _profile_daemon_client(self):
        self._require_open()
        if self.profile_daemon_client is None:
            raise RuntimeError("profile daemon client is not configured")
        return self.profile_daemon_client

    def _bind_target_gpu(self, device_index: int) -> None:
        self._require_open()
        device = int(device_index)
        if self._target_gpu is None:
            self._target_gpu = device
            return
        if int(self._target_gpu) != device:
            raise ValueError("CUDA buffer device_index must match runtime target_gpu")

    def _relay_gpus_for_session(self) -> tuple[int, ...]:
        self._require_open()
        if self._relay_gpus is not None:
            return self._relay_gpus
        discovery = getattr(self._profile_daemon_client(), "discover_relays", None)
        if not callable(discovery):
            raise RuntimeError(
                "daemon client must support relay discovery for runtime sessions"
            )
        response = discovery(target_gpu=int(self._target_gpu))
        require_ok(response, "daemon relay discovery failed")
        payload = response.payload.get("relay_discovery")
        if not isinstance(payload, Mapping):
            raise RuntimeError("daemon relay discovery response is missing payload")
        eligibility = payload.get("relay_eligibility")
        if not isinstance(eligibility, Mapping):
            raise RuntimeError("daemon relay discovery response is missing eligibility")
        relays = tuple(
            int(item["relay_gpu"])
            for item in eligibility.get("eligible_relays", ()) or ()
            if isinstance(item, Mapping)
        )
        self._relay_gpus = relays
        return relays

    def _bootstrap_profile_if_enabled(self) -> None:
        self._require_open()
        if self._profile_bootstrapped:
            return
        if not bool(self.runtime_options.profile_on_first_transfer):
            return
        self.bootstrap_profile(force=False)

    def _bootstrap_daemon_profile(
        self,
        relay_gpus: Iterable[int],
        *,
        force: bool,
    ) -> DaemonResponse:
        _profile, response = bootstrap_daemon_profile(
            self._profile_daemon_client(),
            self.backend,
            self.runtime_options,
            target_gpu=int(self._target_gpu),
            relay_gpus=relay_gpus,
            force=force,
        )
        self._profile_bootstrapped = True
        return response

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("TurboBus runtime session is closed")

    def _validate_intent_uses_runtime_buffers(self, intent: TransferIntent) -> None:
        source = self._buffers[intent.source_buffer_id]
        target = self._buffers[intent.destination_buffer_id]
        validate_intent_uses_runtime_buffers(
            intent,
            source=source,
            target=target,
        )


def _receipt_from_daemon_response(
    response: DaemonResponse,
    *,
    expected_intent_id: str,
) -> TransferReceipt:
    if not isinstance(response, DaemonResponse):
        raise TypeError("daemon response must be a DaemonResponse")
    if not response.ok:
        raise RuntimeError(response.error or "daemon receipt wait failed")
    receipt_payload = response.payload.get("receipt")
    if not isinstance(receipt_payload, Mapping):
        raise ValueError("daemon response missing receipt")
    receipt = _transfer_receipt_from_payload(receipt_payload)
    if receipt.intent_id != str(expected_intent_id):
        raise ValueError("daemon receipt intent_id does not match request")
    return receipt


def _transfer_receipt_from_payload(payload: Mapping[str, object]) -> TransferReceipt:
    names = {field.name for field in fields(TransferReceipt)}
    unknown = sorted(key for key in payload if key not in names)
    if unknown:
        raise ValueError("daemon receipt contains unknown fields: " + ", ".join(unknown))
    return TransferReceipt(**dict(payload))


__all__ = ["TurboBusRuntimeSession"]
