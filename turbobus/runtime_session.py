from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace

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
    TurboBusPersistentDaemonRuntimeClient,
    TurboBusDaemonProfileClient,
    TurboBusDaemonRuntimeClient,
)
from .daemon.startup import DaemonStartupConfig, create_production_daemon
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
from .runtime.lifecycle import (
    copy_buffer_lifecycle_record as _copy_buffer_lifecycle_record,
    copy_lifecycle_mapping as _copy_lifecycle_mapping,
    owned_cpu_release_records as _owned_cpu_release_records,
    receipt_from_daemon_response as _receipt_from_daemon_response,
    runtime_buffer_retention_evidence as _runtime_buffer_retention_evidence,
    transfer_receipt_from_payload as _transfer_receipt_from_payload,
)
from .runtime.lifecycle_records import (
    close_active_intent_receipts as _close_active_intent_receipts,
    finalize_runtime_receipt as _finalize_runtime_receipt,
    record_buffer_lifecycle_cleanup as _record_buffer_lifecycle_cleanup,
    record_buffer_lifecycle_intent_use as _record_buffer_lifecycle_intent_use,
    record_buffer_lifecycle_receipt as _record_buffer_lifecycle_receipt,
    record_buffer_lifecycle_registration as _record_buffer_lifecycle_registration,
    recover_active_intent_receipts as _recover_active_intent_receipts,
)
from .runtime.managed_services import (
    ManagedProductionStartupError,
    bootstrap_attached_runtime_services,
    bootstrap_owned_runtime_services,
    attach_runtime_managed_service_state,
    managed_service_runtime_snapshot,
    runtime_options_with_optional_socket_paths,
    runtime_options_with_socket_paths,
)
from .runtime.route_policy import (
    require_intent_control_plane_safe,
    runtime_metadata_without_physical_routes,
    runtime_policy_hints_without_physical_routes,
)
from .runtime.session_state import (
    clear_runtime_session_state,
    normalize_runtime_session_config,
    resolve_runtime_role_clients,
)
from .runtime.session_records import (
    initialize_runtime_entrypoint_record,
    record_runtime_adapter_context,
    record_runtime_adapter_evidence,
    record_runtime_buffer_cleanup,
    record_runtime_buffer_registered,
    record_runtime_close_recovery,
    record_runtime_daemon_execution,
    record_runtime_intent_submitted,
    record_runtime_receipt_finalized,
    record_runtime_session_close,
    record_runtime_session_open,
    record_runtime_transfer_recovery,
    runtime_entrypoint_snapshot,
)
from .runtime_options import RuntimeOptions
from .schema import (
    DaemonResponse,
    TransferIntent,
    TransferReceipt,
    WorkloadKind,
)
from .worker.socket_client import WorkerServiceSocketClient

logger = logging.getLogger(__name__)


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
    _submitted_intent_buffers: dict[str, tuple[str, str]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _buffer_lifecycle_records: dict[str, dict[str, object]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _active_intent_ids: set[str] = field(
        default_factory=set,
        init=False,
        repr=False,
    )
    _transfer_executor: WorkerIntentTransferExecutor | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _owned_daemon_stop_event: threading.Event | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _owned_daemon_thread: threading.Thread | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _owned_worker_stop_event: threading.Event | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _owned_worker_thread: threading.Thread | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _owned_daemon_socket_path: str | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _owned_worker_socket_path: str | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _runtime_control_connection_owned: bool = field(
        default=False,
        init=False,
        repr=False,
    )
    _managed_service_startup_evidence: dict[str, object] | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _managed_service_records: dict[str, dict[str, object]] | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _managed_service_lock: threading.Lock | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _profile_bootstrapped: bool = field(default=False, init=False, repr=False)
    _profile_bootstrap_evidence: dict[str, object] | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _runtime_entrypoint_record: dict[str, object] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _closed: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        normalize_runtime_session_config(self)
        resolve_runtime_role_clients(
            self,
            daemon_client_factory=TurboBusDaemonClient,
            runtime_client_factory=TurboBusDaemonRuntimeClient,
            execution_client_factory=TurboBusDaemonExecutionClient,
            profile_client_factory=TurboBusDaemonProfileClient,
            worker_client_factory=WorkerServiceSocketClient,
        )
        initialize_runtime_entrypoint_record(self)

    @classmethod
    def open(
        cls,
        daemon_client=None,
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
        persistent_runtime_control: bool = False,
    ) -> "TurboBusRuntimeSession":
        if not str(daemon_socket_path).strip():
            raise ValueError("daemon_socket_path must be non-empty")
        if worker_socket_path is not None and not str(worker_socket_path).strip():
            raise ValueError("worker_socket_path must be non-empty")
        resolved_options = runtime_options or RuntimeOptions()
        resolved_options = runtime_options_with_optional_socket_paths(
            resolved_options,
            daemon_socket_path=str(daemon_socket_path),
            worker_socket_path=(
                None if worker_socket_path is None else str(worker_socket_path)
            ),
        )
        runtime_client_factory = (
            TurboBusPersistentDaemonRuntimeClient
            if persistent_runtime_control
            else TurboBusDaemonRuntimeClient
        )
        session = cls.open(
            None,
            job_id=job_id,
            user_id=user_id,
            runtime_daemon_client=runtime_client_factory(str(daemon_socket_path)),
            max_inflight_chunks=max_inflight_chunks,
            backend=backend,
            runtime_options=resolved_options,
        )
        session._runtime_control_connection_owned = bool(persistent_runtime_control)
        return session

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
        options = runtime_options_with_socket_paths(
            options,
            daemon_socket_path=str(resolved_daemon_socket),
            worker_socket_path=str(resolved_worker_socket),
        )
        daemon_path = str(resolved_daemon_socket)
        worker_path = str(resolved_worker_socket)
        (
            startup_records,
            startup_lock,
            startup_evidence,
            worker_stop_event,
            worker_thread,
        ) = bootstrap_attached_runtime_services(
            daemon_socket_path=daemon_path,
            worker_socket_path=worker_path,
            backend=backend,
            runtime_options=options,
        )
        session = cls.open_socket(
            daemon_socket_path=daemon_path,
            worker_socket_path=worker_path,
            job_id=job_id,
            user_id=user_id,
            max_inflight_chunks=max_inflight_chunks,
            backend=backend,
            runtime_options=options,
            persistent_runtime_control=False,
        )
        attach_runtime_managed_service_state(
            session,
            startup_records=startup_records,
            startup_lock=startup_lock,
            startup_evidence=startup_evidence,
            daemon_socket_path=None,
            daemon_stop_event=None,
            daemon_thread=None,
            worker_socket_path=(
                worker_path
                if worker_stop_event is not None and worker_thread is not None
                else None
            ),
            worker_stop_event=worker_stop_event,
            worker_thread=worker_thread,
            runtime_control_owned=False,
        )
        return session

    @classmethod
    def open_managed_production_socket(
        cls,
        *,
        job_id: str,
        daemon_socket_path: str,
        worker_socket_path: str,
        daemon_startup_config: DaemonStartupConfig | None = None,
        user_id: str | None = None,
        max_inflight_chunks: int = 8,
        backend=default_cuda_backend,
        runtime_options: RuntimeOptions | None = None,
    ) -> "TurboBusRuntimeSession":
        options = runtime_options or RuntimeOptions()
        daemon_path = str(daemon_socket_path)
        worker_path = str(worker_socket_path)
        if not daemon_path.strip():
            raise ValueError("daemon_socket_path must be non-empty")
        if not worker_path.strip():
            raise ValueError("worker_socket_path must be non-empty")
        options = runtime_options_with_socket_paths(
            options,
            daemon_socket_path=daemon_path,
            worker_socket_path=worker_path,
        )
        daemon = create_production_daemon(
            _daemon_startup_config_with_runtime_planner_options(
                daemon_startup_config or DaemonStartupConfig(),
                options,
            )
        )
        (
            startup_records,
            startup_lock,
            startup_evidence,
            daemon_stop_event,
            daemon_thread,
            worker_stop_event,
            worker_thread,
        ) = bootstrap_owned_runtime_services(
            daemon=daemon,
            daemon_socket_path=daemon_path,
            worker_socket_path=worker_path,
            backend=backend,
            runtime_options=options,
        )
        session = cls.open_socket(
            daemon_socket_path=daemon_path,
            worker_socket_path=worker_path,
            job_id=job_id,
            user_id=user_id,
            max_inflight_chunks=max_inflight_chunks,
            backend=backend,
            runtime_options=options,
            persistent_runtime_control=False,
        )
        attach_runtime_managed_service_state(
            session,
            startup_records=startup_records,
            startup_lock=startup_lock,
            startup_evidence=startup_evidence,
            daemon_socket_path=daemon_path,
            daemon_stop_event=daemon_stop_event,
            daemon_thread=daemon_thread,
            worker_socket_path=worker_path,
            worker_stop_event=worker_stop_event,
            worker_thread=worker_thread,
            runtime_control_owned=False,
        )
        return session

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
        self._ensure_managed_services_alive("open_session")
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
                connection_scoped=bool(self._runtime_control_connection_owned),
                worker_relay_capable=self.worker_client is not None,
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
            self._profile_bootstrap_evidence = None
            raise
        self._relay_gpus = relay_gpus
        self._session_id = session_id
        record_runtime_session_open(
            self._runtime_entrypoint_record,
            session_id=session_id,
            target_gpu=int(self._target_gpu),
            relay_gpus=relay_gpus,
            worker_relay_capable=self.worker_client is not None,
            profile_bootstrap=self.profile_bootstrap_snapshot(),
        )
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

    def buffer_lifecycle_snapshot(self) -> dict[str, object]:
        self._require_open()
        records = {
            buffer_id: _copy_buffer_lifecycle_record(record)
            for buffer_id, record in sorted(self._buffer_lifecycle_records.items())
        }
        return {
            "job_id": str(self.job_id),
            "session_id": self._session_id,
            "active_intent_ids": tuple(sorted(self._active_intent_ids)),
            "registered_buffer_ids": tuple(sorted(self._registered_buffer_ids)),
            "runtime_owned_cpu_buffer_ids": tuple(sorted(self._owned_cpu_buffer_ids)),
            "buffers": records,
        }

    def runtime_entrypoint_snapshot(self) -> dict[str, object]:
        self._require_open()
        return runtime_entrypoint_snapshot(self)

    def record_adapter_lifecycle_evidence(
        self,
        *,
        evidence_id: str,
        operation: str,
        intent_ids: Sequence[str],
        receipt_ids: Sequence[str],
    ) -> None:
        # /*
        #  * ========================================================================
        #  * 步骤1：绑定适配器生命周期证据
        #  * ========================================================================
        #  * 目标对象：TurboBusRuntimeSession entrypoint record
        #  * 操作：
        #  *   1) 接收 adapter lifecycle evidence 的 intent/receipt 标识
        #  *   2) 写入 RuntimeSession 生产入口快照
        #  */
        logger.info("开始绑定适配器生命周期证据...")

        # // 1.1 确认 RuntimeSession 仍处于打开状态
        self._require_open()

        # // 1.2 写入 RuntimeSession entrypoint record
        record_runtime_adapter_evidence(
            self._runtime_entrypoint_record,
            evidence_id=evidence_id,
            operation=operation,
            intent_ids=tuple(str(intent_id) for intent_id in intent_ids),
            receipt_ids=tuple(str(receipt_id) for receipt_id in receipt_ids),
        )
        logger.info("适配器生命周期证据绑定完成, evidence_id: %s", evidence_id)

    def record_adapter_transfer_context(
        self,
        *,
        context_id: str,
        workload_kind: str,
        cpu_buffer_id: str,
        gpu_buffer_id: str,
        intent_prefix: str,
        priority: int,
        policy_hints: Mapping[str, object],
        metadata: Mapping[str, object],
        state: str = "created",
        error: str | None = None,
    ) -> None:
        # /*
        #  * ========================================================================
        #  * 步骤1：绑定适配器构造证据
        #  * ========================================================================
        #  * 目标对象：TurboBusRuntimeSession entrypoint record
        #  * 操作：
        #  *   1) 接收 AdapterTransferContext 的构造绑定
        #  *   2) 写入 RuntimeSession 生产入口快照
        #  */
        logger.info("开始绑定适配器构造证据...")

        # // 1.1 确认 RuntimeSession 仍处于打开状态
        self._require_open()

        # // 1.2 写入 RuntimeSession entrypoint record
        record_runtime_adapter_context(
            self._runtime_entrypoint_record,
            context_id=context_id,
            workload_kind=workload_kind,
            cpu_buffer_id=cpu_buffer_id,
            gpu_buffer_id=gpu_buffer_id,
            intent_prefix=intent_prefix,
            priority=priority,
            policy_hints=policy_hints,
            metadata=metadata,
            state=state,
            error=error,
        )
        logger.info("适配器构造证据绑定完成, context_id: %s", context_id)

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

    def submit_transfer_intent(
        self,
        intent: TransferIntent,
        *,
        wait: bool = True,
        timeout_seconds: float | None = None,
    ) -> TransferReceipt:
        return self._submit_runtime_intent(
            intent,
            wait=wait,
            timeout_seconds=timeout_seconds,
        )

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
        return self._finalize_runtime_receipt(
            receipt,
            intent_id=normalized_intent_id,
        )

    def recover_transfer_state(
        self,
        *,
        intent_id: str | None = None,
        transfer_id: str | None = None,
    ) -> dict[str, object]:
        self._require_open()
        if intent_id is None and transfer_id is None:
            raise ValueError("intent_id or transfer_id is required")
        if intent_id is not None and str(intent_id) not in self._submitted_intent_ids:
            raise ValueError(
                "runtime session can only recover intents submitted through it"
            )
        recover = getattr(self.daemon_client, "recover_transfer_state", None)
        if not callable(recover):
            raise TypeError("daemon client must support recover_transfer_state")
        response = recover(intent_id=intent_id, transfer_id=transfer_id)
        require_ok(response, "daemon transfer recovery failed")
        recovery = response.payload.get("transfer_recovery")
        if not isinstance(recovery, Mapping):
            raise ValueError("daemon response missing transfer_recovery")
        receipt_payload = recovery.get("receipt")
        if isinstance(receipt_payload, Mapping):
            receipt = _transfer_receipt_from_payload(receipt_payload)
            if receipt.intent_id in self._submitted_intent_ids:
                self._finalize_runtime_receipt(
                    receipt,
                    intent_id=receipt.intent_id,
                )
        record_runtime_transfer_recovery(
            self._runtime_entrypoint_record,
            intent_id=intent_id,
            transfer_id=transfer_id,
            recovery=recovery,
        )
        return dict(recovery)

    def bootstrap_profile(self, *, force: bool = False):
        self._require_open()
        self.open_session()
        relays = self._relay_gpus_for_session()
        if self._profile_bootstrapped and not force:
            return DaemonResponse(
                ok=True,
                payload={
                    "bootstrapped": True,
                    "already_bootstrapped": True,
                    "profile_bootstrap": self.profile_bootstrap_snapshot(),
                },
            )
        return self._bootstrap_daemon_profile(relays, force=force)

    def close(self) -> DaemonResponse:
        if self._closed:
            return DaemonResponse(
                ok=True,
                payload={"closed": False, "already_closed": True},
            )
        managed_runtime_before_shutdown = self.managed_service_snapshot()
        buffer_lifecycle_before_shutdown = self.buffer_lifecycle_snapshot()
        if self._session_id is None:
            return self._close_without_daemon_session(
                buffer_lifecycle_before_shutdown=buffer_lifecycle_before_shutdown,
                managed_runtime_before_shutdown=managed_runtime_before_shutdown,
            )
        intent_wait_evidence = self._close_active_intent_receipts()
        active_intents_after_wait = set(self._active_intent_ids)
        cleanup_evidence, cleanup_errors = self._cleanup_registered_buffers_for_close()
        try:
            response = self._runtime_daemon_client().close_session(self._session_id)
        except Exception as exc:
            response = DaemonResponse(ok=False, error=str(exc))
        intent_recovery_evidence = self._recover_close_active_intent_receipts(
            active_intents_after_wait
        )
        buffer_lifecycle_after_cleanup = self.buffer_lifecycle_snapshot()
        local_cpu_cleanup = self._cleanup_local_cpu_buffers(
            reason="runtime_session_close"
        )
        direct_runtime_cache_evidence = self._close_direct_runtime_cache()
        runtime_control_evidence = self._close_runtime_control_connection()
        managed_service_evidence = self._stop_owned_services()
        managed_runtime_after_shutdown = self.managed_service_snapshot()
        clear_runtime_session_state(self)
        self._closed = True
        payload = self._close_session_payload(
            response=response,
            buffer_lifecycle_before_shutdown=buffer_lifecycle_before_shutdown,
            buffer_lifecycle_after_cleanup=buffer_lifecycle_after_cleanup,
            intent_wait_evidence=intent_wait_evidence,
            intent_recovery_evidence=intent_recovery_evidence,
            cleanup_evidence=cleanup_evidence,
            local_cpu_cleanup=local_cpu_cleanup,
            direct_runtime_cache_evidence=direct_runtime_cache_evidence,
            managed_runtime_before_shutdown=managed_runtime_before_shutdown,
            managed_runtime_after_shutdown=managed_runtime_after_shutdown,
            runtime_control_evidence=runtime_control_evidence,
            managed_service_evidence=managed_service_evidence,
        )
        if cleanup_errors:
            payload["buffer_cleanup_errors"] = cleanup_errors
        record_runtime_close_recovery(
            self._runtime_entrypoint_record,
            intent_wait_evidence=intent_wait_evidence,
            intent_recovery_evidence=intent_recovery_evidence,
            cleanup_evidence=cleanup_evidence,
            cleanup_errors=cleanup_errors,
            local_cpu_cleanup=local_cpu_cleanup,
            direct_runtime_cache_evidence=direct_runtime_cache_evidence,
            managed_runtime_before_shutdown=managed_runtime_before_shutdown,
            managed_runtime_after_shutdown=managed_runtime_after_shutdown,
            runtime_control_evidence=runtime_control_evidence,
            managed_service_evidence=managed_service_evidence,
        )
        if cleanup_errors:
            record_runtime_session_close(
                self._runtime_entrypoint_record,
                response_ok=response.ok,
                response_error=response.error,
                payload=payload,
            )
            payload["runtime_entrypoint"] = _copy_lifecycle_mapping(
                self._runtime_entrypoint_record
            )
            return DaemonResponse(
                ok=response.ok,
                error=response.error,
                payload=payload,
            )
        if payload != response.payload:
            record_runtime_session_close(
                self._runtime_entrypoint_record,
                response_ok=response.ok,
                response_error=response.error,
                payload=payload,
            )
            payload["runtime_entrypoint"] = _copy_lifecycle_mapping(
                self._runtime_entrypoint_record
            )
            return DaemonResponse(ok=response.ok, error=response.error, payload=payload)
        record_runtime_session_close(
            self._runtime_entrypoint_record,
            response_ok=response.ok,
            response_error=response.error,
            payload=payload,
        )
        if isinstance(response.payload, dict):
            response.payload["runtime_entrypoint"] = _copy_lifecycle_mapping(
                self._runtime_entrypoint_record
            )
        return response

    def _close_without_daemon_session(
        self,
        *,
        buffer_lifecycle_before_shutdown: dict[str, object],
        managed_runtime_before_shutdown: dict[str, object] | None,
    ) -> DaemonResponse:
        local_cpu_cleanup = self._cleanup_local_cpu_buffers(
            reason="runtime_session_close_without_daemon_session"
        )
        direct_runtime_cache_evidence = self._close_direct_runtime_cache()
        runtime_control_evidence = self._close_runtime_control_connection()
        managed_service_evidence = self._stop_owned_services()
        managed_runtime_after_shutdown = self.managed_service_snapshot()
        clear_runtime_session_state(self)
        self._closed = True
        payload = self._close_without_session_payload(
            buffer_lifecycle_before_shutdown=buffer_lifecycle_before_shutdown,
            local_cpu_cleanup=local_cpu_cleanup,
            direct_runtime_cache_evidence=direct_runtime_cache_evidence,
            managed_runtime_before_shutdown=managed_runtime_before_shutdown,
            managed_runtime_after_shutdown=managed_runtime_after_shutdown,
            runtime_control_evidence=runtime_control_evidence,
            managed_service_evidence=managed_service_evidence,
        )
        record_runtime_close_recovery(
            self._runtime_entrypoint_record,
            intent_wait_evidence=(),
            intent_recovery_evidence=(),
            cleanup_evidence=(),
            cleanup_errors=(),
            local_cpu_cleanup=local_cpu_cleanup,
            direct_runtime_cache_evidence=direct_runtime_cache_evidence,
            managed_runtime_before_shutdown=managed_runtime_before_shutdown,
            managed_runtime_after_shutdown=managed_runtime_after_shutdown,
            runtime_control_evidence=runtime_control_evidence,
            managed_service_evidence=managed_service_evidence,
        )
        record_runtime_session_close(
            self._runtime_entrypoint_record,
            response_ok=True,
            response_error=None,
            payload=payload,
        )
        payload["runtime_entrypoint"] = _copy_lifecycle_mapping(
            self._runtime_entrypoint_record
        )
        return DaemonResponse(ok=True, payload=payload)

    def _close_without_session_payload(
        self,
        *,
        buffer_lifecycle_before_shutdown: dict[str, object],
        local_cpu_cleanup: Sequence[Mapping[str, object]],
        direct_runtime_cache_evidence: Mapping[str, object] | None,
        managed_runtime_before_shutdown: dict[str, object] | None,
        managed_runtime_after_shutdown: dict[str, object] | None,
        runtime_control_evidence: Mapping[str, object] | None,
        managed_service_evidence: Sequence[Mapping[str, object]],
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "closed": False,
            "managed_service_shutdown": list(managed_service_evidence),
            "buffer_lifecycle": buffer_lifecycle_before_shutdown,
        }
        self._add_local_cpu_cleanup_payload(payload, local_cpu_cleanup)
        self._add_direct_runtime_cache_payload(payload, direct_runtime_cache_evidence)
        self._add_managed_runtime_close_payload(
            payload,
            managed_runtime_before_shutdown=managed_runtime_before_shutdown,
            managed_runtime_after_shutdown=managed_runtime_after_shutdown,
            runtime_control_evidence=runtime_control_evidence,
        )
        return payload

    def _cleanup_registered_buffers_for_close(
        self,
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        cleanup_errors: list[dict[str, object]] = []
        cleanup_evidence: list[dict[str, object]] = []
        for buffer_id in tuple(self._registered_buffer_ids):
            try:
                cleanup_response = self.cleanup_buffer(
                    buffer_id,
                    reason="runtime_session_close",
                    force=True,
                )
                cleanup_evidence.append(
                    self._buffer_close_cleanup_record(
                        buffer_id,
                        cleanup_response,
                    )
                )
            except Exception as exc:
                cleanup_errors.append({"buffer_id": buffer_id, "error": str(exc)})
        return cleanup_evidence, cleanup_errors

    def _buffer_close_cleanup_record(
        self,
        buffer_id: str,
        cleanup_response: DaemonResponse,
    ) -> dict[str, object]:
        cleanup_record: dict[str, object] = {
            "buffer_id": buffer_id,
            "ok": bool(cleanup_response.ok),
        }
        if cleanup_response.error:
            cleanup_record["error"] = cleanup_response.error
        if isinstance(cleanup_response.payload, Mapping):
            cleanup_record["payload"] = dict(cleanup_response.payload)
        return cleanup_record

    def _close_session_payload(
        self,
        *,
        response: DaemonResponse,
        buffer_lifecycle_before_shutdown: dict[str, object],
        buffer_lifecycle_after_cleanup: dict[str, object],
        intent_wait_evidence: Sequence[Mapping[str, object]],
        intent_recovery_evidence: Sequence[Mapping[str, object]],
        cleanup_evidence: Sequence[Mapping[str, object]],
        local_cpu_cleanup: Sequence[Mapping[str, object]],
        direct_runtime_cache_evidence: Mapping[str, object] | None,
        managed_runtime_before_shutdown: dict[str, object] | None,
        managed_runtime_after_shutdown: dict[str, object] | None,
        runtime_control_evidence: Mapping[str, object] | None,
        managed_service_evidence: Sequence[Mapping[str, object]],
    ) -> dict[str, object]:
        payload = {}
        if isinstance(response.payload, Mapping):
            payload.update(dict(response.payload))
        payload["buffer_lifecycle"] = buffer_lifecycle_before_shutdown
        payload["buffer_lifecycle_after_cleanup"] = buffer_lifecycle_after_cleanup
        if intent_wait_evidence:
            payload["active_intent_receipts"] = list(intent_wait_evidence)
        if intent_recovery_evidence:
            payload["active_intent_recovery"] = list(intent_recovery_evidence)
        if cleanup_evidence:
            payload["buffer_cleanup_evidence"] = list(cleanup_evidence)
        self._add_local_cpu_cleanup_payload(payload, local_cpu_cleanup)
        self._add_direct_runtime_cache_payload(payload, direct_runtime_cache_evidence)
        self._add_managed_runtime_close_payload(
            payload,
            managed_runtime_before_shutdown=managed_runtime_before_shutdown,
            managed_runtime_after_shutdown=managed_runtime_after_shutdown,
            runtime_control_evidence=runtime_control_evidence,
        )
        if managed_service_evidence:
            payload["managed_service_shutdown"] = list(managed_service_evidence)
        return payload

    def _add_local_cpu_cleanup_payload(
        self,
        payload: dict[str, object],
        local_cpu_cleanup: Sequence[Mapping[str, object]],
    ) -> None:
        if not local_cpu_cleanup:
            return
        cleanup_records = [dict(item) for item in local_cpu_cleanup]
        payload["local_cpu_buffer_cleanup"] = cleanup_records
        owned_release = _owned_cpu_release_records(cleanup_records)
        if owned_release:
            payload["owned_cpu_buffer_release"] = owned_release

    def _add_direct_runtime_cache_payload(
        self,
        payload: dict[str, object],
        direct_runtime_cache_evidence: Mapping[str, object] | None,
    ) -> None:
        if direct_runtime_cache_evidence is None:
            return
        payload["direct_runtime_cache_shutdown"] = dict(
            direct_runtime_cache_evidence
        )

    def _add_managed_runtime_close_payload(
        self,
        payload: dict[str, object],
        *,
        managed_runtime_before_shutdown: dict[str, object] | None,
        managed_runtime_after_shutdown: dict[str, object] | None,
        runtime_control_evidence: Mapping[str, object] | None,
    ) -> None:
        if self._managed_service_startup_evidence is not None:
            payload["managed_service_startup"] = dict(
                self._managed_service_startup_evidence
            )
        if managed_runtime_before_shutdown is not None:
            payload["managed_service_runtime_before_shutdown"] = (
                managed_runtime_before_shutdown
            )
        if managed_runtime_after_shutdown is not None:
            payload["managed_service_runtime_after_shutdown"] = (
                managed_runtime_after_shutdown
            )
        if runtime_control_evidence is not None:
            payload["runtime_control_shutdown"] = dict(runtime_control_evidence)

    def __enter__(self) -> "TurboBusRuntimeSession":
        self._require_open()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def managed_service_snapshot(self) -> dict[str, object] | None:
        return managed_service_runtime_snapshot(
            startup_records=self._managed_service_records,
            startup_lock=self._managed_service_lock,
            daemon_thread=self._owned_daemon_thread,
            daemon_stop_event=self._owned_daemon_stop_event,
            daemon_socket_path=self._owned_daemon_socket_path,
            worker_thread=self._owned_worker_thread,
            worker_stop_event=self._owned_worker_stop_event,
            worker_socket_path=self._owned_worker_socket_path,
            runtime_control_owned=self._runtime_control_connection_owned,
            runtime_client=self.runtime_daemon_client,
        )

    def profile_bootstrap_snapshot(self) -> dict[str, object]:
        evidence = (
            {}
            if self._profile_bootstrap_evidence is None
            else dict(self._profile_bootstrap_evidence)
        )
        evidence.setdefault("bootstrapped", bool(self._profile_bootstrapped))
        evidence.setdefault(
            "profile_on_first_transfer",
            bool(self.runtime_options.profile_on_first_transfer),
        )
        if self._target_gpu is not None:
            evidence.setdefault("target_gpu", int(self._target_gpu))
        if self._relay_gpus is not None:
            evidence.setdefault(
                "relay_gpus",
                [int(gpu) for gpu in self._relay_gpus],
            )
        return evidence

    def runtime_telemetry_snapshot(self) -> dict[str, object]:
        self._require_open()
        self._ensure_managed_services_alive("runtime_telemetry_snapshot")
        telemetry = getattr(self._runtime_daemon_client(), "runtime_telemetry", None)
        if not callable(telemetry):
            raise TypeError("daemon runtime client must support runtime_telemetry")
        response = telemetry()
        require_ok(response, "daemon runtime telemetry query failed")
        snapshot = response.payload.get("runtime_telemetry")
        if not isinstance(snapshot, Mapping):
            raise ValueError("daemon response missing runtime_telemetry")
        return dict(snapshot)

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
        resolved_policy_hints = runtime_policy_hints_without_physical_routes(
            policy_hints,
        )
        resolved_policy_hints["chunk_bytes"] = resolved_chunk_bytes
        resolved_metadata = runtime_metadata_without_physical_routes(metadata)
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
            metadata=resolved_metadata,
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
        context_id: str | None = None
        try:
            gpu_buffer = self.register_cuda_buffer(gpu_buffer)
            session_id = self.open_session()
            cpu_buffer = self.register_cpu_buffer(cpu_buffer)
            resolved_policy_hints = runtime_policy_hints_without_physical_routes(
                policy_hints,
            )
            if "chunk_bytes" not in resolved_policy_hints:
                resolved_policy_hints["chunk_bytes"] = int(
                    getattr(self.runtime_options, "chunk_bytes", 16 * 1024 * 1024)
                )
            resolved_metadata = runtime_metadata_without_physical_routes(metadata)
            context = AdapterTransferContext(
                job_id=self.job_id,
                session_id=session_id,
                cpu_buffer_id=cpu_buffer.buffer_id,
                gpu_buffer_id=gpu_buffer.buffer_id,
                cpu_buffer=cpu_buffer,
                gpu_buffer=gpu_buffer,
                workload_kind=workload_kind,
                priority=priority,
                policy_hints=resolved_policy_hints,
                metadata=resolved_metadata,
                intent_prefix=intent_prefix,
                wait_timeout_seconds=wait_timeout_seconds,
            )
            context_id = _adapter_transfer_context_id(context)
            self.record_adapter_transfer_context(
                context_id=context_id,
                workload_kind=str(context.workload_kind.value),
                cpu_buffer_id=context.cpu_buffer_id,
                gpu_buffer_id=context.gpu_buffer_id,
                intent_prefix=context.intent_prefix,
                priority=context.priority,
                policy_hints=context.policy_hints,
                metadata=context.metadata,
            )
            return context
        except Exception as exc:
            if context_id is None:
                context_id = _adapter_transfer_context_id_from_parts(
                    session_id=session_id or self._session_id or "unopened",
                    cpu_buffer_id=cpu_buffer_id or "unknown-cpu-buffer",
                    gpu_buffer_id=gpu_buffer_id or "unknown-gpu-buffer",
                    intent_prefix=intent_prefix,
                    workload_kind=workload_kind,
                )
            try:
                self.record_adapter_transfer_context(
                    context_id=context_id,
                    workload_kind=str(WorkloadKind(workload_kind).value),
                    cpu_buffer_id=cpu_buffer_id or "unknown-cpu-buffer",
                    gpu_buffer_id=gpu_buffer_id or "unknown-gpu-buffer",
                    intent_prefix=(
                        str(intent_prefix)
                        if intent_prefix is not None
                        else str(context_id)
                    ),
                    priority=int(priority),
                    policy_hints=(
                        runtime_policy_hints_without_physical_routes(policy_hints)
                        if policy_hints is not None
                        else {}
                    ),
                    metadata=(
                        runtime_metadata_without_physical_routes(metadata)
                        if metadata is not None
                        else {}
                    ),
                    state="failed",
                    error=str(exc) or exc.__class__.__name__,
                )
            except Exception:
                pass
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
                self._profile_bootstrap_evidence = None
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
        policy_hints: Mapping[str, object] | None = None,
        metadata: Mapping[str, object] | None = None,
        intent_prefix: str | None = None,
        wait_timeout_seconds: float | None = None,
        manifest=None,
    ):
        from .adapters.model_loading import ModelWeightLoader

        context = self.make_adapter_transfer_context(
            cpu_buffer,
            gpu_buffer,
            workload_kind=WorkloadKind.MODEL_WEIGHTS,
            priority=priority,
            policy_hints=policy_hints,
            metadata=metadata,
            intent_prefix=intent_prefix,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        return ModelWeightLoader._from_transfer_context(
            self,
            context,
            cpu_buffer,
            gpu_buffer,
            manifest=manifest,
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

    def _submit_runtime_intent(
        self,
        intent: TransferIntent,
        *,
        wait: bool = True,
        timeout_seconds: float | None = None,
    ) -> TransferReceipt:
        self._prepare_runtime_intent(intent)
        self._record_submitted_intent(intent)
        try:
            receipt = self._execute_intent_through_daemon(intent)
            if wait:
                return self.wait_transfer_receipt(
                    intent.intent_id,
                    timeout_seconds=timeout_seconds,
                )
            return self._finalize_runtime_receipt(
                receipt,
                intent_id=intent.intent_id,
            )
        except Exception:
            self._active_intent_ids.discard(intent.intent_id)
            raise

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
        require_intent_control_plane_safe(intent)
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
        receipt = self._make_worker_intent_transfer_executor().execute_transfer_intent(
            intent,
            response,
            execution_view,
        )
        record_runtime_daemon_execution(
            self._runtime_entrypoint_record,
            intent,
            receipt=receipt,
        )
        return receipt

    def _make_worker_intent_transfer_executor(self) -> WorkerIntentTransferExecutor:
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
                self._record_buffer_lifecycle_registration(
                    buffer,
                    runtime_owned=runtime_owned,
                )
                record_runtime_buffer_registered(
                    self._runtime_entrypoint_record,
                    buffer_id=buffer_id,
                    registration={
                        "metadata": metadata,
                        "runtime_owned": runtime_owned,
                        "fingerprint": fingerprint,
                    },
                )
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
        local_cleanup_error: Exception | None = None
        local_cpu_cleanup: dict[str, object] | None = None
        retention_error: Exception | None = None
        retention_evidence: dict[str, object] | None = None
        runtime_owned = normalized_id in self._owned_cpu_buffer_ids
        buffer_was_registered = normalized_id in self._registered_buffer_ids
        if buffer_was_registered:
            try:
                response = self._cleanup_daemon_registered_buffer(
                    normalized_id,
                    reason=reason,
                    force=force,
                )
            except Exception as exc:
                cleanup_error = exc
        self._forget_runtime_buffer(normalized_id)
        local_cpu_cleanup, local_cleanup_error = self._cleanup_local_buffer_for_release(
            normalized_id,
            buffer,
            reason=reason,
            runtime_owned=runtime_owned,
        )
        if cleanup_error is None and buffer_was_registered:
            retention_evidence, retention_error = self._retain_runtime_buffer_cleanup(
                normalized_id,
                buffer,
                reason=reason,
                runtime_owned=runtime_owned,
                local_cpu_cleanup=local_cpu_cleanup,
            )
        self._raise_buffer_cleanup_error_if_needed(
            normalized_id,
            reason=reason,
            daemon_response=response,
            cleanup_error=cleanup_error,
            local_cleanup_error=local_cleanup_error,
            retention_error=retention_error,
            local_cpu_cleanup=local_cpu_cleanup,
            retention_evidence=retention_evidence,
        )
        self._record_buffer_lifecycle_cleanup(
            normalized_id,
            reason=reason,
            ok=True,
            daemon_response=response,
            local_cpu_cleanup=local_cpu_cleanup,
            retention_evidence=retention_evidence,
        )
        record_runtime_buffer_cleanup(
            self._runtime_entrypoint_record,
            buffer_id=normalized_id,
            cleanup_record={
                "reason": reason,
                "ok": True,
                "daemon_ok": bool(response.ok),
                "local_cpu_cleanup": (
                    None if local_cpu_cleanup is None else dict(local_cpu_cleanup)
                ),
                "retention_evidence": (
                    None if retention_evidence is None else dict(retention_evidence)
                ),
            },
        )
        payload = self._cleanup_buffer_response_payload(
            response,
            local_cpu_cleanup=local_cpu_cleanup,
            retention_evidence=retention_evidence,
        )
        return DaemonResponse(ok=response.ok, error=response.error, payload=payload)

    def _cleanup_daemon_registered_buffer(
        self,
        buffer_id: str,
        *,
        reason: str,
        force: bool,
    ) -> DaemonResponse:
        response = self._execution_daemon_client().cleanup(
            target_kind="buffer",
            target_id=buffer_id,
            reason=reason,
            force=force,
        )
        require_ok(response, "daemon buffer cleanup failed")
        return response

    def _forget_runtime_buffer(self, buffer_id: str) -> None:
        self._registered_buffer_ids.discard(buffer_id)
        self._registered_buffer_fingerprints.pop(buffer_id, None)
        self._buffers.pop(buffer_id, None)
        self._owned_cpu_buffer_ids.discard(buffer_id)

    def _cleanup_local_buffer_for_release(
        self,
        buffer_id: str,
        buffer: ExecutableBuffer | None,
        *,
        reason: str,
        runtime_owned: bool,
    ) -> tuple[dict[str, object] | None, Exception | None]:
        if not isinstance(buffer, SharedPinnedCpuBuffer):
            return None, None
        local_cpu_cleanup = self._cleanup_local_cpu_buffer(
            buffer_id,
            buffer,
            reason=reason,
            runtime_owned=runtime_owned,
        )
        if bool(local_cpu_cleanup.get("ok", False)):
            return local_cpu_cleanup, None
        return local_cpu_cleanup, RuntimeError(
            str(local_cpu_cleanup.get("error") or "local CPU buffer cleanup failed")
        )

    def _retain_runtime_buffer_cleanup(
        self,
        buffer_id: str,
        buffer: ExecutableBuffer | None,
        *,
        reason: str,
        runtime_owned: bool,
        local_cpu_cleanup: Mapping[str, object] | None,
    ) -> tuple[dict[str, object], Exception | None]:
        retention_payload = _runtime_buffer_retention_evidence(
            buffer_id=buffer_id,
            buffer=buffer,
            reason=reason,
            runtime_owned=runtime_owned,
            local_cpu_cleanup=local_cpu_cleanup,
            lifecycle_record=self._buffer_lifecycle_records.get(buffer_id),
        )
        retention_evidence = self._record_buffer_cleanup_retention(
            buffer_id,
            retention_payload,
        )
        if bool(retention_evidence.get("ok", False)):
            return retention_evidence, None
        return retention_evidence, RuntimeError(
            str(
                retention_evidence.get("error")
                or "daemon buffer retention update failed"
            )
        )

    def _raise_buffer_cleanup_error_if_needed(
        self,
        buffer_id: str,
        *,
        reason: str,
        daemon_response: DaemonResponse,
        cleanup_error: Exception | None,
        local_cleanup_error: Exception | None,
        retention_error: Exception | None,
        local_cpu_cleanup: Mapping[str, object] | None,
        retention_evidence: Mapping[str, object] | None,
    ) -> None:
        error = cleanup_error or local_cleanup_error or retention_error
        if error is None:
            return
        self._record_buffer_lifecycle_cleanup(
            buffer_id,
            reason=reason,
            ok=False,
            daemon_response=daemon_response,
            local_cpu_cleanup=local_cpu_cleanup,
            retention_evidence=retention_evidence,
            error=error,
        )
        record_runtime_buffer_cleanup(
            self._runtime_entrypoint_record,
            buffer_id=buffer_id,
            cleanup_record={
                "reason": reason,
                "ok": False,
                "daemon_ok": bool(daemon_response.ok),
                "local_cpu_cleanup": (
                    None if local_cpu_cleanup is None else dict(local_cpu_cleanup)
                ),
                "retention_evidence": (
                    None if retention_evidence is None else dict(retention_evidence)
                ),
                "error": str(error) or error.__class__.__name__,
            },
        )
        if cleanup_error is not None and local_cleanup_error is not None:
            raise RuntimeError(
                f"{cleanup_error}; local CPU buffer cleanup failed: {local_cleanup_error}"
            ) from cleanup_error
        raise error

    def _cleanup_buffer_response_payload(
        self,
        response: DaemonResponse,
        *,
        local_cpu_cleanup: Mapping[str, object] | None,
        retention_evidence: Mapping[str, object] | None,
    ) -> dict[str, object]:
        payload = dict(response.payload) if isinstance(response.payload, Mapping) else {}
        if local_cpu_cleanup is not None:
            payload["local_cpu_buffer_cleanup"] = dict(local_cpu_cleanup)
            if bool(local_cpu_cleanup.get("runtime_owned", False)):
                payload["owned_cpu_buffer_release"] = dict(local_cpu_cleanup)
        if retention_evidence is not None:
            payload["runtime_buffer_retention"] = dict(retention_evidence)
        return payload

    def _record_buffer_cleanup_retention(
        self,
        buffer_id: str,
        retention_evidence: Mapping[str, object],
    ) -> dict[str, object]:
        retention_record = {
            "buffer_id": str(buffer_id),
            "reason": str(
                retention_evidence.get("reason") or "runtime_buffer_released"
            ),
            "ok": False,
        }
        try:
            response = self._execution_daemon_client().cleanup(
                target_kind="buffer",
                target_id=str(buffer_id),
                reason=retention_record["reason"],
                force=False,
                retention_evidence=dict(retention_evidence),
            )
            require_ok(response, "daemon buffer retention update failed")
        except Exception as exc:
            retention_record["error"] = str(exc) or exc.__class__.__name__
            return retention_record
        retention_record["ok"] = True
        if isinstance(response.payload, Mapping):
            retention_record["payload"] = dict(response.payload)
        return retention_record

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
        runtime_owned = normalized_id in self._owned_cpu_buffer_ids
        if normalized_id in self._owned_cpu_buffer_ids:
            self._owned_cpu_buffer_ids.discard(normalized_id)
        if isinstance(buffer, SharedPinnedCpuBuffer):
            self._cleanup_local_cpu_buffer(
                normalized_id,
                buffer,
                reason="runtime_adapter_context_creation_failed",
                runtime_owned=runtime_owned,
            )

    def _cleanup_local_cpu_buffers(self, *, reason: str) -> list[dict[str, object]]:
        evidence: list[dict[str, object]] = []
        for buffer_id, buffer in tuple(self._buffers.items()):
            if not isinstance(buffer, SharedPinnedCpuBuffer):
                continue
            runtime_owned = buffer_id in self._owned_cpu_buffer_ids
            if runtime_owned:
                self._owned_cpu_buffer_ids.discard(buffer_id)
            buffer = self._buffers.get(buffer_id)
            if not isinstance(buffer, SharedPinnedCpuBuffer):
                continue
            evidence.append(
                self._cleanup_local_cpu_buffer(
                    buffer_id,
                    buffer,
                    reason=reason,
                    runtime_owned=runtime_owned,
                )
            )
        return evidence

    def _cleanup_local_cpu_buffer(
        self,
        buffer_id: str,
        buffer: SharedPinnedCpuBuffer,
        *,
        reason: str,
        runtime_owned: bool,
    ) -> dict[str, object]:
        mode = "release" if bool(runtime_owned) else "close"
        evidence = {
            "buffer_id": str(buffer_id),
            "job_id": buffer.job_id,
            "reason": str(reason),
            "runtime_owned": bool(runtime_owned),
            "owner": bool(buffer.owner),
            "mode": mode,
            "shared_memory_name": buffer.shared_memory_name,
            "closed_before_release": buffer.closed,
            "unlinked_before_release": bool(getattr(buffer, "_unlinked", False)),
            "ok": False,
        }
        try:
            if bool(runtime_owned):
                buffer.release()
            else:
                buffer.close()
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
        self._ensure_managed_services_alive("runtime daemon client use")
        if self.runtime_daemon_client is None:
            raise RuntimeError("runtime daemon client is not configured")
        return self.runtime_daemon_client

    def _execution_daemon_client(self):
        self._require_open()
        self._ensure_managed_services_alive("execution daemon client use")
        if self.execution_daemon_client is None:
            raise RuntimeError("execution daemon client is not configured")
        return self.execution_daemon_client

    def _profile_daemon_client(self):
        self._require_open()
        self._ensure_managed_services_alive("profile daemon client use")
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
        profile_enabled = bool(self.runtime_options.profile_on_first_transfer)
        if not profile_enabled:
            self._profile_bootstrap_evidence = {
                "bootstrapped": False,
                "profile_on_first_transfer": False,
                "source": "disabled",
            }
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
        evidence = response.payload.get("profile_bootstrap")
        if isinstance(evidence, Mapping):
            snapshot = dict(evidence)
        else:
            snapshot = {
                "source": "unknown",
                "target_gpu": int(self._target_gpu),
                "relay_gpus": [int(gpu) for gpu in relay_gpus],
            }
        snapshot["bootstrapped"] = True
        snapshot["profile_on_first_transfer"] = bool(
            self.runtime_options.profile_on_first_transfer
        )
        self._profile_bootstrap_evidence = snapshot
        return response

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("TurboBus runtime session is closed")

    def _stop_owned_services(self) -> list[dict[str, object]]:
        evidence: list[dict[str, object]] = []
        worker_stop_event = self._owned_worker_stop_event
        daemon_stop_event = self._owned_daemon_stop_event
        worker_thread = self._owned_worker_thread
        daemon_thread = self._owned_daemon_thread
        if worker_stop_event is not None:
            worker_stop_event.set()
            self._update_managed_service_record(
                "worker",
                state="shutdown_requested",
                shutdown_requested=True,
                shutdown_requested_at=time.time(),
            )
        if daemon_stop_event is not None:
            daemon_stop_event.set()
            self._update_managed_service_record(
                "daemon",
                state="shutdown_requested",
                shutdown_requested=True,
                shutdown_requested_at=time.time(),
            )
        if worker_thread is not None:
            worker_thread.join(timeout=1.0)
            service_evidence = {
                "service": "worker",
                "socket_path": self._owned_worker_socket_path,
                "alive_after_join": worker_thread.is_alive(),
            }
            evidence.append(service_evidence)
            self._update_managed_service_record(
                "worker",
                state=(
                    "shutdown_timeout"
                    if worker_thread.is_alive()
                    else "shutdown_complete"
                ),
                shutdown_complete=not worker_thread.is_alive(),
                shutdown_checked_at=time.time(),
                socket_path=self._owned_worker_socket_path,
                alive_after_join=worker_thread.is_alive(),
            )
        if daemon_thread is not None:
            daemon_thread.join(timeout=1.0)
            service_evidence = {
                "service": "daemon",
                "socket_path": self._owned_daemon_socket_path,
                "alive_after_join": daemon_thread.is_alive(),
            }
            evidence.append(service_evidence)
            self._update_managed_service_record(
                "daemon",
                state=(
                    "shutdown_timeout"
                    if daemon_thread.is_alive()
                    else "shutdown_complete"
                ),
                shutdown_complete=not daemon_thread.is_alive(),
                shutdown_checked_at=time.time(),
                socket_path=self._owned_daemon_socket_path,
                alive_after_join=daemon_thread.is_alive(),
            )
        self._owned_worker_stop_event = None
        self._owned_worker_thread = None
        self._owned_daemon_stop_event = None
        self._owned_daemon_thread = None
        self._owned_daemon_socket_path = None
        self._owned_worker_socket_path = None
        return evidence

    def _close_runtime_control_connection(self) -> dict[str, object] | None:
        if not bool(self._runtime_control_connection_owned):
            return None
        client = self.runtime_daemon_client
        close = getattr(client, "close", None)
        closed_before = bool(getattr(client, "closed", False))
        evidence = {
            "owned": True,
            "client_type": (
                None if client is None else client.__class__.__name__
            ),
            "closed_before_shutdown": closed_before,
            "ok": False,
        }
        try:
            if callable(close):
                close()
        except Exception as exc:
            evidence["error"] = str(exc) or exc.__class__.__name__
            evidence["closed_after_shutdown"] = bool(getattr(client, "closed", False))
            self._runtime_control_connection_owned = False
            return evidence
        evidence["ok"] = True
        evidence["closed_after_shutdown"] = bool(getattr(client, "closed", False))
        self._runtime_control_connection_owned = False
        return evidence

    def _close_direct_runtime_cache(self) -> dict[str, object] | None:
        executor = self._transfer_executor
        if executor is None:
            return None
        closer = getattr(executor, "close_direct_runtime_cache", None)
        if not callable(closer):
            return None
        try:
            return dict(closer())
        except Exception as exc:
            return {
                "source": "runtime_session_direct_backend_cache",
                "closed": False,
                "error": str(exc) or exc.__class__.__name__,
                "error_type": exc.__class__.__name__,
            }

    def _ensure_managed_services_alive(self, phase: str) -> None:
        snapshot = self.managed_service_snapshot()
        if snapshot is None:
            return
        failures: list[str] = []
        services = snapshot.get("services")
        if not isinstance(services, Mapping):
            services = {}
        for service, value in services.items():
            if not isinstance(value, Mapping):
                continue
            owned = bool(value.get("owned", False))
            stop_requested = bool(value.get("stop_requested", False))
            state = str(value.get("state", "")).lower()
            thread_alive = bool(value.get("thread_alive", False))
            if state in {"failed", "stopped"} and not stop_requested:
                failures.append(f"{service}:{state}")
            if owned and not thread_alive and not stop_requested:
                failures.append(f"{service}:thread_dead")
            socket_path = value.get("socket_path")
            socket_exists = value.get("socket_exists")
            if (
                socket_path is not None
                and socket_exists is False
                and not stop_requested
            ):
                failures.append(f"{service}:socket_missing")
        runtime_control = snapshot.get("runtime_control")
        if isinstance(runtime_control, Mapping):
            if bool(runtime_control.get("owned", False)) and bool(
                runtime_control.get("closed", False)
            ):
                failures.append("runtime_control:closed")
        if not failures:
            return
        raise ManagedProductionStartupError(
            "managed production services are unavailable during "
            f"{phase}: {', '.join(failures)}",
            evidence={"managed_runtime": snapshot},
        )

    def _update_managed_service_record(
        self,
        service: str,
        **updates,
    ) -> None:
        if self._managed_service_records is None or self._managed_service_lock is None:
            return
        update_managed_service_startup_record(
            self._managed_service_records,
            self._managed_service_lock,
            service,
            **updates,
        )

    def _validate_intent_uses_runtime_buffers(self, intent: TransferIntent) -> None:
        source = self._buffers[intent.source_buffer_id]
        target = self._buffers[intent.destination_buffer_id]
        validate_intent_uses_runtime_buffers(
            intent,
            source=source,
            target=target,
        )

    def _record_submitted_intent(self, intent: TransferIntent) -> None:
        normalized_intent_id = str(intent.intent_id)
        self._submitted_intent_ids.add(normalized_intent_id)
        self._submitted_intent_buffers[normalized_intent_id] = (
            str(intent.source_buffer_id),
            str(intent.destination_buffer_id),
        )
        self._active_intent_ids.add(normalized_intent_id)
        self._record_buffer_lifecycle_intent_use(intent)
        record_runtime_intent_submitted(
            self._runtime_entrypoint_record,
            intent,
        )

    def _finalize_runtime_receipt(
        self,
        receipt: TransferReceipt,
        *,
        intent_id: str,
    ) -> TransferReceipt:
        finalized = _finalize_runtime_receipt(
            receipt,
            intent_id=intent_id,
            job_id=self.job_id,
            session_id=self.session_id,
            submitted_intent_buffers=self._submitted_intent_buffers,
            active_intent_ids=self._active_intent_ids,
            record_buffer_lifecycle_receipt_fn=self._record_buffer_lifecycle_receipt,
        )
        record_runtime_receipt_finalized(
            self._runtime_entrypoint_record,
            finalized,
        )
        return finalized

    def _record_buffer_lifecycle_registration(
        self,
        buffer: ExecutableBuffer,
        *,
        runtime_owned: bool,
    ) -> None:
        _record_buffer_lifecycle_registration(
            self._buffer_lifecycle_records,
            buffer,
            session_id=self.session_id,
            runtime_owned=runtime_owned,
            registered_at=time.time(),
        )

    def _record_buffer_lifecycle_intent_use(self, intent: TransferIntent) -> None:
        _record_buffer_lifecycle_intent_use(
            self._buffer_lifecycle_records,
            intent,
            now=time.time(),
        )

    def _record_buffer_lifecycle_receipt(
        self,
        receipt: TransferReceipt,
        *,
        intent_id: str,
    ) -> None:
        _record_buffer_lifecycle_receipt(
            self._buffer_lifecycle_records,
            self._submitted_intent_buffers,
            receipt,
            intent_id=intent_id,
        )

    def _record_buffer_lifecycle_cleanup(
        self,
        buffer_id: str,
        *,
        reason: str,
        ok: bool,
        daemon_response: DaemonResponse,
        local_cpu_cleanup: Mapping[str, object] | None,
        retention_evidence: Mapping[str, object] | None,
        error: Exception | None = None,
    ) -> None:
        _record_buffer_lifecycle_cleanup(
            self._buffer_lifecycle_records,
            buffer_id,
            reason=reason,
            ok=ok,
            daemon_response=daemon_response,
            local_cpu_cleanup=local_cpu_cleanup,
            retention_evidence=retention_evidence,
            error=error,
        )

    def _close_active_intent_receipts(self) -> list[dict[str, object]]:
        return _close_active_intent_receipts(
            self._active_intent_ids,
            self.wait_transfer_receipt,
        )

    def _recover_close_active_intent_receipts(
        self,
        active_intent_ids: set[str],
    ) -> list[dict[str, object]]:
        if not active_intent_ids:
            return []
        return _recover_active_intent_receipts(
            active_intent_ids,
            self.recover_transfer_state,
        )


def _daemon_startup_config_with_runtime_planner_options(
    config: DaemonStartupConfig,
    options: RuntimeOptions,
) -> DaemonStartupConfig:
    return replace(
        config,
        min_pool_bytes=int(options.min_pool_bytes),
        min_chunks_for_relay=int(options.min_chunks_for_relay),
        relay_min_effective_bw_gbps=float(options.relay_min_effective_bw_gbps),
        relay_min_direct_ratio=float(options.relay_min_direct_ratio),
    )


def _adapter_transfer_context_id(context) -> str:
    return _adapter_transfer_context_id_from_parts(
        session_id=context.session_id,
        cpu_buffer_id=context.cpu_buffer_id,
        gpu_buffer_id=context.gpu_buffer_id,
        intent_prefix=context.intent_prefix,
        workload_kind=context.workload_kind,
    )


def _adapter_transfer_context_id_from_parts(
    *,
    session_id: str,
    cpu_buffer_id: str,
    gpu_buffer_id: str,
    intent_prefix: str | None,
    workload_kind: object,
) -> str:
    # /*
    #  * ========================================================================
    #  * 步骤1：生成适配器构造证据标识
    #  * ========================================================================
    #  * 数据源：RuntimeSession session/buffer/context 字段
    #  * 操作：
    #  *   1) 归一化构造上下文关键字段
    #  *   2) 生成稳定的 RuntimeSession entrypoint record key
    #  */
    logger.info("开始生成适配器构造证据标识...")

    # // 1.1 归一化 workload 与 intent 前缀
    workload = str(getattr(workload_kind, "value", workload_kind))
    prefix = str(intent_prefix or "adapter")

    # // 1.2 生成稳定 context_id
    context_id = (
        f"adapter-context-{session_id}-{workload}-{prefix}-"
        f"{cpu_buffer_id}-{gpu_buffer_id}"
    )
    logger.info("适配器构造证据标识生成完成, context_id: %s", context_id)
    return context_id




__all__ = ["ManagedProductionStartupError", "TurboBusRuntimeSession"]
