from __future__ import annotations

import os
import socket
import threading
import time
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
from .worker.process import run_worker_service_process
from .worker.socket_client import WorkerServiceSocketClient


class ManagedProductionStartupError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        evidence: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.evidence = None if evidence is None else dict(evidence)


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
        persistent_runtime_control: bool = False,
    ) -> "TurboBusRuntimeSession":
        if not str(daemon_socket_path).strip():
            raise ValueError("daemon_socket_path must be non-empty")
        worker_client = None
        if worker_socket_path is not None:
            if not str(worker_socket_path).strip():
                raise ValueError("worker_socket_path must be non-empty")
            worker_client = WorkerServiceSocketClient(str(worker_socket_path))
        daemon_client = TurboBusDaemonClient(str(daemon_socket_path))
        runtime_client_factory = (
            TurboBusPersistentDaemonRuntimeClient
            if persistent_runtime_control
            else TurboBusDaemonRuntimeClient
        )
        return cls.open(
            daemon_client,
            job_id=job_id,
            user_id=user_id,
            runtime_daemon_client=runtime_client_factory(str(daemon_socket_path)),
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
        options = _runtime_options_with_socket_paths(
            options,
            daemon_socket_path=str(resolved_daemon_socket),
            worker_socket_path=str(resolved_worker_socket),
        )
        daemon_path = str(resolved_daemon_socket)
        worker_path = str(resolved_worker_socket)
        startup_lock = threading.Lock()
        startup_records: dict[str, dict[str, object]] = {}
        _update_managed_service_startup_record(
            startup_records,
            startup_lock,
            "daemon",
            service="daemon",
            state="attaching",
            owned=False,
            socket_path=daemon_path,
        )
        worker_stop_event: threading.Event | None = None
        worker_thread: threading.Thread | None = None
        startup_evidence: dict[str, object] | None = None
        try:
            _wait_for_daemon_socket_ready(
                daemon_socket_path=daemon_path,
                startup_records=startup_records,
                startup_lock=startup_lock,
            )
            _update_managed_service_startup_record(
                startup_records,
                startup_lock,
                "worker",
                service="worker",
                state="probing",
                owned=False,
                daemon_socket_path=daemon_path,
                socket_path=worker_path,
            )
            try:
                _wait_for_worker_socket_ready(
                    worker_socket_path=worker_path,
                    startup_records=startup_records,
                    startup_lock=startup_lock,
                    timeout_seconds=0.1,
                    poll_interval_seconds=0.01,
                )
            except Exception:
                _update_managed_service_startup_record(
                    startup_records,
                    startup_lock,
                    "worker",
                    service="worker",
                    state="starting",
                    owned=True,
                    daemon_socket_path=daemon_path,
                    socket_path=worker_path,
                )
                worker_stop_event = threading.Event()
                worker_thread = threading.Thread(
                    target=_run_managed_worker_service,
                    kwargs={
                        "daemon_socket_path": daemon_path,
                        "worker_socket_path": worker_path,
                        "stop_event": worker_stop_event,
                        "backend": backend,
                        "runtime_options": options,
                        "startup_records": startup_records,
                        "startup_lock": startup_lock,
                    },
                    name="turbobus-worker-service",
                    daemon=True,
                )
                worker_thread.start()
                _wait_for_worker_socket_ready(
                    worker_socket_path=worker_path,
                    startup_records=startup_records,
                    startup_lock=startup_lock,
                )
            startup_evidence = _managed_service_startup_snapshot(
                startup_records,
                startup_lock,
            )
            session = cls.open(
                TurboBusDaemonClient(daemon_path),
                job_id=job_id,
                user_id=user_id,
                runtime_daemon_client=TurboBusPersistentDaemonRuntimeClient(daemon_path),
                execution_daemon_client=TurboBusDaemonExecutionClient(daemon_path),
                profile_daemon_client=TurboBusDaemonProfileClient(daemon_path),
                worker_client=WorkerServiceSocketClient(worker_path),
                max_inflight_chunks=max_inflight_chunks,
                backend=backend,
                runtime_options=options,
            )
        except Exception as exc:
            shutdown_evidence = _shutdown_managed_service_threads(
                daemon_stop_event=None,
                daemon_thread=None,
                daemon_socket_path=daemon_path,
                worker_stop_event=worker_stop_event,
                worker_thread=worker_thread,
                worker_socket_path=worker_path,
            )
            startup_snapshot = _managed_service_startup_snapshot(
                startup_records,
                startup_lock,
            )
            raise _managed_startup_error(
                exc,
                startup_evidence=startup_snapshot,
                shutdown_evidence=shutdown_evidence,
            ) from exc
        session._managed_service_startup_evidence = startup_evidence
        session._managed_service_records = startup_records
        session._managed_service_lock = startup_lock
        if worker_stop_event is not None and worker_thread is not None:
            session._owned_worker_stop_event = worker_stop_event
            session._owned_worker_thread = worker_thread
            session._owned_worker_socket_path = worker_path
        session._owned_daemon_socket_path = None
        session._owned_daemon_stop_event = None
        session._owned_daemon_thread = None
        session._runtime_control_connection_owned = True
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
        options = _runtime_options_with_socket_paths(
            options,
            daemon_socket_path=daemon_path,
            worker_socket_path=worker_path,
        )
        daemon = create_production_daemon(
            daemon_startup_config or DaemonStartupConfig()
        )
        startup_lock = threading.Lock()
        startup_records: dict[str, dict[str, object]] = {}
        _update_managed_service_startup_record(
            startup_records,
            startup_lock,
            "daemon",
            service="daemon",
            state="starting",
            socket_path=daemon_path,
            require_authenticated_peers=bool(
                getattr(daemon, "_require_authenticated_peers", False)
            ),
        )
        daemon_stop_event = threading.Event()
        daemon_thread = threading.Thread(
            target=_run_managed_daemon_service,
            kwargs={
                "daemon": daemon,
                "socket_path": daemon_path,
                "stop_event": daemon_stop_event,
                "startup_records": startup_records,
                "startup_lock": startup_lock,
            },
            name="turbobus-daemon-service",
            daemon=True,
        )
        daemon_thread.start()
        _update_managed_service_startup_record(
            startup_records,
            startup_lock,
            "worker",
            service="worker",
            state="starting",
            daemon_socket_path=daemon_path,
            socket_path=worker_path,
        )
        worker_stop_event = threading.Event()
        worker_thread = threading.Thread(
            target=_run_managed_worker_service,
            kwargs={
                "daemon_socket_path": daemon_path,
                "worker_socket_path": worker_path,
                "stop_event": worker_stop_event,
                "backend": backend,
                "runtime_options": options,
                "startup_records": startup_records,
                "startup_lock": startup_lock,
            },
            name="turbobus-worker-service",
            daemon=True,
        )
        worker_thread.start()
        try:
            startup_evidence = _wait_for_managed_services_ready(
                daemon_socket_path=daemon_path,
                worker_socket_path=worker_path,
                startup_records=startup_records,
                startup_lock=startup_lock,
            )
            session = cls.open_socket(
                daemon_socket_path=daemon_path,
                worker_socket_path=worker_path,
                job_id=job_id,
                user_id=user_id,
                max_inflight_chunks=max_inflight_chunks,
                backend=backend,
                runtime_options=options,
                persistent_runtime_control=True,
            )
        except Exception as exc:
            shutdown_evidence = _shutdown_managed_service_threads(
                daemon_stop_event=daemon_stop_event,
                daemon_thread=daemon_thread,
                daemon_socket_path=daemon_path,
                worker_stop_event=worker_stop_event,
                worker_thread=worker_thread,
                worker_socket_path=worker_path,
            )
            startup_snapshot = _managed_service_startup_snapshot(
                startup_records,
                startup_lock,
            )
            raise _managed_startup_error(
                exc,
                startup_evidence=startup_snapshot,
                shutdown_evidence=shutdown_evidence,
            ) from exc
        session._owned_daemon_stop_event = daemon_stop_event
        session._owned_daemon_thread = daemon_thread
        session._owned_worker_stop_event = worker_stop_event
        session._owned_worker_thread = worker_thread
        session._owned_daemon_socket_path = daemon_path
        session._owned_worker_socket_path = worker_path
        session._runtime_control_connection_owned = True
        session._managed_service_startup_evidence = startup_evidence
        session._managed_service_records = startup_records
        session._managed_service_lock = startup_lock
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
        buffer_ids = self._submitted_intent_buffers.get(normalized_intent_id)
        validate_runtime_receipt(
            receipt,
            intent_id=normalized_intent_id,
            job_id=self.job_id,
            session_id=self.session_id,
            source_buffer_id=(
                None if buffer_ids is None else buffer_ids[0]
            ),
            destination_buffer_id=(
                None if buffer_ids is None else buffer_ids[1]
            ),
        )
        terminal_states = {"complete", "failed", "canceled"}
        state_text = str(getattr(receipt.state, "value", receipt.state)).lower()
        if state_text in terminal_states:
            self._active_intent_ids.discard(normalized_intent_id)
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
        managed_runtime_before_shutdown = self.managed_service_snapshot()
        if self._session_id is None:
            local_cpu_cleanup = self._cleanup_local_cpu_buffers(
                reason="runtime_session_close_without_daemon_session"
            )
            runtime_control_evidence = self._close_runtime_control_connection()
            managed_service_evidence = self._stop_owned_services()
            managed_runtime_after_shutdown = self.managed_service_snapshot()
            clear_runtime_session_state(self)
            self._closed = True
            payload = {
                "closed": False,
                "managed_service_shutdown": managed_service_evidence,
            }
            if local_cpu_cleanup:
                payload["local_cpu_buffer_cleanup"] = local_cpu_cleanup
                owned_release = _owned_cpu_release_records(local_cpu_cleanup)
                if owned_release:
                    payload["owned_cpu_buffer_release"] = owned_release
            if managed_runtime_before_shutdown is not None:
                payload["managed_service_runtime_before_shutdown"] = (
                    managed_runtime_before_shutdown
                )
            if managed_runtime_after_shutdown is not None:
                payload["managed_service_runtime_after_shutdown"] = (
                    managed_runtime_after_shutdown
                )
            if self._managed_service_startup_evidence is not None:
                payload["managed_service_startup"] = dict(
                    self._managed_service_startup_evidence
                )
            if runtime_control_evidence is not None:
                payload["runtime_control_shutdown"] = runtime_control_evidence
            return DaemonResponse(
                ok=True,
                payload=payload,
            )
        intent_wait_evidence = self._close_active_intent_receipts()
        cleanup_errors: list[dict[str, object]] = []
        cleanup_evidence: list[dict[str, object]] = []
        local_cpu_cleanup: list[dict[str, object]] = []
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
            local_cpu_cleanup = self._cleanup_local_cpu_buffers(
                reason="runtime_session_close"
            )
            runtime_control_evidence = self._close_runtime_control_connection()
            managed_service_evidence = self._stop_owned_services()
            managed_runtime_after_shutdown = self.managed_service_snapshot()
            clear_runtime_session_state(self)
            self._closed = True
        payload = {}
        if isinstance(response.payload, Mapping):
            payload.update(dict(response.payload))
        if intent_wait_evidence:
            payload["active_intent_receipts"] = intent_wait_evidence
        if cleanup_evidence:
            payload["buffer_cleanup_evidence"] = cleanup_evidence
        if local_cpu_cleanup:
            payload["local_cpu_buffer_cleanup"] = local_cpu_cleanup
            owned_release = _owned_cpu_release_records(local_cpu_cleanup)
            if owned_release:
                payload["owned_cpu_buffer_release"] = owned_release
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
            payload["runtime_control_shutdown"] = runtime_control_evidence
        if managed_service_evidence:
            payload["managed_service_shutdown"] = managed_service_evidence
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

    def managed_service_snapshot(self) -> dict[str, object] | None:
        return _managed_service_runtime_snapshot(
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
            return receipt
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
        local_cleanup_error: Exception | None = None
        local_cpu_cleanup: dict[str, object] | None = None
        retention_error: Exception | None = None
        retention_evidence: dict[str, object] | None = None
        runtime_owned = normalized_id in self._owned_cpu_buffer_ids
        buffer_was_registered = normalized_id in self._registered_buffer_ids
        if buffer_was_registered:
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
            local_cpu_cleanup = self._cleanup_local_cpu_buffer(
                normalized_id,
                buffer,
                reason=reason,
                runtime_owned=runtime_owned,
            )
            if not bool(local_cpu_cleanup.get("ok", False)):
                local_cleanup_error = RuntimeError(
                    str(
                        local_cpu_cleanup.get("error")
                        or "local CPU buffer cleanup failed"
                    )
                )
        if cleanup_error is None and buffer_was_registered:
            retention_payload = _runtime_buffer_retention_evidence(
                buffer_id=normalized_id,
                buffer=buffer,
                reason=reason,
                runtime_owned=runtime_owned,
                local_cpu_cleanup=local_cpu_cleanup,
            )
            retention_evidence = self._record_buffer_cleanup_retention(
                normalized_id,
                retention_payload,
            )
            if not bool(retention_evidence.get("ok", False)):
                retention_error = RuntimeError(
                    str(
                        retention_evidence.get("error")
                        or "daemon buffer retention update failed"
                    )
                )
        if cleanup_error is not None:
            if local_cleanup_error is not None:
                raise RuntimeError(
                    f"{cleanup_error}; local CPU buffer cleanup failed: {local_cleanup_error}"
                ) from cleanup_error
            raise cleanup_error
        if local_cleanup_error is not None:
            raise local_cleanup_error
        if retention_error is not None:
            raise retention_error
        payload = dict(response.payload) if isinstance(response.payload, Mapping) else {}
        if local_cpu_cleanup is not None:
            payload["local_cpu_buffer_cleanup"] = local_cpu_cleanup
            if bool(local_cpu_cleanup.get("runtime_owned", False)):
                payload["owned_cpu_buffer_release"] = local_cpu_cleanup
        if retention_evidence is not None:
            payload["runtime_buffer_retention"] = retention_evidence
        return DaemonResponse(ok=response.ok, error=response.error, payload=payload)

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
        _update_managed_service_startup_record(
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

    def _close_active_intent_receipts(self) -> list[dict[str, object]]:
        evidence: list[dict[str, object]] = []
        for intent_id in tuple(self._active_intent_ids):
            try:
                receipt = self.wait_transfer_receipt(intent_id, timeout_seconds=0.0)
                evidence.append(
                    {
                        "intent_id": str(intent_id),
                        "ok": True,
                        "state": str(getattr(receipt.state, "value", receipt.state)),
                        "bytes_completed": int(receipt.bytes_completed),
                    }
                )
            except Exception as exc:
                evidence.append(
                    {
                        "intent_id": str(intent_id),
                        "ok": False,
                        "error": str(exc) or exc.__class__.__name__,
                    }
                )
        return evidence


def _run_managed_daemon_service(
    *,
    daemon,
    socket_path: str,
    stop_event: threading.Event,
    startup_records: dict[str, dict[str, object]],
    startup_lock: threading.Lock,
) -> None:
    try:
        daemon.serve_forever(
            socket_path=str(socket_path),
            stop_event=stop_event,
        )
    except Exception as exc:
        _update_managed_service_startup_record(
            startup_records,
            startup_lock,
            "daemon",
            state="failed",
            error=str(exc) or exc.__class__.__name__,
            error_type=exc.__class__.__name__,
        )
        raise
    if not stop_event.is_set():
        _update_managed_service_startup_record(
            startup_records,
            startup_lock,
            "daemon",
            state="stopped",
            error="managed daemon service exited before runtime shutdown",
            error_type="UnexpectedExit",
        )


def _run_managed_worker_service(
    *,
    daemon_socket_path: str,
    worker_socket_path: str,
    stop_event: threading.Event,
    backend,
    runtime_options: RuntimeOptions,
    startup_records: dict[str, dict[str, object]],
    startup_lock: threading.Lock,
) -> None:
    def report_startup(record: dict[str, object]) -> None:
        payload = dict(record)
        payload.pop("service", None)
        _update_managed_service_startup_record(
            startup_records,
            startup_lock,
            "worker",
            **payload,
        )

    try:
        run_worker_service_process(
            daemon_socket_path,
            worker_socket_path,
            stop_event=stop_event,
            backend=backend,
            runtime_options=runtime_options,
            startup_reporter=report_startup,
        )
    except Exception:
        raise
    if not stop_event.is_set():
        _update_managed_service_startup_record(
            startup_records,
            startup_lock,
            "worker",
            state="stopped",
            error="managed worker service exited before runtime shutdown",
            error_type="UnexpectedExit",
        )


def _update_managed_service_startup_record(
    startup_records: dict[str, dict[str, object]],
    startup_lock: threading.Lock,
    service: str,
    **updates,
) -> None:
    with startup_lock:
        existing = dict(startup_records.get(service, {}))
        startup_evidence = updates.get("startup_evidence")
        if isinstance(existing.get("startup_evidence"), Mapping) and not isinstance(
            startup_evidence, Mapping
        ):
            updates["startup_evidence"] = dict(existing["startup_evidence"])
        record = {
            **existing,
            **updates,
            "service": str(service),
        }
        startup_records[str(service)] = record


def _managed_service_startup_snapshot(
    startup_records: dict[str, dict[str, object]],
    startup_lock: threading.Lock,
) -> dict[str, object]:
    with startup_lock:
        services = {
            name: dict(record)
            for name, record in startup_records.items()
        }
    return {
        "services": services,
    }


def _managed_service_failure_record(
    startup_records: dict[str, dict[str, object]],
    startup_lock: threading.Lock,
    service: str,
) -> dict[str, object] | None:
    with startup_lock:
        record = startup_records.get(str(service))
        if not isinstance(record, Mapping):
            return None
        state = str(record.get("state", "")).lower()
        if state not in {"failed", "stopped"}:
            return None
        return dict(record)


def _shutdown_managed_service_threads(
    *,
    daemon_stop_event: threading.Event | None,
    daemon_thread: threading.Thread | None,
    daemon_socket_path: str | None,
    worker_stop_event: threading.Event | None,
    worker_thread: threading.Thread | None,
    worker_socket_path: str | None,
) -> list[dict[str, object]]:
    evidence: list[dict[str, object]] = []
    if worker_stop_event is not None:
        worker_stop_event.set()
    if daemon_stop_event is not None:
        daemon_stop_event.set()
    if worker_thread is not None:
        worker_thread.join(timeout=1.0)
        evidence.append(
            {
                "service": "worker",
                "socket_path": worker_socket_path,
                "alive_after_join": worker_thread.is_alive(),
            }
        )
    if daemon_thread is not None:
        daemon_thread.join(timeout=1.0)
        evidence.append(
            {
                "service": "daemon",
                "socket_path": daemon_socket_path,
                "alive_after_join": daemon_thread.is_alive(),
            }
        )
    return evidence


def _managed_startup_error(
    exc: Exception,
    *,
    startup_evidence: Mapping[str, object],
    shutdown_evidence: Sequence[Mapping[str, object]],
) -> ManagedProductionStartupError:
    message = str(exc) or exc.__class__.__name__
    existing_evidence = (
        dict(exc.evidence)
        if isinstance(exc, ManagedProductionStartupError)
        and isinstance(exc.evidence, Mapping)
        else {}
    )
    startup_payload = dict(existing_evidence.get("startup", {}))
    startup_payload.update(dict(startup_evidence))
    shutdown_payload = [
        dict(item)
        for item in existing_evidence.get("shutdown", ())
        if isinstance(item, Mapping)
    ]
    shutdown_payload.extend(dict(item) for item in shutdown_evidence)
    return ManagedProductionStartupError(
        message,
        evidence={
            "startup": startup_payload,
            "shutdown": shutdown_payload,
        },
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


def _runtime_buffer_retention_evidence(
    *,
    buffer_id: str,
    buffer: ExecutableBuffer | None,
    reason: str,
    runtime_owned: bool,
    local_cpu_cleanup: Mapping[str, object] | None,
) -> dict[str, object]:
    retention = {
        "buffer_id": str(buffer_id),
        "reason": str(reason),
        "runtime_owned": bool(runtime_owned),
    }
    if isinstance(buffer, SharedPinnedCpuBuffer):
        retention["runtime_buffer_kind"] = "shared_pinned_cpu"
        if local_cpu_cleanup is not None:
            retention["local_cpu_buffer_cleanup"] = dict(local_cpu_cleanup)
            if bool(local_cpu_cleanup.get("runtime_owned", False)):
                retention["owned_cpu_buffer_release"] = dict(local_cpu_cleanup)
    elif isinstance(buffer, CudaIpcDeviceBuffer):
        retention["runtime_buffer_kind"] = "cuda_ipc_device"
    else:
        retention["runtime_buffer_kind"] = "unknown"
    return retention


def _owned_cpu_release_records(
    cleanup_records: Iterable[Mapping[str, object]],
) -> list[dict[str, object]]:
    return [
        dict(record)
        for record in cleanup_records
        if isinstance(record, Mapping) and bool(record.get("runtime_owned", False))
    ]


def _wait_for_managed_services_ready(
    *,
    daemon_socket_path: str,
    worker_socket_path: str,
    startup_records: dict[str, dict[str, object]] | None = None,
    startup_lock: threading.Lock | None = None,
    timeout_seconds: float = 5.0,
    poll_interval_seconds: float = 0.05,
) -> dict[str, object]:
    _wait_for_daemon_socket_ready(
        daemon_socket_path=daemon_socket_path,
        startup_records=startup_records,
        startup_lock=startup_lock,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )
    _wait_for_worker_socket_ready(
        worker_socket_path=worker_socket_path,
        startup_records=startup_records,
        startup_lock=startup_lock,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )
    if startup_records is None or startup_lock is None:
        return {}
    return _managed_service_startup_snapshot(startup_records, startup_lock)


def _probe_worker_socket_ready(socket_path: str) -> None:
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        client.connect(str(socket_path))
    finally:
        client.close()


def _managed_service_failure_record_or_none(
    startup_records: dict[str, dict[str, object]] | None,
    startup_lock: threading.Lock | None,
    service: str,
) -> dict[str, object] | None:
    if startup_records is None or startup_lock is None:
        return None
    return _managed_service_failure_record(startup_records, startup_lock, service)


def _update_managed_service_startup_record_if_available(
    startup_records: dict[str, dict[str, object]] | None,
    startup_lock: threading.Lock | None,
    service: str,
    **updates,
) -> None:
    if startup_records is None or startup_lock is None:
        return
    _update_managed_service_startup_record(
        startup_records,
        startup_lock,
        service,
        **updates,
    )


def _managed_service_runtime_snapshot(
    *,
    startup_records: dict[str, dict[str, object]] | None,
    startup_lock: threading.Lock | None,
    daemon_thread: threading.Thread | None,
    daemon_stop_event: threading.Event | None,
    daemon_socket_path: str | None,
    worker_thread: threading.Thread | None,
    worker_stop_event: threading.Event | None,
    worker_socket_path: str | None,
    runtime_control_owned: bool,
    runtime_client: object | None,
) -> dict[str, object] | None:
    if (
        startup_records is None
        and startup_lock is None
        and daemon_thread is None
        and daemon_stop_event is None
        and worker_thread is None
        and worker_stop_event is None
        and daemon_socket_path is None
        and worker_socket_path is None
        and not runtime_control_owned
        and runtime_client is None
    ):
        return None
    snapshot = (
        {"services": {}}
        if startup_records is None or startup_lock is None
        else _managed_service_startup_snapshot(startup_records, startup_lock)
    )
    services = (
        dict(snapshot.get("services", {}))
        if isinstance(snapshot.get("services"), Mapping)
        else {}
    )
    for service_name, thread, stop_event, socket_path in (
        ("daemon", daemon_thread, daemon_stop_event, daemon_socket_path),
        ("worker", worker_thread, worker_stop_event, worker_socket_path),
    ):
        record = (
            dict(services.get(service_name, {}))
            if isinstance(services.get(service_name), Mapping)
            else {}
        )
        present = (
            thread is not None
            or stop_event is not None
            or socket_path is not None
            or bool(record)
        )
        if not present:
            continue
        owned = bool(record.get("owned", False)) or thread is not None or stop_event is not None
        record["service"] = str(service_name)
        record["owned"] = owned
        if thread is not None:
            record["thread_alive"] = bool(thread.is_alive())
        elif owned and "thread_alive" not in record:
            record["thread_alive"] = False
        if stop_event is not None:
            record["stop_requested"] = bool(stop_event.is_set())
        else:
            record["stop_requested"] = bool(record.get("stop_requested", False))
        if socket_path is not None:
            record["socket_path"] = str(socket_path)
            record["socket_exists"] = os.path.exists(str(socket_path))
        elif "socket_path" in record:
            record["socket_exists"] = os.path.exists(str(record["socket_path"]))
        services[service_name] = record
    snapshot["services"] = services
    snapshot["runtime_control"] = {
        "owned": bool(runtime_control_owned),
        "client_type": (
            None if runtime_client is None else runtime_client.__class__.__name__
        ),
        "closed": bool(getattr(runtime_client, "closed", False)),
    }
    return snapshot


def _runtime_options_with_socket_paths(
    options: RuntimeOptions,
    *,
    daemon_socket_path: str,
    worker_socket_path: str,
) -> RuntimeOptions:
    values = {
        field.name: getattr(options, field.name)
        for field in fields(RuntimeOptions)
    }
    values["daemon_socket_path"] = str(daemon_socket_path)
    values["worker_socket_path"] = str(worker_socket_path)
    return RuntimeOptions(**values)


def _wait_for_daemon_socket_ready(
    *,
    daemon_socket_path: str,
    startup_records: dict[str, dict[str, object]] | None = None,
    startup_lock: threading.Lock | None = None,
    timeout_seconds: float = 5.0,
    poll_interval_seconds: float = 0.05,
) -> None:
    deadline = time.time() + max(0.1, float(timeout_seconds))
    daemon_error: Exception | None = None
    while time.time() < deadline:
        daemon_failure = _managed_service_failure_record_or_none(
            startup_records,
            startup_lock,
            "daemon",
        )
        if daemon_failure is not None:
            raise ManagedProductionStartupError(
                "managed daemon service failed before startup completed",
                evidence={"startup": {"services": {"daemon": daemon_failure}}},
            )
        try:
            daemon_ready = TurboBusDaemonProfileClient(str(daemon_socket_path))
            require_ok(daemon_ready.discover_relays(), "daemon startup probe failed")
            _update_managed_service_startup_record_if_available(
                startup_records,
                startup_lock,
                "daemon",
                state="ready",
                ready_at=time.time(),
                ready_probe="discover_relays",
                socket_path=str(daemon_socket_path),
            )
            return
        except Exception as exc:
            daemon_error = exc
            time.sleep(max(0.001, float(poll_interval_seconds)))
    raise RuntimeError(
        f"managed daemon socket did not become ready: {daemon_error}"
    ) from daemon_error


def _wait_for_worker_socket_ready(
    *,
    worker_socket_path: str,
    startup_records: dict[str, dict[str, object]] | None = None,
    startup_lock: threading.Lock | None = None,
    timeout_seconds: float = 5.0,
    poll_interval_seconds: float = 0.05,
) -> None:
    deadline = time.time() + max(0.1, float(timeout_seconds))
    worker_error: Exception | None = None
    while time.time() < deadline:
        worker_failure = _managed_service_failure_record_or_none(
            startup_records,
            startup_lock,
            "worker",
        )
        if worker_failure is not None:
            raise ManagedProductionStartupError(
                "managed worker service failed before startup completed",
                evidence={"startup": {"services": {"worker": worker_failure}}},
            )
        try:
            _probe_worker_socket_ready(str(worker_socket_path))
            _update_managed_service_startup_record_if_available(
                startup_records,
                startup_lock,
                "worker",
                state="ready",
                ready_at=time.time(),
                ready_probe="worker_socket_connect",
                socket_path=str(worker_socket_path),
            )
            return
        except Exception as exc:
            worker_error = exc
            time.sleep(max(0.001, float(poll_interval_seconds)))
    raise RuntimeError(
        f"managed worker socket did not become ready: {worker_error}"
    ) from worker_error


__all__ = ["ManagedProductionStartupError", "TurboBusRuntimeSession"]
