from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field

from .api import TurboBusClient
from .backends.cuda import default_cuda_backend
from .buffer_registration import (
    ExecutableBuffer,
    ranges_or_full_buffer,
    register_executable_buffer,
)
from .client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer
from .daemon import (
    TurboBusDaemonClient,
    TurboBusDaemonExecutionClient,
    TurboBusDaemonProfileClient,
    TurboBusDaemonRuntimeClient,
)
from .intent_executor import WorkerIntentTransferExecutor
from .intent_execution_support import require_ok
from . import profile as runtime_profile
from .ranges import TransferRange, range_as_dict
from .runtime_engine import RuntimeOptions
from .schema import (
    DaemonResponse,
    TransferIntent,
    TransferReceipt,
    WorkloadKind,
)
from .worker import (
    CudaWorkerExecutor,
    WorkerServiceSocketClient,
    WorkerDataPlaneResourceBinder,
    WorkerTransferClient,
)


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
    _submitted_intent_ids: set[str] = field(
        default_factory=set,
        init=False,
        repr=False,
    )
    _client: TurboBusClient | None = field(default=None, init=False, repr=False)
    _profile_bootstrapped: bool = field(default=False, init=False, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

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
        socket_path = getattr(daemon_client, "socket_path", None)
        if runtime_daemon_client is None:
            if socket_path is None:
                raise ValueError("runtime_daemon_client is required without socket_path")
            runtime_daemon_client = TurboBusDaemonRuntimeClient(str(socket_path))
        if execution_daemon_client is None:
            if socket_path is None:
                raise ValueError("execution_daemon_client is required without socket_path")
            execution_daemon_client = TurboBusDaemonExecutionClient(str(socket_path))
        if profile_daemon_client is None:
            if socket_path is None:
                raise ValueError("profile_daemon_client is required without socket_path")
            profile_daemon_client = TurboBusDaemonProfileClient(str(socket_path))
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
        response = self._runtime_daemon_client().register_session(
            int(self._target_gpu),
            int(self.max_inflight_chunks),
        )
        require_ok(response, "daemon session registration failed")
        session_payload = response.payload["session"]
        session_id = str(session_payload["session_id"])
        self._relay_gpus = tuple(
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
        self._session_id = session_id
        return session_id

    def register_cpu_buffer(
        self,
        buffer: SharedPinnedCpuBuffer,
    ) -> SharedPinnedCpuBuffer:
        self._require_open()
        if not isinstance(buffer, SharedPinnedCpuBuffer):
            raise TypeError("buffer must be a SharedPinnedCpuBuffer")
        self._register_buffer(buffer)
        return buffer

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
        chunk_bytes: int = 16 * 1024 * 1024,
        workload_kind: WorkloadKind | str = WorkloadKind.GENERIC,
        priority: int = 0,
        metadata: Mapping[str, object] | None = None,
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
        )

    def offload_d2h(
        self,
        source: CudaIpcDeviceBuffer,
        target: SharedPinnedCpuBuffer,
        *,
        ranges: Iterable[TransferRange | tuple[int, int, int] | dict] | None = None,
        chunk_bytes: int = 16 * 1024 * 1024,
        workload_kind: WorkloadKind | str = WorkloadKind.GENERIC,
        priority: int = 0,
        metadata: Mapping[str, object] | None = None,
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
        )

    def submit_transfer_intent(self, intent: TransferIntent) -> TransferReceipt:
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
        receipt = self._intent_client().submit_transfer_intent(intent)
        _validate_runtime_receipt(
            receipt,
            intent_id=intent.intent_id,
            job_id=self.job_id,
            session_id=self.session_id,
        )
        self._submitted_intent_ids.add(intent.intent_id)
        return receipt

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
        receipt = self._intent_client().wait_transfer_receipt(
            normalized_intent_id,
            timeout_seconds=timeout_seconds,
        )
        _validate_runtime_receipt(
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
        profile, response = runtime_profile.bootstrap_daemon_profile(
            self._profile_daemon_client(),
            self.backend,
            self.runtime_options,
            target_gpu=int(self._target_gpu),
            relay_gpus=relays,
            force=force,
        )
        self._profile_bootstrapped = True
        return response

    def close(self) -> DaemonResponse:
        if self._closed:
            return DaemonResponse(
                ok=True,
                payload={"closed": False, "already_closed": True},
            )
        if self._session_id is None:
            self._clear_local_session_state()
            self._closed = True
            return DaemonResponse(ok=True, payload={"closed": False})
        response = self._runtime_daemon_client().close_session(self._session_id)
        if response.ok:
            self._clear_local_session_state()
            self._closed = True
        return response

    def __enter__(self) -> "TurboBusRuntimeSession":
        self._require_open()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def _register_buffer(self, buffer: ExecutableBuffer) -> None:
        self._require_open()
        if buffer.job_id != self.job_id:
            raise ValueError("buffer job_id must match the runtime session job_id")
        if isinstance(buffer, CudaIpcDeviceBuffer):
            self._bind_target_gpu(buffer.device_index)
        self._buffers[buffer.buffer_id] = buffer
        if self._target_gpu is None:
            return
        self.open_session()
        self._register_pending_buffers()

    def _submit_transfer_intent(
        self,
        source: ExecutableBuffer,
        target: ExecutableBuffer,
        *,
        direction: str,
        ranges: Iterable[TransferRange | tuple[int, int, int] | dict] | None,
        chunk_bytes: int,
        workload_kind: WorkloadKind | str,
        priority: int,
        metadata: Mapping[str, object] | None,
    ) -> TransferReceipt:
        self._require_open()
        self._ensure_transfer_buffers(source, target)
        self._bootstrap_profile_if_enabled()
        normalized_ranges = tuple(
            range_as_dict(item)
            for item in ranges_or_full_buffer(ranges, source.size_bytes, target.size_bytes)
        )
        total_bytes = sum(int(item["bytes"]) for item in normalized_ranges)
        intent = TransferIntent(
            intent_id=f"intent-{uuid.uuid4().hex}",
            job_id=self.job_id,
            session_id=self.session_id,
            source_buffer_id=source.buffer_id,
            destination_buffer_id=target.buffer_id,
            direction=direction,
            total_bytes=total_bytes,
            ranges=normalized_ranges,
            workload_kind=workload_kind,
            priority=int(priority),
            policy_hints={"chunk_bytes": int(chunk_bytes)},
            metadata={} if metadata is None else dict(metadata),
        )
        self._validate_intent_uses_runtime_buffers(intent)
        receipt = self._intent_client().submit_transfer_intent(intent)
        _validate_runtime_receipt(
            receipt,
            intent_id=intent.intent_id,
            job_id=self.job_id,
            session_id=self.session_id,
        )
        self._submitted_intent_ids.add(intent.intent_id)
        return receipt

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

    def _intent_client(self) -> TurboBusClient:
        self._require_open()
        if self._client is not None:
            return self._client
        execution_daemon_client = self._execution_daemon_client()
        worker_client = self.worker_client or WorkerTransferClient(
            execution_daemon_client,
            executor=CudaWorkerExecutor(
                backend=self.backend,
                options=self.runtime_options,
            ),
            resource_binder=WorkerDataPlaneResourceBinder(backend=self.backend),
        )
        self.worker_client = worker_client
        executor = WorkerIntentTransferExecutor(
            buffers=self._buffers,
            worker_client=worker_client,
            backend=self.backend,
            runtime_options=self.runtime_options,
        )
        self._client = TurboBusClient(
            daemon=self.daemon_client,
            transfer_executor=executor,
            execution_daemon=execution_daemon_client,
        )
        return self._client

    def _register_pending_buffers(self) -> None:
        self._require_open()
        self.session_id
        for buffer_id, buffer in tuple(self._buffers.items()):
            fingerprint = _buffer_registration_fingerprint(buffer)
            if self._registered_buffer_fingerprints.get(buffer_id) == fingerprint:
                continue
            register_executable_buffer(self._runtime_daemon_client(), buffer)
            self._registered_buffer_ids.add(buffer_id)
            self._registered_buffer_fingerprints[buffer_id] = fingerprint

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

    def _clear_local_session_state(self) -> None:
        self._session_id = None
        self._target_gpu = None
        self._relay_gpus = None
        self._client = None
        self._profile_bootstrapped = False
        self._buffers.clear()
        self._registered_buffer_ids.clear()
        self._registered_buffer_fingerprints.clear()
        self._submitted_intent_ids.clear()

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

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("TurboBus runtime session is closed")

    def _validate_intent_uses_runtime_buffers(self, intent: TransferIntent) -> None:
        source = self._buffers[intent.source_buffer_id]
        target = self._buffers[intent.destination_buffer_id]
        direction = str(intent.direction).lower()
        if direction == "h2d":
            if not isinstance(source, SharedPinnedCpuBuffer):
                raise ValueError("h2d intent source must be a registered CPU buffer")
            if not isinstance(target, CudaIpcDeviceBuffer):
                raise ValueError("h2d intent destination must be a registered CUDA buffer")
        elif direction == "d2h":
            if not isinstance(source, CudaIpcDeviceBuffer):
                raise ValueError("d2h intent source must be a registered CUDA buffer")
            if not isinstance(target, SharedPinnedCpuBuffer):
                raise ValueError("d2h intent destination must be a registered CPU buffer")
        else:
            raise ValueError("intent direction must be h2d or d2h")
        _validate_intent_ranges_fit_buffers(
            intent,
            source_bytes=source.size_bytes,
            target_bytes=target.size_bytes,
        )


__all__ = ["TurboBusRuntimeSession"]


def _buffer_registration_fingerprint(buffer: ExecutableBuffer) -> tuple[object, ...]:
    registration = buffer.buffer_registration()
    metadata = tuple(
        sorted((str(key), str(value)) for key, value in registration.metadata.items())
    )
    return (
        registration.buffer_id,
        registration.job_id,
        registration.kind,
        registration.size_bytes,
        registration.device_index,
        registration.address,
        registration.pinned,
        registration.handle_type,
        metadata,
    )


def _validate_intent_ranges_fit_buffers(
    intent: TransferIntent,
    *,
    source_bytes: int,
    target_bytes: int,
) -> None:
    total_bytes = 0
    for item in intent.ranges:
        source_offset = int(item["src_offset"])
        target_offset = int(item["dst_offset"])
        bytes_count = int(item["bytes"])
        if source_offset < 0 or target_offset < 0:
            raise ValueError("intent range offsets must be non-negative")
        if bytes_count <= 0:
            raise ValueError("intent range bytes must be positive")
        if source_offset + bytes_count > int(source_bytes):
            raise ValueError("intent range exceeds runtime source buffer")
        if target_offset + bytes_count > int(target_bytes):
            raise ValueError("intent range exceeds runtime destination buffer")
        total_bytes += bytes_count
    if total_bytes != int(intent.total_bytes):
        raise ValueError("intent total_bytes must match runtime buffer ranges")


def _validate_runtime_receipt(
    receipt: TransferReceipt,
    *,
    intent_id: str,
    job_id: str,
    session_id: str,
) -> None:
    if not isinstance(receipt, TransferReceipt):
        raise TypeError("runtime transfer must return a TransferReceipt")
    if receipt.intent_id != str(intent_id):
        raise ValueError("runtime receipt intent_id does not match submitted intent")
    if receipt.job_id != str(job_id):
        raise ValueError("runtime receipt job_id does not match runtime session")
    if receipt.session_id != str(session_id):
        raise ValueError("runtime receipt session_id does not match runtime session")
    metadata = receipt.metadata if isinstance(receipt.metadata, Mapping) else {}
    for key in ("execution_ticket_id", "evidence_ticket_id"):
        ticket_id = metadata.get(key)
        if ticket_id is not None and str(ticket_id) != receipt.ticket_id:
            raise ValueError(f"runtime receipt {key} does not match receipt ticket_id")
