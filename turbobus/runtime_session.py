from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

from .api import TurboBusClient
from .backends.cuda import default_cuda_backend
from .buffer_registration import (
    ExecutableBuffer,
    ranges_or_full_buffer,
    register_executable_buffer,
)
from .client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer
from .intent_executor import WorkerIntentTransferExecutor
from .runtime_engine import RuntimeOptions
from .schema import (
    DaemonResponse,
    TransferIntent,
    TransferReceipt,
    WorkloadKind,
)
from .transfer import TransferRange
from .transfer_execution import require_ok
from .worker import (
    CudaWorkerExecutor,
    WorkerDataPlaneResourceBinder,
    WorkerTransferClient,
)


@dataclass
class TurboBusRuntimeSession:
    daemon_client: object
    job_id: str
    target_gpu: int | None = None
    relay_gpus: Iterable[int] | None = None
    user_id: str | None = None
    worker_client: object | None = None
    max_inflight_chunks: int = 8
    backend: object = default_cuda_backend
    runtime_options: RuntimeOptions = field(default_factory=RuntimeOptions)
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
    _client: TurboBusClient | None = field(default=None, init=False, repr=False)

    @classmethod
    def open(
        cls,
        daemon_client,
        *,
        job_id: str,
        target_gpu: int | None = None,
        relay_gpus: Iterable[int] | None = None,
        user_id: str | None = None,
        worker_client: object | None = None,
        max_inflight_chunks: int = 8,
        backend=default_cuda_backend,
        runtime_options: RuntimeOptions | None = None,
    ) -> "TurboBusRuntimeSession":
        session = cls(
            daemon_client=daemon_client,
            job_id=str(job_id),
            target_gpu=None if target_gpu is None else int(target_gpu),
            relay_gpus=(
                None if relay_gpus is None else tuple(int(gpu) for gpu in relay_gpus)
            ),
            user_id=user_id,
            worker_client=worker_client,
            max_inflight_chunks=int(max_inflight_chunks),
            backend=backend,
            runtime_options=runtime_options or RuntimeOptions(),
        )
        if session.target_gpu is not None:
            session.open_session()
        return session

    @property
    def session_id(self) -> str:
        if self._session_id is None:
            raise RuntimeError("TurboBus runtime session is not open")
        return self._session_id

    def open_session(self) -> str:
        if self._session_id is not None:
            return self._session_id
        if self.target_gpu is None:
            raise RuntimeError(
                "target_gpu is not known; register a CUDA buffer before opening "
                "the daemon session"
            )
        relay_gpus = self._relay_gpus_for_session()
        response = self.daemon_client.register_session(
            int(self.target_gpu),
            list(relay_gpus),
            int(self.max_inflight_chunks),
        )
        require_ok(response, "daemon session registration failed")
        session_id = str(response.payload["session"]["session_id"])
        require_ok(
            self.daemon_client.register_job(
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
        if not isinstance(buffer, SharedPinnedCpuBuffer):
            raise TypeError("buffer must be a SharedPinnedCpuBuffer")
        self._register_buffer(buffer)
        return buffer

    def register_cuda_buffer(
        self,
        buffer: CudaIpcDeviceBuffer,
    ) -> CudaIpcDeviceBuffer:
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

    def close(self) -> DaemonResponse:
        if self._session_id is None:
            return DaemonResponse(ok=True, payload={"closed": False})
        response = self.daemon_client.close_session(self._session_id)
        if response.ok:
            self._session_id = None
            self._client = None
            self._registered_buffer_ids.clear()
        return response

    def __enter__(self) -> "TurboBusRuntimeSession":
        self.open_session()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def _register_buffer(self, buffer: ExecutableBuffer) -> None:
        if buffer.job_id != self.job_id:
            raise ValueError("buffer job_id must match the runtime session job_id")
        if isinstance(buffer, CudaIpcDeviceBuffer):
            self._bind_target_gpu(buffer.device_index)
        self._buffers[buffer.buffer_id] = buffer
        if self.target_gpu is None:
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
        self._ensure_transfer_buffers(source, target)
        normalized_ranges = tuple(
            _range_as_dict(item)
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
        return self._intent_client().submit_transfer_intent(intent)

    def _ensure_transfer_buffers(
        self,
        source: ExecutableBuffer,
        target: ExecutableBuffer,
    ) -> None:
        if source.job_id != self.job_id or target.job_id != self.job_id:
            raise ValueError("transfer buffers must match the runtime session job_id")
        if source.buffer_id not in self._buffers:
            self._register_buffer(source)
        if target.buffer_id not in self._buffers:
            self._register_buffer(target)
        self.open_session()
        self._register_pending_buffers()

    def _intent_client(self) -> TurboBusClient:
        if self._client is not None:
            return self._client
        worker_client = self.worker_client or WorkerTransferClient(
            self.daemon_client,
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
        )
        return self._client

    def _register_pending_buffers(self) -> None:
        self.session_id
        for buffer_id, buffer in tuple(self._buffers.items()):
            if buffer_id in self._registered_buffer_ids:
                continue
            register_executable_buffer(self.daemon_client, buffer)
            self._registered_buffer_ids.add(buffer_id)

    def _bind_target_gpu(self, device_index: int) -> None:
        device = int(device_index)
        if self.target_gpu is None:
            self.target_gpu = device
            return
        if int(self.target_gpu) != device:
            raise ValueError("CUDA buffer device_index must match runtime target_gpu")

    def _relay_gpus_for_session(self) -> tuple[int, ...]:
        if self.relay_gpus is not None:
            return tuple(int(gpu) for gpu in self.relay_gpus)
        discovery = getattr(self.daemon_client, "discover_relays", None)
        if not callable(discovery):
            raise RuntimeError(
                "relay_gpus were not provided and daemon client does not support "
                "relay discovery"
            )
        response = discovery(target_gpu=int(self.target_gpu))
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
        self.relay_gpus = relays
        return relays


__all__ = ["TurboBusRuntimeSession"]


def _range_as_dict(
    item: TransferRange | tuple[int, int, int] | dict,
) -> dict[str, int]:
    if isinstance(item, TransferRange):
        return item.as_dict()
    if isinstance(item, Mapping):
        return {
            "src_offset": int(item["src_offset"]),
            "dst_offset": int(item["dst_offset"]),
            "bytes": int(item["bytes"]),
        }
    if isinstance(item, tuple) or isinstance(item, list):
        if len(item) != 3:
            raise ValueError("range tuples must be (src_offset, dst_offset, bytes)")
        return {
            "src_offset": int(item[0]),
            "dst_offset": int(item[1]),
            "bytes": int(item[2]),
        }
    return {
        "src_offset": int(getattr(item, "src_offset")),
        "dst_offset": int(getattr(item, "dst_offset")),
        "bytes": int(getattr(item, "bytes")),
    }
