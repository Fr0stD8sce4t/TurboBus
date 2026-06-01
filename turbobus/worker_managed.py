from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

from .backends.cuda import default_cuda_backend
from .buffer_registration import (
    ExecutableBuffer,
    ranges_or_full_buffer,
    register_executable_buffers,
)
from .client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer
from .direct_fallback import execute_direct_fallback_transfer, is_direct_only_worker_plan
from .runtime_engine import RuntimeOptions
from .schema import (
    DaemonResponse,
    WorkerTransferAuthorizationRequest,
)
from .transfer import TransferRange, TransferRequest
from .transfer_execution import (
    WorkerCompletionEnvelopeError,
    cleanup_planned_relay_leases,
    plan_transfer_request,
    require_daemon_transfer_complete,
    require_ok,
    require_worker_plan_matches_leases,
    submit_worker_execution,
    worker_lease_tokens,
)
from .worker import (
    CudaWorkerExecutor,
    WorkerDataPlaneCompletionEnvelope,
    WorkerDataPlaneResourceBinder,
    WorkerTransferClient,
    WorkerTransferLifecycleRecord,
)


@dataclass(frozen=True)
class WorkerManagedTransferResult:
    transfer_id: str
    session_id: str
    job_id: str
    source_buffer_id: str
    target_buffer_id: str
    plan: Mapping[str, object]
    lease_token: Mapping[str, object] | None
    authorization_request: WorkerTransferAuthorizationRequest | None
    worker_lifecycle: WorkerTransferLifecycleRecord | None
    final_status: Mapping[str, object]
    worker_completion: WorkerDataPlaneCompletionEnvelope | None = None
    lease_tokens: tuple[Mapping[str, object], ...] = ()

    @property
    def bytes_completed(self) -> int:
        return int(self.final_status.get("bytes_completed", 0))

    @property
    def state(self) -> str:
        state = self.final_status.get("state", "unknown")
        return str(getattr(state, "value", state))


@dataclass
class WorkerManagedTransferClient:
    daemon_client: object
    worker_client: object
    target_gpu: int
    relay_gpus: Iterable[int]
    max_inflight_chunks: int = 8
    backend: object = default_cuda_backend
    runtime_options: RuntimeOptions = field(default_factory=RuntimeOptions)
    _session_id: str | None = field(default=None, init=False, repr=False)

    def open_session(self) -> str:
        if self._session_id is not None:
            return self._session_id
        response = self.daemon_client.register_session(
            int(self.target_gpu),
            [int(gpu) for gpu in self.relay_gpus],
            int(self.max_inflight_chunks),
        )
        require_ok(response, "daemon session registration failed")
        session_id = str(response.payload["session"]["session_id"])
        self._session_id = session_id
        return session_id

    def close_session(self) -> DaemonResponse:
        if self._session_id is None:
            return DaemonResponse(ok=True, payload={"closed": False})
        response = self.daemon_client.close_session(self._session_id)
        if response.ok:
            self._session_id = None
        return response

    def fetch_shared_cpu_to_cuda_ipc(
        self,
        source: SharedPinnedCpuBuffer,
        target: CudaIpcDeviceBuffer,
        *,
        ranges: Iterable[TransferRange | tuple[int, int, int] | dict] | None = None,
        chunk_bytes: int = 16 * 1024 * 1024,
        mode: str = "relay",
        job_id: str | None = None,
        user_id: str | None = None,
    ) -> WorkerManagedTransferResult:
        if not isinstance(source, SharedPinnedCpuBuffer):
            raise TypeError("source must be a SharedPinnedCpuBuffer")
        if not isinstance(target, CudaIpcDeviceBuffer):
            raise TypeError("target must be a CudaIpcDeviceBuffer")
        return self._submit_worker_managed_transfer(
            source,
            target,
            direction="h2d",
            ranges=ranges,
            chunk_bytes=chunk_bytes,
            mode=mode,
            job_id=job_id,
            user_id=user_id,
        )

    def offload_cuda_ipc_to_shared_cpu(
        self,
        source: CudaIpcDeviceBuffer,
        target: SharedPinnedCpuBuffer,
        *,
        ranges: Iterable[TransferRange | tuple[int, int, int] | dict] | None = None,
        chunk_bytes: int = 16 * 1024 * 1024,
        mode: str = "relay",
        job_id: str | None = None,
        user_id: str | None = None,
    ) -> WorkerManagedTransferResult:
        if not isinstance(source, CudaIpcDeviceBuffer):
            raise TypeError("source must be a CudaIpcDeviceBuffer")
        if not isinstance(target, SharedPinnedCpuBuffer):
            raise TypeError("target must be a SharedPinnedCpuBuffer")
        return self._submit_worker_managed_transfer(
            source,
            target,
            direction="d2h",
            ranges=ranges,
            chunk_bytes=chunk_bytes,
            mode=mode,
            job_id=job_id,
            user_id=user_id,
        )

    def _submit_worker_managed_transfer(
        self,
        source: ExecutableBuffer,
        target: ExecutableBuffer,
        *,
        direction: str,
        ranges: Iterable[TransferRange | tuple[int, int, int] | dict] | None,
        chunk_bytes: int,
        mode: str,
        job_id: str | None,
        user_id: str | None,
    ) -> WorkerManagedTransferResult:
        job = str(job_id or source.job_id)
        if target.job_id != job or source.job_id != job:
            raise ValueError("source and target buffers must belong to the transfer job")
        session_id = self.open_session()
        require_ok(
            self.daemon_client.register_job(
                job_id=job,
                user_id=user_id,
                session_id=session_id,
            ),
            "daemon job registration failed",
        )
        register_executable_buffers(self.daemon_client, (source, target))

        transfer_request = TransferRequest.from_ranges(
            ranges_or_full_buffer(ranges, source.size_bytes, target.size_bytes),
            chunk_bytes=int(chunk_bytes),
            direction=direction,
            mode=mode,
            job_id=job,
            metadata={
                "buffer_ids": (
                    source.buffer_id,
                    target.buffer_id,
                )
            },
        )
        planned = plan_transfer_request(
            self.daemon_client,
            session_id,
            transfer_request,
            mode=mode,
        )
        require_ok(planned, "daemon transfer planning failed")
        if is_direct_only_worker_plan(planned.payload):
            return execute_direct_fallback_transfer(
                daemon_client=self.daemon_client,
                backend=self.backend,
                runtime_options=self.runtime_options,
                transfer_request=transfer_request,
                planned_payload=planned.payload,
                session_id=session_id,
                job_id=job,
                source=source,
                target=target,
                result_factory=WorkerManagedTransferResult,
            )
        lease_tokens = worker_lease_tokens(self.daemon_client, planned)
        primary_lease_token = lease_tokens[0]
        try:
            require_worker_plan_matches_leases(
                planned.payload,
                lease_tokens,
                direction=direction,
            )
        except Exception:
            cleanup_planned_relay_leases(self.daemon_client, lease_tokens)
            raise
        authorization_request = WorkerTransferAuthorizationRequest(
            transfer_id=str(planned.payload["transfer_id"]),
            lease_id=str(primary_lease_token["lease_id"]),
            token=str(primary_lease_token["token"]),
            session_id=session_id,
            job_id=job,
            src_buffer_id=source.buffer_id,
            dst_buffer_id=target.buffer_id,
            direction=direction,
            ranges=(),
            relay_gpu=int(primary_lease_token["relay_gpu"]),
        )
        try:
            worker_execution = submit_worker_execution(
                self.worker_client,
                authorization_request,
                expected_bytes=transfer_request.total_bytes,
            )
        except WorkerCompletionEnvelopeError:
            cleanup_planned_relay_leases(
                self.daemon_client,
                lease_tokens,
                reason="worker_completion_invalid",
                strict=False,
            )
            raise
        except Exception:
            cleanup_planned_relay_leases(
                self.daemon_client,
                lease_tokens,
                reason="worker_execution_exception",
                strict=False,
            )
            raise
        try:
            status = self.daemon_client.transfer_status(
                str(planned.payload["transfer_id"])
            )
            require_ok(status, "daemon transfer status query failed")
            final_status = dict(status.payload["status"])
        except Exception:
            cleanup_planned_relay_leases(
                self.daemon_client,
                lease_tokens,
                reason="daemon_status_query_failed",
                strict=False,
            )
            raise
        if worker_execution.final_state != "complete":
            cleanup_planned_relay_leases(
                self.daemon_client,
                lease_tokens,
                reason="worker_completion_not_complete",
                strict=False,
            )
            raise RuntimeError(
                worker_execution.error
                or final_status.get("error")
                or "worker-managed transfer did not complete"
            )
        try:
            require_daemon_transfer_complete(
                final_status,
                expected_bytes=transfer_request.total_bytes,
            )
        except Exception:
            cleanup_planned_relay_leases(
                self.daemon_client,
                lease_tokens,
                reason="daemon_completion_mismatch",
                strict=False,
            )
            raise
        return WorkerManagedTransferResult(
            transfer_id=str(planned.payload["transfer_id"]),
            session_id=session_id,
            job_id=job,
            source_buffer_id=source.buffer_id,
            target_buffer_id=target.buffer_id,
            plan=planned.payload,
            lease_token=primary_lease_token,
            lease_tokens=lease_tokens,
            authorization_request=authorization_request,
            worker_lifecycle=worker_execution.lifecycle,
            worker_completion=worker_execution.completion,
            final_status=final_status,
        )


def make_worker_managed_transfer_client(
    daemon_client,
    *,
    target_gpu: int,
    relay_gpus: Iterable[int],
    worker_client: object | None = None,
    max_inflight_chunks: int = 8,
    backend=default_cuda_backend,
    runtime_options: RuntimeOptions | None = None,
) -> WorkerManagedTransferClient:
    options = runtime_options or RuntimeOptions()
    return WorkerManagedTransferClient(
        daemon_client=daemon_client,
        worker_client=worker_client or WorkerTransferClient(
            daemon_client,
            executor=CudaWorkerExecutor(backend=backend, options=options),
            resource_binder=WorkerDataPlaneResourceBinder(backend=backend),
        ),
        target_gpu=int(target_gpu),
        relay_gpus=tuple(int(gpu) for gpu in relay_gpus),
        max_inflight_chunks=int(max_inflight_chunks),
        backend=backend,
        runtime_options=options,
    )


__all__ = [
    "WorkerManagedTransferClient",
    "WorkerManagedTransferResult",
    "make_worker_managed_transfer_client",
]
