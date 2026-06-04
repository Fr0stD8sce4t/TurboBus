from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
import time

from .backends.cuda import default_cuda_backend
from .buffer_registration import ExecutableBuffer
from .client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer
from .direct_fallback import execute_direct_fallback_transfer, is_direct_only_worker_plan
from .runtime_engine import RuntimeOptions
from .schema import (
    DaemonResponse,
    TransferIntent,
    TransferReceipt,
    WorkerTransferAuthorizationRequest,
)
from .transfer import TransferRequest
from .transfer_execution import (
    WorkerCompletionEnvelopeError,
    cleanup_planned_relay_leases,
    receipt_from_daemon_payload,
    require_ok,
    require_worker_plan_matches_leases,
    submit_worker_execution,
    wait_for_intent_receipt,
    worker_lease_tokens,
)
from .worker import WorkerDataPlaneCompletionEnvelope, WorkerTransferLifecycleRecord


@dataclass(frozen=True)
class WorkerIntentTransferResult:
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
class WorkerIntentTransferExecutor:
    """Execute daemon-submitted TransferIntent payloads without choosing routes."""

    buffers: Mapping[str, ExecutableBuffer]
    worker_client: object
    backend: object = default_cuda_backend
    runtime_options: RuntimeOptions = field(default_factory=RuntimeOptions)

    def execute_transfer_intent(
        self,
        intent: TransferIntent,
        response: DaemonResponse,
        daemon_client,
    ) -> TransferReceipt:
        if not isinstance(intent, TransferIntent):
            raise TypeError("intent must be a TransferIntent")
        require_ok(response, "daemon transfer intent submission failed")
        source, target = _intent_buffers(self.buffers, intent)
        transfer_request = _transfer_request_from_intent(intent)
        payload = _intent_execution_payload(response.payload)
        admission_error = _intent_execution_admission_error(payload)
        if admission_error is not None:
            raise RuntimeError(admission_error)
        if is_direct_only_worker_plan(payload):
            execute_direct_fallback_transfer(
                daemon_client=daemon_client,
                backend=self.backend,
                runtime_options=self.runtime_options,
                transfer_request=transfer_request,
                planned_payload=payload,
                session_id=intent.session_id,
                job_id=intent.job_id,
                source=source,
                target=target,
                result_factory=WorkerIntentTransferResult,
            )
            return wait_for_intent_receipt(daemon_client, intent.intent_id)
        lease_tokens = worker_lease_tokens(daemon_client, response)
        if not lease_tokens:
            return receipt_from_daemon_payload(
                payload,
                expected_intent_id=intent.intent_id,
            )
        _validate_intent_lease_tokens(daemon_client, intent, lease_tokens)
        primary_lease_token = lease_tokens[0]
        try:
            require_worker_plan_matches_leases(
                payload,
                lease_tokens,
                direction=intent.direction,
            )
            authorization_request = WorkerTransferAuthorizationRequest(
                transfer_id=str(payload["transfer_id"]),
                lease_id=str(primary_lease_token["lease_id"]),
                token=str(primary_lease_token["token"]),
                session_id=intent.session_id,
                job_id=intent.job_id,
                src_buffer_id=intent.source_buffer_id,
                dst_buffer_id=intent.destination_buffer_id,
                direction=intent.direction,
                ranges=(),
                relay_gpu=int(primary_lease_token["relay_gpu"]),
            )
            worker_execution = submit_worker_execution(
                self.worker_client,
                authorization_request,
                expected_bytes=int(intent.total_bytes),
            )
        except WorkerCompletionEnvelopeError:
            cleanup_planned_relay_leases(
                daemon_client,
                lease_tokens,
                reason="worker_completion_invalid",
                strict=False,
            )
            raise
        except Exception:
            cleanup_planned_relay_leases(
                daemon_client,
                lease_tokens,
                reason="worker_execution_exception",
                strict=False,
            )
            raise
        if worker_execution.final_state != "complete":
            cleanup_planned_relay_leases(
                daemon_client,
                lease_tokens,
                reason="worker_completion_not_complete",
                strict=False,
            )
            raise RuntimeError(
                worker_execution.error or "worker-managed intent transfer did not complete"
            )
        return wait_for_intent_receipt(daemon_client, intent.intent_id)


def _intent_buffers(
    buffers: Mapping[str, ExecutableBuffer],
    intent: TransferIntent,
) -> tuple[ExecutableBuffer, ExecutableBuffer]:
    try:
        source = buffers[intent.source_buffer_id]
        target = buffers[intent.destination_buffer_id]
    except KeyError as exc:
        raise ValueError(f"missing executable buffer for intent: {exc.args[0]}") from exc
    if source.job_id != intent.job_id or target.job_id != intent.job_id:
        raise ValueError("intent buffers must belong to the intent job")
    return source, target


def _transfer_request_from_intent(intent: TransferIntent) -> TransferRequest:
    return TransferRequest.from_ranges(
        intent.ranges,
        chunk_bytes=_intent_chunk_bytes(intent),
        direction=intent.direction,
        mode="auto",
        job_id=intent.job_id,
        metadata={
            "buffer_ids": (
                intent.source_buffer_id,
                intent.destination_buffer_id,
            )
        },
    )


def _intent_chunk_bytes(intent: TransferIntent) -> int:
    for source in (intent.policy_hints, intent.metadata):
        if not isinstance(source, Mapping):
            continue
        value = source.get("chunk_bytes")
        if value is None:
            continue
        chunk_bytes = int(value)
        if chunk_bytes <= 0:
            raise ValueError("chunk_bytes must be positive")
        return chunk_bytes
    return max(1, int(intent.total_bytes))


def _intent_execution_payload(payload: Mapping[str, object]) -> dict[str, object]:
    execution_payload = dict(payload)
    if "plan" not in execution_payload:
        decision = execution_payload.get("decision")
        if isinstance(decision, Mapping):
            plan = decision.get("plan")
            if isinstance(plan, Mapping):
                execution_payload["plan"] = dict(plan)
    return execution_payload


def _intent_execution_admission_error(payload: Mapping[str, object]) -> str | None:
    admission = payload.get("admission")
    if isinstance(admission, Mapping):
        state = str(admission.get("state", "")).lower()
        if state and state != "admitted":
            return f"transfer admission is {state}"
    expires_at = payload.get("plan_expires_at")
    if expires_at is not None and time.time() > float(expires_at):
        return "transfer plan expired"
    return None


def _validate_intent_lease_tokens(
    daemon_client,
    intent: TransferIntent,
    lease_tokens: Iterable[Mapping[str, object]],
) -> None:
    validator = getattr(daemon_client, "validate_lease", None)
    if not callable(validator):
        return
    for lease_token in lease_tokens:
        response = validator(
            lease_id=str(lease_token["lease_id"]),
            token=str(lease_token["token"]),
            session_id=intent.session_id,
            relay_gpu=int(lease_token["relay_gpu"]),
            job_id=intent.job_id,
            buffer_ids=[intent.source_buffer_id, intent.destination_buffer_id],
        )
        if not isinstance(response, DaemonResponse):
            raise TypeError("daemon lease validation must return a DaemonResponse")
        if not response.ok:
            raise RuntimeError(response.error or "intent lease validation failed")


__all__ = ["WorkerIntentTransferExecutor"]
