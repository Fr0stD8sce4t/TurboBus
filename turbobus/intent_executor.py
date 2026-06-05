from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
import time

from .backends.cuda import default_cuda_backend
from .buffer_registration import ExecutableBuffer
from .client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer
from .direct_fallback import (
    execute_direct_fallback_transfer,
    execute_direct_ticket_plan,
    is_direct_only_worker_plan,
)
from .runtime_options import RuntimeOptions
from .schema import (
    DaemonResponse,
    TransferIntent,
    TransferReceipt,
    WorkerTransferAuthorizationRequest,
)
from .intent_execution_support import (
    WorkerCompletionEnvelopeError,
    cleanup_planned_relay_leases,
    receipt_from_daemon_payload,
    require_ok,
    require_worker_plan_matches_leases,
    submit_worker_execution,
    wait_for_intent_receipt,
)
from .worker.models import (
    WorkerDataPlaneCompletionEnvelope,
    WorkerTransferLifecycleRecord,
)


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
        payload = _admitted_intent_execution_payload(
            daemon_client=daemon_client,
            intent=intent,
            response=response,
            timeout_seconds=self.runtime_options.admission_retry_timeout_seconds,
            interval_seconds=self.runtime_options.admission_retry_interval_seconds,
        )
        admission_error = _intent_execution_admission_error(payload)
        if admission_error is not None:
            raise RuntimeError(admission_error)
        if is_direct_only_worker_plan(payload):
            execute_direct_fallback_transfer(
                daemon_client=daemon_client,
                backend=self.backend,
                runtime_options=self.runtime_options,
                intent=intent,
                planned_payload=payload,
                source=source,
                target=target,
                result_factory=WorkerIntentTransferResult,
            )
            return wait_for_intent_receipt(daemon_client, intent.intent_id)
        direct_plan_bytes = _plan_assignment_bytes(payload, "direct")
        relay_plan_bytes = _plan_assignment_bytes(payload, "relay")
        direct_completion_evidence: Mapping[str, object] | None = None
        if direct_plan_bytes and relay_plan_bytes:
            direct_bytes_completed, direct_completion_evidence = (
                execute_direct_ticket_plan(
                    backend=self.backend,
                    runtime_options=self.runtime_options,
                    intent=intent,
                    planned_payload=payload,
                    source=source,
                    target=target,
                )
            )
            if int(direct_bytes_completed) != int(direct_plan_bytes):
                raise RuntimeError(
                    "direct mixed-pooled execution completed "
                    f"{direct_bytes_completed} of {direct_plan_bytes} daemon-planned bytes"
                )
            running = daemon_client.transfer_status(
                str(payload["transfer_id"]),
                state="running",
                bytes_completed=int(direct_bytes_completed),
                completion_source="backend",
                completion_evidence=direct_completion_evidence,
            )
            require_ok(running, "daemon mixed-pooled direct progress update failed")
        lease_tokens = _payload_lease_tokens(payload)
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
                expected_bytes=(
                    int(relay_plan_bytes)
                    if int(direct_plan_bytes) > 0 and int(relay_plan_bytes) > 0
                    else int(intent.total_bytes)
                ),
                report_terminal_status=not (
                    int(direct_plan_bytes) > 0 and int(relay_plan_bytes) > 0
                ),
            )
        except WorkerCompletionEnvelopeError:
            cleanup_planned_relay_leases(
                daemon_client,
                lease_tokens,
                reason="worker_completion_invalid",
                strict=False,
            )
            _mark_mixed_transfer_failed(
                daemon_client,
                payload,
                error="mixed pooled worker completion invalid",
                completion_evidence=direct_completion_evidence,
            )
            raise
        except Exception as exc:
            cleanup_planned_relay_leases(
                daemon_client,
                lease_tokens,
                reason="worker_execution_exception",
                strict=False,
            )
            _mark_mixed_transfer_failed(
                daemon_client,
                payload,
                error=str(exc) or exc.__class__.__name__,
                completion_evidence=direct_completion_evidence,
            )
            raise
        if worker_execution.final_state == "authorization_failed":
            cleanup_planned_relay_leases(
                daemon_client,
                lease_tokens,
                reason="worker_authorization_failed",
                strict=False,
            )
            raise RuntimeError(
                worker_execution.error or "worker authorization failed"
            )
        if worker_execution.final_state == "parse_failed":
            cleanup_planned_relay_leases(
                daemon_client,
                lease_tokens,
                reason="worker_parse_failed",
                strict=False,
            )
            raise RuntimeError(
                worker_execution.error or "worker transfer parse failed"
            )
        if worker_execution.final_state == "cleanup_failed":
            completion = worker_execution.completion
            if (
                completion is not None
                and (
                    completion.worker_result is not None
                    or completion.daemon_status_update is not None
                )
            ):
                return wait_for_intent_receipt(daemon_client, intent.intent_id)
            cleanup_planned_relay_leases(
                daemon_client,
                lease_tokens,
                reason="worker_cleanup_failed",
                strict=False,
            )
            raise RuntimeError(
                worker_execution.error or "worker cleanup failed"
            )
        if worker_execution.final_state in {"failed", "status_failed"}:
            return wait_for_intent_receipt(daemon_client, intent.intent_id)
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
        if int(direct_plan_bytes) > 0 and int(relay_plan_bytes) > 0:
            worker_evidence = _worker_completion_evidence(worker_execution.completion)
            completion = daemon_client.transfer_status(
                str(payload["transfer_id"]),
                state="complete",
                bytes_completed=int(intent.total_bytes),
                completion_source="worker",
                completion_evidence=_merge_mixed_completion_evidence(
                    direct_completion_evidence,
                    worker_evidence,
                    expected_bytes=int(intent.total_bytes),
                    direct_bytes=int(direct_plan_bytes),
                    relay_bytes=int(relay_plan_bytes),
                ),
            )
            require_ok(completion, "daemon mixed-pooled completion update failed")
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


def _intent_execution_admission_state(payload: Mapping[str, object]) -> str:
    admission = payload.get("admission")
    if not isinstance(admission, Mapping):
        return "admitted"
    state = str(admission.get("state", "")).lower()
    return state or "admitted"


def _payload_lease_tokens(
    payload: Mapping[str, object],
) -> tuple[Mapping[str, object], ...]:
    lease_tokens = payload.get("lease_tokens") or ()
    return tuple(dict(lease_token) for lease_token in lease_tokens)


def _admitted_intent_execution_payload(
    *,
    daemon_client,
    intent: TransferIntent,
    response: DaemonResponse,
    timeout_seconds: float,
    interval_seconds: float,
) -> dict[str, object]:
    payload = _intent_execution_payload(response.payload)
    if _intent_execution_admission_state(payload) != "delayed":
        return payload
    submitter = getattr(daemon_client, "submit_transfer_intent", None)
    if not callable(submitter):
        return payload
    deadline = time.time() + max(0.0, float(timeout_seconds))
    interval = max(0.001, float(interval_seconds))
    while time.time() < deadline:
        time.sleep(min(interval, max(0.0, deadline - time.time())))
        retry_response = submitter(intent)
        require_ok(retry_response, "daemon transfer intent admission retry failed")
        payload = _intent_execution_payload(retry_response.payload)
        if _intent_execution_admission_state(payload) != "delayed":
            return payload
    return payload


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


def _plan_assignment_bytes(
    payload: Mapping[str, object],
    path_kind: str,
) -> int:
    plan = payload.get("plan")
    if not isinstance(plan, Mapping):
        return 0
    total = 0
    for assignment in plan.get("assignments", ()) or ():
        if not isinstance(assignment, Mapping):
            continue
        path = assignment.get("path")
        if not isinstance(path, Mapping):
            continue
        if str(path.get("kind", "")).lower() != str(path_kind).lower():
            continue
        assignment_bytes = assignment.get("bytes")
        if assignment_bytes is not None:
            total += int(assignment_bytes)
            continue
        for chunk in assignment.get("chunks", ()) or ():
            if isinstance(chunk, Mapping):
                total += int(chunk.get("bytes", 0))
    return total


def _plan_assignment_chunks(
    payload: Mapping[str, object],
    path_kind: str,
) -> int:
    plan = payload.get("plan")
    if not isinstance(plan, Mapping):
        return 0
    total = 0
    for assignment in plan.get("assignments", ()) or ():
        if not isinstance(assignment, Mapping):
            continue
        path = assignment.get("path")
        if not isinstance(path, Mapping):
            continue
        if str(path.get("kind", "")).lower() != str(path_kind).lower():
            continue
        chunks = assignment.get("chunks", ()) or ()
        total += len(chunks) if not isinstance(chunks, (str, bytes)) else 0
    return total


def _worker_completion_evidence(
    completion: WorkerDataPlaneCompletionEnvelope | None,
) -> Mapping[str, object]:
    if completion is None or completion.worker_result is None:
        raise RuntimeError("mixed-pooled worker completion missing worker result")
    metadata = completion.worker_result.get("metadata")
    if isinstance(metadata, Mapping):
        nested = metadata.get("completion_evidence")
        if isinstance(nested, Mapping):
            merged = dict(nested)
            for key, value in metadata.items():
                merged.setdefault(str(key), value)
            return merged
        return dict(metadata)
    raise RuntimeError("mixed-pooled worker completion missing evidence")


def _merge_mixed_completion_evidence(
    direct_evidence: Mapping[str, object] | None,
    worker_evidence: Mapping[str, object],
    *,
    expected_bytes: int,
    direct_bytes: int,
    relay_bytes: int,
) -> dict[str, object]:
    if not isinstance(direct_evidence, Mapping):
        raise RuntimeError("mixed-pooled direct completion missing evidence")
    expected = int(expected_bytes)
    direct = int(direct_bytes)
    relay = int(relay_bytes)
    if direct + relay != expected:
        raise RuntimeError(
            f"mixed-pooled byte split mismatch: {direct} + {relay} != {expected}"
        )
    direct_resource = direct_evidence.get("resource_evidence")
    worker_resource = worker_evidence.get("resource_evidence")
    evidence = {
        "verified_bytes": expected,
        "expected_bytes": expected,
        "content_match": bool(direct_evidence.get("content_match", False))
        and bool(worker_evidence.get("content_match", False)),
        "verification_source": "mixed_pooled_worker_backend",
        "verification_method": "direct_and_relay_completion",
        "executor": "mixed_worker_backend",
        "plan_source": "daemon",
        "path": "mixed_pooled",
        "direct_bytes": direct,
        "direct_chunks": int(direct_evidence.get("direct_chunks", 0)),
        "relay_bytes": relay,
        "relay_chunks": int(worker_evidence.get("relay_chunks", 0)),
        "target_device": int(
            worker_evidence.get(
                "target_device",
                direct_evidence.get("target_device", -1),
            )
        ),
        "resource_evidence": {
            "direct": dict(direct_resource) if isinstance(direct_resource, Mapping) else {},
            "relay": dict(worker_resource) if isinstance(worker_resource, Mapping) else {},
        },
        "direct_completion_evidence": dict(direct_evidence),
        "relay_completion_evidence": dict(worker_evidence),
    }
    if worker_evidence.get("relay_gpu") is not None:
        evidence["relay_gpu"] = int(worker_evidence["relay_gpu"])
    if worker_evidence.get("relay_gpus") is not None:
        evidence["relay_gpus"] = tuple(int(item) for item in worker_evidence["relay_gpus"])
    for key in ("ticket_id", "transfer_id", "plan_generation"):
        if key in worker_evidence:
            evidence[key] = worker_evidence[key]
        elif key in direct_evidence:
            evidence[key] = direct_evidence[key]
    return evidence


def _mark_mixed_transfer_failed(
    daemon_client,
    payload: Mapping[str, object],
    *,
    error: str,
    completion_evidence: Mapping[str, object] | None,
) -> None:
    if not isinstance(completion_evidence, Mapping):
        return
    transfer_id = payload.get("transfer_id")
    if transfer_id is None:
        return
    daemon_client.transfer_status(
        str(transfer_id),
        state="failed",
        bytes_completed=int(completion_evidence.get("direct_bytes", 0) or 0),
        error=error,
        completion_source="backend",
        completion_evidence=completion_evidence,
    )


__all__ = ["WorkerIntentTransferExecutor"]
