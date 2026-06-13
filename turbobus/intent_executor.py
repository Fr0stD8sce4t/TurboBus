from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
import os
import time
from threading import Lock

from .backends.cuda import default_cuda_backend
from .buffer_registration import ExecutableBuffer
from .client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer
from .direct_fallback import (
    execute_direct_fallback_transfer,
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
    require_ok,
    require_worker_plan_matches_leases,
    submit_worker_execution,
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
    worker_client: object | None
    backend: object = default_cuda_backend
    runtime_options: RuntimeOptions = field(default_factory=RuntimeOptions)
    _direct_runtime_cache: OrderedDict[tuple[object, ...], object] = field(
        default_factory=OrderedDict,
        init=False,
        repr=False,
    )
    _direct_runtime_cache_evictions: int = field(default=0, init=False, repr=False)
    _direct_runtime_cache_eviction_keys: OrderedDict[tuple[object, ...], None] = field(
        default_factory=OrderedDict,
        init=False,
        repr=False,
    )
    _direct_runtime_cache_lock: Lock = field(
        default_factory=Lock,
        init=False,
        repr=False,
    )
    _direct_runtime_key_locks: dict[tuple[object, ...], Lock] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _direct_runtime_key_lock_users: dict[tuple[object, ...], int] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _direct_runtime_max_key_lock_count: int = field(default=0, init=False, repr=False)
    _direct_runtime_max_key_waiter_count: int = field(default=0, init=False, repr=False)

    def execute_transfer_intent(
        self,
        intent: TransferIntent,
        response: DaemonResponse,
        daemon_client,
    ) -> TransferReceipt:
        if not isinstance(intent, TransferIntent):
            raise TypeError("intent must be a TransferIntent")
        _trace_runtime_stage(
            "intent_executor_start",
            intent_id=intent.intent_id,
            bytes=intent.total_bytes,
            direction=intent.direction,
        )
        require_ok(response, "daemon transfer intent submission failed")
        source, target = _intent_buffers(self.buffers, intent)
        _trace_runtime_stage("intent_executor_admission_start", intent_id=intent.intent_id)
        payload = _admitted_intent_execution_payload(
            daemon_client=daemon_client,
            intent=intent,
            response=response,
            timeout_seconds=self.runtime_options.admission_retry_timeout_seconds,
            interval_seconds=self.runtime_options.admission_retry_interval_seconds,
        )
        _trace_runtime_stage(
            "intent_executor_admission_done",
            intent_id=intent.intent_id,
            transfer_id=payload.get("transfer_id"),
        )
        admission_error = _intent_execution_admission_error(payload)
        if admission_error is not None:
            raise RuntimeError(admission_error)
        if is_direct_only_worker_plan(payload):
            return self._execute_direct_only_intent(
                intent,
                payload,
                daemon_client,
                source=source,
                target=target,
            )
        return self._execute_worker_managed_intent(
            intent,
            payload,
            daemon_client,
        )

    def _execute_direct_only_intent(
        self,
        intent: TransferIntent,
        payload: Mapping[str, object],
        daemon_client,
        *,
        source: ExecutableBuffer,
        target: ExecutableBuffer,
    ) -> TransferReceipt:
        _trace_runtime_stage(
            "intent_executor_direct_only_start",
            intent_id=intent.intent_id,
        )
        execute_direct_fallback_transfer(
            daemon_client=daemon_client,
            backend=self.backend,
            runtime_options=self.runtime_options,
            runtime_provider=lambda: self._direct_runtime_for_intent(intent),
            runtime_cache_evidence_provider=self._direct_runtime_cache_evidence,
            intent=intent,
            planned_payload=payload,
            source=source,
            target=target,
            result_factory=WorkerIntentTransferResult,
        )
        _trace_runtime_stage(
            "intent_executor_direct_only_done",
            intent_id=intent.intent_id,
        )
        return _receipt_from_status_query(daemon_client, intent.intent_id)

    def _direct_runtime_for_intent(self, intent: TransferIntent) -> tuple[object, bool]:
        target_device = _direct_target_device_for_intent(
            intent,
            self.buffers,
        )
        key = _direct_runtime_cache_key(
            self.runtime_options,
            target_device=target_device,
        )
        with self._direct_runtime_cache_lock:
            runtime = self._direct_runtime_cache.get(key)
            if runtime is not None:
                self._direct_runtime_cache.move_to_end(key)
                return runtime, True
            key_lock = self._acquire_direct_runtime_key_lock_locked(key)
        try:
            with key_lock:
                with self._direct_runtime_cache_lock:
                    runtime = self._direct_runtime_cache.get(key)
                    if runtime is not None:
                        self._direct_runtime_cache.move_to_end(key)
                        return runtime, True
                runtime = self.backend.create_runtime(self.runtime_options)
                self.backend.initialize_runtime(runtime, int(target_device), [])
                if int(self.runtime_options.worker_runtime_cache_entries) == 0:
                    return runtime, False
                with self._direct_runtime_cache_lock:
                    current = self._direct_runtime_cache.get(key)
                    if current is not None:
                        self._direct_runtime_cache.move_to_end(key)
                        return current, True
                    self._direct_runtime_cache[key] = runtime
                    self._trim_direct_runtime_cache_locked(protected_key=key)
                return runtime, False
        finally:
            with self._direct_runtime_cache_lock:
                self._release_direct_runtime_key_lock_locked(key)

    def _acquire_direct_runtime_key_lock_locked(self, key: tuple[object, ...]) -> Lock:
        key_lock = self._direct_runtime_key_locks.setdefault(key, Lock())
        self._direct_runtime_key_lock_users[key] = (
            int(self._direct_runtime_key_lock_users.get(key, 0)) + 1
        )
        self._direct_runtime_max_key_lock_count = max(
            int(self._direct_runtime_max_key_lock_count),
            len(self._direct_runtime_key_locks),
        )
        self._direct_runtime_max_key_waiter_count = max(
            int(self._direct_runtime_max_key_waiter_count),
            _direct_runtime_key_waiter_count(self._direct_runtime_key_lock_users),
        )
        return key_lock

    def _release_direct_runtime_key_lock_locked(self, key: tuple[object, ...]) -> None:
        users = int(self._direct_runtime_key_lock_users.get(key, 0)) - 1
        if users > 0:
            self._direct_runtime_key_lock_users[key] = users
            return
        self._direct_runtime_key_lock_users.pop(key, None)
        self._direct_runtime_key_locks.pop(key, None)

    def _trim_direct_runtime_cache_locked(
        self,
        *,
        protected_key: tuple[object, ...],
    ) -> None:
        limit = int(self.runtime_options.worker_runtime_cache_entries)
        if limit < 0:
            return
        protected_key = tuple(protected_key)
        while len(self._direct_runtime_cache) > limit:
            for key in tuple(self._direct_runtime_cache):
                if tuple(key) == protected_key:
                    continue
                self._direct_runtime_cache.pop(key, None)
                self._direct_runtime_cache_evictions += 1
                self._record_direct_runtime_cache_eviction_locked(tuple(key))
                break
            else:
                break

    def _record_direct_runtime_cache_eviction_locked(
        self,
        key: tuple[object, ...],
    ) -> None:
        self._direct_runtime_cache_eviction_keys[tuple(key)] = None
        self._direct_runtime_cache_eviction_keys.move_to_end(tuple(key))
        while len(self._direct_runtime_cache_eviction_keys) > 8:
            self._direct_runtime_cache_eviction_keys.popitem(last=False)

    def _direct_runtime_cache_evidence(self) -> dict[str, object]:
        with self._direct_runtime_cache_lock:
            return self._direct_runtime_cache_snapshot_locked()

    def _direct_runtime_cache_snapshot_locked(
        self,
        *,
        include_cache_records: bool = False,
    ) -> dict[str, object]:
        cache_size = len(self._direct_runtime_cache)
        cache_limit = int(self.runtime_options.worker_runtime_cache_entries)
        cache_over_limit = 0 if cache_limit < 0 else max(0, cache_size - cache_limit)
        snapshot = {
            "cache_size": cache_size,
            "cache_limit": cache_limit,
            "cache_over_limit": cache_over_limit,
            "cache_evictions": int(self._direct_runtime_cache_evictions),
            "cache_eviction_records": tuple(
                _direct_runtime_cache_record_from_key(key)
                for key in self._direct_runtime_cache_eviction_keys
            ),
            "key_lock_count": len(self._direct_runtime_key_locks),
            "key_waiter_count": _direct_runtime_key_waiter_count(
                self._direct_runtime_key_lock_users
            ),
            "max_key_lock_count": int(self._direct_runtime_max_key_lock_count),
            "max_key_waiter_count": int(self._direct_runtime_max_key_waiter_count),
        }
        if include_cache_records:
            snapshot["cache_records"] = tuple(
                _direct_runtime_cache_record_from_key(key)
                for key in self._direct_runtime_cache
            )
        return snapshot

    def close_direct_runtime_cache(self) -> dict[str, object]:
        with self._direct_runtime_cache_lock:
            snapshot = self._direct_runtime_cache_snapshot_locked(
                include_cache_records=True,
            )
            self._direct_runtime_cache.clear()
            self._direct_runtime_key_locks.clear()
            self._direct_runtime_key_lock_users.clear()
        return {
            "source": "runtime_session_direct_backend_cache",
            "closed": True,
            "runtime_count": int(snapshot["cache_size"]),
            "runtime_cache_limit": int(snapshot["cache_limit"]),
            "runtime_cache_over_limit": int(snapshot["cache_over_limit"]),
            "runtime_cache_evictions": int(snapshot["cache_evictions"]),
            "runtime_cache_eviction_records": snapshot["cache_eviction_records"],
            "runtime_key_lock_count": int(snapshot["key_lock_count"]),
            "runtime_key_waiter_count": int(snapshot["key_waiter_count"]),
            "max_runtime_key_lock_count": int(snapshot["max_key_lock_count"]),
            "max_runtime_key_waiter_count": int(snapshot["max_key_waiter_count"]),
            "runtime_cache_records": snapshot.get("cache_records", ()),
        }

    def _execute_worker_managed_intent(
        self,
        intent: TransferIntent,
        payload: Mapping[str, object],
        daemon_client,
    ) -> TransferReceipt:
        direct_plan_bytes = _plan_assignment_bytes(payload, "direct")
        relay_plan_bytes = _plan_assignment_bytes(payload, "relay")
        direct_completion_evidence: Mapping[str, object] | None = None
        mixed_mode = int(direct_plan_bytes) > 0 and int(relay_plan_bytes) > 0
        relay_only_mode = int(direct_plan_bytes) == 0 and int(relay_plan_bytes) > 0
        lease_tokens = _payload_lease_tokens(payload)
        if not lease_tokens:
            _fail_transfer_without_relay_leases(
                daemon_client=daemon_client,
                intent=intent,
                payload=payload,
                direct_completion_evidence=direct_completion_evidence,
                direct_bytes_completed=int(direct_plan_bytes),
            )
            return _receipt_from_status_query(daemon_client, intent.intent_id)
        if self.worker_client is None:
            _fail_transfer_without_worker_client(
                daemon_client=daemon_client,
                intent=intent,
                payload=payload,
                lease_tokens=lease_tokens,
                direct_completion_evidence=direct_completion_evidence,
                direct_bytes_completed=int(direct_plan_bytes),
            )
            return _receipt_from_status_query(daemon_client, intent.intent_id)
        _validate_intent_lease_tokens(daemon_client, intent, lease_tokens)
        worker_execution = self._submit_worker_managed_execution(
            intent,
            payload,
            lease_tokens,
            daemon_client,
            report_terminal_status=not (mixed_mode or relay_only_mode),
            direct_completion_evidence=direct_completion_evidence,
        )
        terminal_receipt = self._receipt_for_worker_terminal_state(
            intent,
            payload,
            daemon_client,
            worker_execution=worker_execution,
            lease_tokens=lease_tokens,
            direct_completion_evidence=direct_completion_evidence,
            direct_plan_bytes=int(direct_plan_bytes),
            relay_only_mode=relay_only_mode,
            mixed_mode=mixed_mode,
        )
        if terminal_receipt is not None:
            return terminal_receipt
        self._report_worker_completion(
            intent,
            payload,
            daemon_client,
            worker_execution=worker_execution,
            direct_completion_evidence=direct_completion_evidence,
            direct_plan_bytes=int(direct_plan_bytes),
            relay_plan_bytes=int(relay_plan_bytes),
            relay_only_mode=relay_only_mode,
            mixed_mode=mixed_mode,
        )
        return _receipt_from_status_query(daemon_client, intent.intent_id)

    def _submit_worker_managed_execution(
        self,
        intent: TransferIntent,
        payload: Mapping[str, object],
        lease_tokens: tuple[Mapping[str, object], ...],
        daemon_client,
        *,
        report_terminal_status: bool,
        direct_completion_evidence: Mapping[str, object] | None,
    ):
        primary_lease_token = lease_tokens[0]
        try:
            require_worker_plan_matches_leases(
                payload,
                lease_tokens,
                direction=intent.direction,
            )
            authorization_request = _worker_authorization_request(
                intent,
                payload,
                primary_lease_token,
            )
            _trace_runtime_stage(
                "intent_executor_worker_submit_start",
                intent_id=intent.intent_id,
                transfer_id=payload["transfer_id"],
                relay_gpu=primary_lease_token["relay_gpu"],
            )
            worker_execution = submit_worker_execution(
                self.worker_client,
                authorization_request,
                expected_bytes=int(intent.total_bytes),
                report_terminal_status=report_terminal_status,
            )
            _trace_runtime_stage(
                "intent_executor_worker_submit_done",
                intent_id=intent.intent_id,
                final_state=worker_execution.final_state,
            )
            return worker_execution
        except WorkerCompletionEnvelopeError:
            _fail_worker_execution_exception(
                daemon_client=daemon_client,
                payload=payload,
                lease_tokens=lease_tokens,
                cleanup_reason="worker_completion_invalid",
                error="mixed pooled worker completion invalid",
                direct_completion_evidence=direct_completion_evidence,
            )
            raise
        except Exception as exc:
            _fail_worker_execution_exception(
                daemon_client=daemon_client,
                payload=payload,
                lease_tokens=lease_tokens,
                cleanup_reason="worker_execution_exception",
                error=str(exc) or exc.__class__.__name__,
                direct_completion_evidence=direct_completion_evidence,
            )
            raise

    def _receipt_for_worker_terminal_state(
        self,
        intent: TransferIntent,
        payload: Mapping[str, object],
        daemon_client,
        *,
        worker_execution,
        lease_tokens: tuple[Mapping[str, object], ...],
        direct_completion_evidence: Mapping[str, object] | None,
        direct_plan_bytes: int,
        relay_only_mode: bool,
        mixed_mode: bool,
    ) -> TransferReceipt | None:
        if worker_execution.final_state == "complete":
            return None
        if worker_execution.final_state == "authorization_failed":
            _fail_worker_terminal_state(
                daemon_client=daemon_client,
                payload=payload,
                lease_tokens=lease_tokens,
                cleanup_reason="worker_authorization_failed",
                error=worker_execution.error or "worker authorization failed",
                direct_completion_evidence=direct_completion_evidence,
            )
            raise RuntimeError(worker_execution.error or "worker authorization failed")
        if worker_execution.final_state == "parse_failed":
            _fail_worker_terminal_state(
                daemon_client=daemon_client,
                payload=payload,
                lease_tokens=lease_tokens,
                cleanup_reason="worker_parse_failed",
                error=worker_execution.error or "worker transfer parse failed",
                direct_completion_evidence=direct_completion_evidence,
            )
            raise RuntimeError(worker_execution.error or "worker transfer parse failed")
        if worker_execution.final_state == "cleanup_failed":
            if _worker_cleanup_failure_has_status(worker_execution):
                return _receipt_from_status_query(daemon_client, intent.intent_id)
            _fail_worker_terminal_state(
                daemon_client=daemon_client,
                payload=payload,
                lease_tokens=lease_tokens,
                cleanup_reason="worker_cleanup_failed",
                error=worker_execution.error or "worker cleanup failed",
                direct_completion_evidence=direct_completion_evidence,
            )
            raise RuntimeError(worker_execution.error or "worker cleanup failed")
        if worker_execution.final_state in {"failed", "status_failed"}:
            if relay_only_mode or mixed_mode:
                _report_deferred_worker_failure(
                    daemon_client=daemon_client,
                    payload=payload,
                    intent=intent,
                    worker_execution=worker_execution,
                    direct_completion_evidence=direct_completion_evidence,
                    direct_bytes_completed=int(direct_plan_bytes),
                )
            return _receipt_from_status_query(daemon_client, intent.intent_id)
        _fail_worker_terminal_state(
            daemon_client=daemon_client,
            payload=payload,
            lease_tokens=lease_tokens,
            cleanup_reason="worker_completion_not_complete",
            error=worker_execution.error
            or "worker-managed intent transfer did not complete",
            direct_completion_evidence=direct_completion_evidence,
        )
        raise RuntimeError(
            worker_execution.error or "worker-managed intent transfer did not complete"
        )

    def _report_worker_completion(
        self,
        intent: TransferIntent,
        payload: Mapping[str, object],
        daemon_client,
        *,
        worker_execution,
        direct_completion_evidence: Mapping[str, object] | None,
        direct_plan_bytes: int,
        relay_plan_bytes: int,
        relay_only_mode: bool,
        mixed_mode: bool,
    ) -> None:
        if relay_only_mode:
            worker_evidence = _worker_completion_evidence(worker_execution.completion)
            completion = daemon_client.transfer_status(
                str(payload["transfer_id"]),
                state="complete",
                bytes_completed=int(intent.total_bytes),
                completion_source="worker",
                completion_evidence=_relay_only_completion_evidence(
                    worker_evidence,
                    expected_bytes=int(intent.total_bytes),
                ),
            )
            require_ok(completion, "daemon relay-only completion update failed")
        elif mixed_mode:
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
        elif not mixed_mode:
            raise RuntimeError("worker-managed transfer has no relay path")


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


def _direct_target_device_for_intent(
    intent: TransferIntent,
    buffers: Mapping[str, ExecutableBuffer],
) -> int:
    source, target = _intent_buffers(buffers, intent)
    if str(intent.direction).lower() == "h2d":
        if not isinstance(target, CudaIpcDeviceBuffer):
            raise TypeError("direct h2d target must be a CUDA device buffer")
        return int(target.device_index)
    if not isinstance(source, CudaIpcDeviceBuffer):
        raise TypeError("direct d2h source must be a CUDA device buffer")
    return int(source.device_index)


def _direct_runtime_cache_key(
    runtime_options: RuntimeOptions,
    *,
    target_device: int,
) -> tuple[object, ...]:
    return (
        int(target_device),
        int(runtime_options.chunk_bytes),
        int(runtime_options.staging_slots),
        bool(runtime_options.enable_peer_access),
        int(runtime_options.profile_bytes),
        bool(runtime_options.profile_on_first_transfer),
        bool(runtime_options.profile_cache_enabled),
        int(runtime_options.min_chunks_for_relay),
        int(runtime_options.min_pool_bytes),
        float(runtime_options.relay_min_effective_bw_gbps),
        float(runtime_options.relay_min_direct_ratio),
        bool(runtime_options.enable_dynamic_weights),
        float(runtime_options.dynamic_weight_alpha),
        bool(runtime_options.clear_relay_staging_on_chunk),
    )


def _direct_runtime_cache_record_from_key(
    key: tuple[object, ...],
) -> dict[str, object]:
    if len(key) < 14:
        return {"source": "runtime_session_direct_backend_cache", "key_width": len(key)}
    return {
        "source": "runtime_session_direct_backend_cache",
        "target_device": int(key[0]),
        "chunk_bytes": int(key[1]),
        "staging_slots": int(key[2]),
        "peer_access_enabled": bool(key[3]),
        "profile_bytes": int(key[4]),
        "profile_cache_enabled": bool(key[6]),
        "dynamic_weights_enabled": bool(key[11]),
        "clear_relay_staging_on_chunk": bool(key[13]),
    }


def _direct_runtime_key_waiter_count(
    key_lock_users: Mapping[tuple[object, ...], int],
) -> int:
    return sum(max(0, int(value) - 1) for value in key_lock_users.values())


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


def _worker_authorization_request(
    intent: TransferIntent,
    payload: Mapping[str, object],
    primary_lease_token: Mapping[str, object],
) -> WorkerTransferAuthorizationRequest:
    return WorkerTransferAuthorizationRequest(
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


def _worker_cleanup_failure_has_status(worker_execution) -> bool:
    completion = worker_execution.completion
    return bool(
        completion is not None
        and (
            completion.worker_result is not None
            or completion.daemon_status_update is not None
        )
    )


def _fail_worker_execution_exception(
    *,
    daemon_client,
    payload: Mapping[str, object],
    lease_tokens: Iterable[Mapping[str, object]],
    cleanup_reason: str,
    error: str,
    direct_completion_evidence: Mapping[str, object] | None,
) -> None:
    cleanup_evidence = cleanup_planned_relay_leases(
        daemon_client,
        lease_tokens,
        reason=cleanup_reason,
        strict=False,
    )
    _mark_transfer_failed(
        daemon_client,
        payload,
        error=error,
        completion_evidence=direct_completion_evidence,
        cleanup_evidence=cleanup_evidence,
    )


def _fail_worker_terminal_state(
    *,
    daemon_client,
    payload: Mapping[str, object],
    lease_tokens: Iterable[Mapping[str, object]],
    cleanup_reason: str,
    error: str,
    direct_completion_evidence: Mapping[str, object] | None,
) -> None:
    cleanup_evidence = cleanup_planned_relay_leases(
        daemon_client,
        lease_tokens,
        reason=cleanup_reason,
        strict=False,
    )
    _mark_transfer_failed(
        daemon_client,
        payload,
        error=error,
        completion_evidence=direct_completion_evidence,
        cleanup_evidence=cleanup_evidence,
    )


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
            evidence = merged
        else:
            evidence = dict(metadata)
        return _worker_envelope_evidence(evidence, completion)
    raise RuntimeError("mixed-pooled worker completion missing evidence")


def _worker_envelope_evidence(
    evidence: Mapping[str, object],
    completion: WorkerDataPlaneCompletionEnvelope,
) -> dict[str, object]:
    merged = dict(evidence)
    resource_evidence = dict(merged.get("resource_evidence") or {})
    staging_slot = (
        dict(completion.staging_slot)
        if isinstance(completion.staging_slot, Mapping)
        else None
    )
    staging_release = (
        dict(completion.staging_release)
        if isinstance(completion.staging_release, Mapping)
        else None
    )
    cleanup_evidence = _worker_cleanup_evidence_from_completion(completion)
    running_update = (
        dict(completion.daemon_running_update)
        if isinstance(completion.daemon_running_update, Mapping)
        else None
    )
    worker_result = (
        completion.worker_result
        if isinstance(completion.worker_result, Mapping)
        else None
    )
    if staging_slot is not None:
        merged.setdefault("staging_slot_id", str(staging_slot.get("slot_id")))
        resource_evidence["staging_slot"] = staging_slot
    if staging_release is not None:
        resource_evidence["staging_release"] = staging_release
    if cleanup_evidence is not None:
        merged["cleanup"] = cleanup_evidence
        resource_evidence["cleanup"] = dict(cleanup_evidence)
    if running_update is not None:
        merged["daemon_running_update"] = running_update
    if worker_result is not None:
        merged.setdefault(
            "worker_bytes_completed",
            int(worker_result.get("bytes_completed", 0) or 0),
        )
        worker_state = worker_result.get("state")
        if worker_state is not None:
            merged.setdefault("worker_state", str(worker_state))
    if resource_evidence:
        merged["resource_evidence"] = resource_evidence
    return merged


def _worker_cleanup_evidence_from_completion(
    completion: WorkerDataPlaneCompletionEnvelope,
) -> dict[str, object] | None:
    response = completion.daemon_cleanup_response
    if not isinstance(response, Mapping):
        return None
    payload = response.get("payload")
    if not isinstance(payload, Mapping):
        payload = {}
    return {
        "ok": bool(response.get("ok", False)),
        "target_kind": payload.get("cleanup_kind"),
        "target_id": payload.get("reservation_id"),
        "mode": payload.get("cleanup_mode"),
        "reason": payload.get("reason"),
        "lease_ids": tuple(str(item) for item in payload.get("lease_ids", ()) or ()),
        "cleanup_scope_target_ids": tuple(
            str(item)
            for item in payload.get("cleanup_scope_target_ids", ()) or ()
        ),
        "cleaned_reservation_ids": tuple(
            str(item) for item in payload.get("cleaned_reservation_ids", ()) or ()
        ),
        **(
            {}
            if not isinstance(payload.get("owner_binding"), Mapping)
            else {"owner_binding": dict(payload["owner_binding"])}
        ),
    }


def _relay_only_completion_evidence(
    worker_evidence: Mapping[str, object],
    *,
    expected_bytes: int,
) -> dict[str, object]:
    verified_bytes = int(worker_evidence.get("verified_bytes", expected_bytes) or 0)
    if verified_bytes != int(expected_bytes):
        raise RuntimeError(
            "relay-only worker completion verified "
            f"{verified_bytes} of {expected_bytes} daemon-planned bytes"
        )
    evidence = dict(worker_evidence)
    evidence["verified_bytes"] = int(expected_bytes)
    evidence["expected_bytes"] = int(expected_bytes)
    evidence.setdefault("content_match", True)
    evidence.setdefault("verification_source", "relay_worker")
    evidence.setdefault("verification_method", "worker_completion")
    evidence.setdefault("executor", "relay_worker")
    evidence.setdefault("plan_source", "daemon")
    evidence.setdefault("path", "relay_only")
    evidence.setdefault("relay_bytes", int(expected_bytes))
    evidence.setdefault("direct_bytes", 0)
    evidence.setdefault("direct_chunks", 0)
    cleanup = _cleanup_evidence_from_mapping(worker_evidence)
    if cleanup is not None:
        evidence["cleanup"] = cleanup
    _copy_worker_lifecycle_evidence(evidence, worker_evidence)
    _copy_path_level_evidence(evidence, worker_evidence)
    evidence["worker_completion_evidence"] = dict(worker_evidence)
    evidence["relay_completion_evidence"] = dict(worker_evidence)
    evidence["execution_path_evidence"] = _execution_path_evidence(
        evidence,
        expected_bytes=int(expected_bytes),
    )
    return evidence


def _merge_mixed_completion_evidence(
    direct_evidence: Mapping[str, object] | None,
    worker_evidence: Mapping[str, object],
    *,
    expected_bytes: int,
    direct_bytes: int,
    relay_bytes: int,
) -> dict[str, object]:
    expected = int(expected_bytes)
    direct = int(direct_bytes)
    relay = int(relay_bytes)
    if direct + relay != expected:
        raise RuntimeError(
            f"mixed-pooled byte split mismatch: {direct} + {relay} != {expected}"
        )
    worker_direct_bytes = int(worker_evidence.get("direct_bytes", direct) or 0)
    worker_relay_bytes = int(worker_evidence.get("relay_bytes", relay) or 0)
    worker_verified_bytes = int(worker_evidence.get("verified_bytes", expected) or 0)
    if worker_direct_bytes != direct or worker_relay_bytes != relay:
        raise RuntimeError("mixed-pooled worker path split does not match daemon plan")
    if worker_verified_bytes != expected:
        raise RuntimeError(
            "mixed-pooled worker completion verified "
            f"{worker_verified_bytes} of {expected} daemon-planned bytes"
        )
    direct_resource = (
        None
        if not isinstance(direct_evidence, Mapping)
        else direct_evidence.get("resource_evidence")
    )
    worker_resource = worker_evidence.get("resource_evidence")
    evidence = {
        "verified_bytes": expected,
        "expected_bytes": expected,
        "content_match": bool(worker_evidence.get("content_match", False)),
        "verification_source": "mixed_pooled_worker_backend",
        "verification_method": "unified_worker_backend_completion",
        "executor": "mixed_worker_backend",
        "plan_source": "daemon",
        "path": "mixed_pooled",
        "direct_bytes": direct,
        "direct_chunks": int(worker_evidence.get("direct_chunks", 0)),
        "relay_bytes": relay,
        "relay_chunks": int(worker_evidence.get("relay_chunks", 0)),
        "target_device": int(worker_evidence.get("target_device", -1)),
        "resource_evidence": {
            "direct": dict(direct_resource) if isinstance(direct_resource, Mapping) else {},
            "worker": dict(worker_resource) if isinstance(worker_resource, Mapping) else {},
        },
        "worker_completion_evidence": dict(worker_evidence),
        "relay_completion_evidence": dict(worker_evidence),
    }
    _copy_path_level_evidence(evidence, worker_evidence)
    if isinstance(direct_evidence, Mapping):
        evidence["direct_completion_evidence"] = dict(direct_evidence)
    if worker_evidence.get("relay_gpu") is not None:
        evidence["relay_gpu"] = int(worker_evidence["relay_gpu"])
    if worker_evidence.get("relay_gpus") is not None:
        evidence["relay_gpus"] = tuple(int(item) for item in worker_evidence["relay_gpus"])
    for key in ("ticket_id", "transfer_id", "plan_generation"):
        if key in worker_evidence:
            evidence[key] = worker_evidence[key]
        elif isinstance(direct_evidence, Mapping) and key in direct_evidence:
            evidence[key] = direct_evidence[key]
    cleanup = _cleanup_evidence_from_mapping(worker_evidence)
    if cleanup is not None:
        evidence["cleanup"] = cleanup
    _copy_worker_lifecycle_evidence(evidence, worker_evidence)
    evidence["execution_path_evidence"] = _execution_path_evidence(
        evidence,
        expected_bytes=expected,
    )
    return evidence


def _report_deferred_worker_failure(
    *,
    daemon_client,
    payload: Mapping[str, object],
    intent: TransferIntent,
    worker_execution,
    direct_completion_evidence: Mapping[str, object] | None,
    direct_bytes_completed: int,
) -> None:
    completion = worker_execution.completion
    if completion is None:
        raise RuntimeError("deferred worker failure missing completion envelope")
    worker_evidence = _worker_completion_evidence(completion)
    error = worker_execution.error or "worker-managed intent transfer failed"
    transfer_id = str(payload["transfer_id"])
    plan_direct_bytes = _plan_assignment_bytes(payload, "direct")
    plan_relay_bytes = _plan_assignment_bytes(payload, "relay")
    if int(plan_direct_bytes) > 0 and int(plan_relay_bytes) > 0:
        failure_evidence = _merge_mixed_worker_failure_evidence(
            payload=payload,
            direct_evidence=direct_completion_evidence,
            worker_evidence=worker_evidence,
            expected_bytes=int(intent.total_bytes),
            direct_bytes=int(plan_direct_bytes),
            relay_bytes=int(plan_relay_bytes),
        )
        completion_source = "worker"
        bytes_completed = _worker_reported_bytes(worker_evidence)
    else:
        failure_evidence = _relay_only_failure_evidence(
            worker_evidence,
            expected_bytes=int(intent.total_bytes),
        )
        completion_source = "worker"
        bytes_completed = _worker_reported_bytes(worker_evidence)
    response = daemon_client.transfer_status(
        transfer_id,
        state="failed",
        bytes_completed=max(0, bytes_completed),
        error=error,
        completion_source=completion_source,
        completion_evidence=failure_evidence,
    )
    require_ok(response, "daemon deferred worker failure update failed")


def _relay_only_failure_evidence(
    worker_evidence: Mapping[str, object],
    *,
    expected_bytes: int,
) -> dict[str, object]:
    evidence = dict(worker_evidence)
    evidence.setdefault("expected_bytes", int(expected_bytes))
    evidence["verified_bytes"] = min(
        int(expected_bytes),
        max(0, int(worker_evidence.get("verified_bytes", 0) or 0)),
    )
    evidence.setdefault("content_match", False)
    evidence.setdefault("executor", "relay_worker")
    evidence.setdefault("plan_source", "daemon")
    evidence.setdefault("path", "relay_only")
    evidence.setdefault("relay_bytes", int(expected_bytes))
    evidence.setdefault("direct_bytes", 0)
    evidence.setdefault("direct_chunks", 0)
    evidence["relay_bytes_completed"] = min(
        int(expected_bytes),
        _worker_reported_bytes(worker_evidence),
    )
    evidence.setdefault("failure_source", "relay_worker")
    cleanup = _cleanup_evidence_from_mapping(worker_evidence)
    if cleanup is not None:
        evidence["cleanup"] = cleanup
    _copy_worker_lifecycle_evidence(evidence, worker_evidence)
    evidence["worker_completion_evidence"] = dict(worker_evidence)
    evidence["relay_completion_evidence"] = dict(worker_evidence)
    _copy_path_level_evidence(evidence, worker_evidence)
    evidence["execution_path_evidence"] = _execution_path_evidence(
        evidence,
        expected_bytes=int(expected_bytes),
    )
    return evidence


def _merge_mixed_worker_failure_evidence(
    *,
    payload: Mapping[str, object],
    direct_evidence: Mapping[str, object] | None,
    worker_evidence: Mapping[str, object],
    expected_bytes: int,
    direct_bytes: int,
    relay_bytes: int,
) -> dict[str, object]:
    expected = int(expected_bytes)
    direct = int(direct_bytes)
    relay = int(relay_bytes)
    if direct + relay != expected:
        raise RuntimeError(
            f"mixed-pooled byte split mismatch: {direct} + {relay} != {expected}"
        )
    worker_direct_bytes = int(worker_evidence.get("direct_bytes", direct) or 0)
    worker_relay_bytes = int(worker_evidence.get("relay_bytes", relay) or 0)
    if worker_direct_bytes != direct or worker_relay_bytes != relay:
        raise RuntimeError("mixed-pooled worker path split does not match daemon plan")
    direct_resource = (
        None
        if not isinstance(direct_evidence, Mapping)
        else direct_evidence.get("resource_evidence")
    )
    worker_resource = worker_evidence.get("resource_evidence")
    evidence = {
        "expected_bytes": expected,
        "verified_bytes": min(
            expected,
            max(0, int(worker_evidence.get("verified_bytes", 0) or 0)),
        ),
        "content_match": False,
        "verification_source": "mixed_pooled_partial_worker_backend",
        "verification_method": "unified_worker_backend_incomplete",
        "executor": "mixed_worker_backend",
        "plan_source": "daemon",
        "path": "mixed_pooled",
        "direct_bytes": direct,
        "direct_chunks": int(
            worker_evidence.get(
                "direct_chunks",
                _plan_assignment_chunks(payload, "direct"),
            )
            or 0
        ),
        "relay_bytes": relay,
        "relay_chunks": int(
            worker_evidence.get(
                "relay_chunks",
                _plan_assignment_chunks(payload, "relay"),
            )
            or 0
        ),
        "relay_bytes_completed": min(relay, _worker_reported_bytes(worker_evidence)),
        "target_device": int(worker_evidence.get("target_device", -1)),
        "resource_evidence": {
            "direct": (
                dict(direct_resource)
                if isinstance(direct_resource, Mapping)
                else {}
            ),
            "worker": (
                dict(worker_resource)
                if isinstance(worker_resource, Mapping)
                else {}
            ),
        },
        "worker_completion_evidence": dict(worker_evidence),
        "relay_completion_evidence": dict(worker_evidence),
        "failure_source": "mixed_worker_backend",
    }
    _copy_path_level_evidence(evidence, worker_evidence)
    if isinstance(direct_evidence, Mapping):
        evidence["direct_completion_evidence"] = dict(direct_evidence)
    if worker_evidence.get("relay_gpu") is not None:
        evidence["relay_gpu"] = int(worker_evidence["relay_gpu"])
    if worker_evidence.get("relay_gpus") is not None:
        evidence["relay_gpus"] = tuple(
            int(item) for item in worker_evidence["relay_gpus"]
        )
    for key in (
        "ticket_id",
        "transfer_id",
        "plan_generation",
        "worker_bytes_completed",
        "worker_state",
        "completion_validation",
        "reported_bytes",
    ):
        if key in worker_evidence:
            evidence[key] = worker_evidence[key]
        elif isinstance(direct_evidence, Mapping) and key in direct_evidence:
            evidence[key] = direct_evidence[key]
    cleanup = _cleanup_evidence_from_mapping(worker_evidence)
    if cleanup is not None:
        evidence["cleanup"] = cleanup
    _copy_worker_lifecycle_evidence(evidence, worker_evidence)
    evidence["execution_path_evidence"] = _execution_path_evidence(
        evidence,
        expected_bytes=expected,
    )
    return evidence


def _mark_transfer_failed(
    daemon_client,
    payload: Mapping[str, object],
    *,
    error: str,
    completion_evidence: Mapping[str, object] | None,
    cleanup_evidence: Iterable[Mapping[str, object]] | None = None,
    failure_source: str | None = None,
) -> None:
    transfer_id = payload.get("transfer_id")
    if transfer_id is None:
        return
    failure_evidence = _planned_execution_failure_evidence(
        payload,
        completion_evidence=completion_evidence,
        cleanup_evidence=cleanup_evidence,
        failure_source=failure_source,
    )
    response = daemon_client.transfer_status(
        str(transfer_id),
        state="failed",
        bytes_completed=max(
            0,
            int(failure_evidence.get("verified_bytes", 0) or 0),
        ),
        error=error,
        completion_source=_failure_completion_source(payload, completion_evidence),
        completion_evidence=failure_evidence,
    )
    require_ok(response, "daemon transfer failure update failed")


def _planned_execution_failure_evidence(
    payload: Mapping[str, object],
    *,
    completion_evidence: Mapping[str, object] | None,
    cleanup_evidence: Iterable[Mapping[str, object]] | None,
    failure_source: str | None,
) -> dict[str, object]:
    direct_bytes = _plan_assignment_bytes(payload, "direct")
    relay_bytes = _plan_assignment_bytes(payload, "relay")
    expected_bytes = direct_bytes + relay_bytes
    planned_cleanup = _planned_relay_cleanup_records(cleanup_evidence)
    if isinstance(completion_evidence, Mapping):
        evidence = {
            "expected_bytes": int(expected_bytes),
            "verified_bytes": min(
                int(expected_bytes),
                max(
                    0,
                    int(
                        completion_evidence.get(
                            "verified_bytes",
                            completion_evidence.get("direct_bytes", direct_bytes),
                        )
                        or 0
                    ),
                ),
            ),
            "content_match": False,
            "verification_source": (
                "mixed_pooled_partial_backend"
                if relay_bytes > 0
                else str(
                    completion_evidence.get(
                        "verification_source",
                        "direct_backend_partial",
                    )
                )
            ),
            "verification_method": (
                "direct_completed_relay_not_started"
                if relay_bytes > 0
                else str(
                    completion_evidence.get(
                        "verification_method",
                        "direct_completion",
                    )
                )
            ),
            "executor": (
                "mixed_worker_backend"
                if relay_bytes > 0
                else str(completion_evidence.get("executor", "direct_backend"))
            ),
            "plan_source": str(completion_evidence.get("plan_source", "daemon")),
            "path": (
                "mixed_pooled"
                if relay_bytes > 0
                else str(completion_evidence.get("path", "direct"))
            ),
            "direct_bytes": int(direct_bytes),
            "direct_chunks": int(
                completion_evidence.get(
                    "direct_chunks",
                    _plan_assignment_chunks(payload, "direct"),
                )
                or 0
            ),
            "relay_bytes": int(relay_bytes),
            "relay_chunks": int(_plan_assignment_chunks(payload, "relay")),
            "relay_bytes_completed": 0,
            "failure_source": str(
                failure_source
                or ("relay_worker" if relay_bytes > 0 else "backend")
            ),
            "direct_completion_evidence": dict(completion_evidence),
        }
        target_device = completion_evidence.get("target_device")
        if target_device is not None:
            evidence["target_device"] = int(target_device)
        resource_evidence = completion_evidence.get("resource_evidence")
        if relay_bytes > 0:
            evidence["resource_evidence"] = {
                "direct": (
                    dict(resource_evidence)
                    if isinstance(resource_evidence, Mapping)
                    else {}
                ),
                "relay": {},
            }
        elif isinstance(resource_evidence, Mapping):
            evidence["resource_evidence"] = dict(resource_evidence)
        cleanup = _cleanup_evidence_from_mapping(completion_evidence)
        if cleanup is not None:
            evidence["cleanup"] = cleanup
        _copy_path_level_evidence(evidence, completion_evidence)
    else:
        evidence = {
            "expected_bytes": int(expected_bytes),
            "verified_bytes": 0,
            "content_match": False,
            "verification_source": (
                "relay_worker_not_started"
                if relay_bytes > 0
                else "direct_backend_not_started"
            ),
            "verification_method": "transfer_not_started",
            "executor": "relay_worker" if relay_bytes > 0 else "direct_backend",
            "plan_source": "daemon",
            "path": "relay_only" if direct_bytes == 0 else "mixed_pooled",
            "direct_bytes": int(direct_bytes),
            "direct_chunks": int(_plan_assignment_chunks(payload, "direct")),
            "relay_bytes": int(relay_bytes),
            "relay_chunks": int(_plan_assignment_chunks(payload, "relay")),
            "relay_bytes_completed": 0,
            "failure_source": str(failure_source or "relay_worker"),
        }
    if planned_cleanup:
        evidence["planned_relay_cleanup"] = planned_cleanup
    evidence.update(_ticket_binding_from_payload(payload))
    evidence["execution_path_evidence"] = _execution_path_evidence(
        evidence,
        expected_bytes=int(expected_bytes),
    )
    return evidence


def _failure_completion_source(
    payload: Mapping[str, object],
    completion_evidence: Mapping[str, object] | None,
) -> str:
    if isinstance(completion_evidence, Mapping):
        return "backend"
    if _plan_assignment_bytes(payload, "relay") > 0:
        return "worker"
    return "backend"


def _planned_relay_cleanup_records(
    cleanup_evidence: Iterable[Mapping[str, object]] | None,
) -> list[dict[str, object]]:
    if cleanup_evidence is None:
        return []
    records: list[dict[str, object]] = []
    for record in cleanup_evidence:
        if isinstance(record, Mapping):
            records.append(dict(record))
    return records


def _ticket_binding_from_payload(
    payload: Mapping[str, object],
) -> dict[str, object]:
    ticket = payload.get("ticket")
    binding: dict[str, object] = {}
    if isinstance(ticket, Mapping):
        ticket_id = ticket.get("ticket_id")
        if ticket_id is not None:
            binding["ticket_id"] = str(ticket_id)
        metadata = ticket.get("metadata")
        if isinstance(metadata, Mapping):
            transfer_id = metadata.get("transfer_id")
            if transfer_id is not None:
                binding["transfer_id"] = str(transfer_id)
            plan_generation = metadata.get("plan_generation")
            if plan_generation is not None:
                binding["plan_generation"] = int(plan_generation)
    transfer_id = payload.get("transfer_id")
    if transfer_id is not None:
        binding.setdefault("transfer_id", str(transfer_id))
    plan_generation = payload.get("plan_generation")
    if plan_generation is not None:
        binding.setdefault("plan_generation", int(plan_generation))
    return binding


def _worker_reported_bytes(worker_evidence: Mapping[str, object]) -> int:
    return max(0, int(worker_evidence.get("worker_bytes_completed", 0) or 0))


def _cleanup_evidence_from_mapping(
    evidence: Mapping[str, object] | None,
) -> dict[str, object] | None:
    if not isinstance(evidence, Mapping):
        return None
    cleanup = evidence.get("cleanup")
    if not isinstance(cleanup, Mapping):
        return None
    return dict(cleanup)


def _execution_path_evidence(
    evidence: Mapping[str, object],
    *,
    expected_bytes: int,
) -> dict[str, object]:
    path_evidence = {
        "direct_bytes": int(evidence.get("direct_bytes", 0) or 0),
        "direct_chunks": int(evidence.get("direct_chunks", 0) or 0),
        "relay_bytes": int(evidence.get("relay_bytes", 0) or 0),
        "relay_chunks": int(evidence.get("relay_chunks", 0) or 0),
    }
    for key in (
        "executor",
        "path",
        "plan_source",
        "staging_slot_id",
        "src_buffer_id",
        "dst_buffer_id",
    ):
        value = evidence.get(key)
        if value is not None:
            path_evidence[key] = str(value)
    for key in ("target_device", "relay_gpu"):
        value = evidence.get(key)
        if value is not None:
            path_evidence[key] = int(value)
    relay_gpus = evidence.get("relay_gpus")
    if relay_gpus is not None:
        path_evidence["relay_gpus"] = tuple(int(item) for item in relay_gpus)
    path_level_evidence = evidence.get("path_level_evidence")
    if isinstance(path_level_evidence, Mapping):
        path_evidence["path_level_evidence"] = dict(path_level_evidence)
    native_path_stats = evidence.get("native_path_stats")
    if isinstance(native_path_stats, (list, tuple)):
        path_evidence["native_path_stats"] = tuple(
            dict(item) for item in native_path_stats if isinstance(item, Mapping)
        )
    relay_device_stats = evidence.get("relay_device_stats")
    if isinstance(relay_device_stats, (list, tuple)):
        path_evidence["relay_device_stats"] = tuple(
            dict(item) for item in relay_device_stats if isinstance(item, Mapping)
        )
    if (
        int(path_evidence["direct_bytes"]) + int(path_evidence["relay_bytes"])
        != int(expected_bytes)
    ):
        raise RuntimeError(
            "completion path evidence did not match daemon-planned byte total"
        )
    return path_evidence


def _copy_path_level_evidence(
    evidence: dict[str, object],
    worker_evidence: Mapping[str, object],
) -> None:
    path_level_evidence = worker_evidence.get("path_level_evidence")
    if isinstance(path_level_evidence, Mapping):
        evidence["path_level_evidence"] = dict(path_level_evidence)
    native_path_stats = worker_evidence.get("native_path_stats")
    if isinstance(native_path_stats, (list, tuple)):
        evidence["native_path_stats"] = tuple(
            dict(item) for item in native_path_stats if isinstance(item, Mapping)
        )
    relay_device_stats = worker_evidence.get("relay_device_stats")
    if isinstance(relay_device_stats, (list, tuple)):
        evidence["relay_device_stats"] = tuple(
            dict(item) for item in relay_device_stats if isinstance(item, Mapping)
        )


def _copy_worker_lifecycle_evidence(
    evidence: dict[str, object],
    worker_evidence: Mapping[str, object],
) -> None:
    for key in (
        "worker_startup",
        "worker_async_pool",
        "worker_runtime_feedback",
        "async_data_plane",
    ):
        value = worker_evidence.get(key)
        if isinstance(value, Mapping):
            evidence[key] = dict(value)


def _fail_transfer_without_worker_client(
    *,
    daemon_client,
    intent: TransferIntent,
    payload: Mapping[str, object],
    lease_tokens: Iterable[Mapping[str, object]],
    direct_completion_evidence: Mapping[str, object] | None,
    direct_bytes_completed: int,
) -> None:
    cleanup_evidence = cleanup_planned_relay_leases(
        daemon_client,
        lease_tokens,
        reason="worker_client_unavailable",
        strict=False,
    )
    failure_message = (
        "daemon-issued relay execution requires a runtime-session-managed "
        "worker service; use TurboBusRuntimeSession.open_production_socket "
        "or open_managed_production_socket"
    )
    _mark_transfer_failed(
        daemon_client,
        payload,
        error=failure_message,
        completion_evidence=direct_completion_evidence,
        cleanup_evidence=cleanup_evidence,
        failure_source="worker_service_unavailable",
    )


def _fail_transfer_without_relay_leases(
    *,
    daemon_client,
    intent: TransferIntent,
    payload: Mapping[str, object],
    direct_completion_evidence: Mapping[str, object] | None,
    direct_bytes_completed: int,
) -> None:
    failure_message = (
        "daemon-issued mixed or relay execution requires relay lease tokens; "
        "daemon planned a non-direct transfer without worker relay leases"
    )
    _mark_transfer_failed(
        daemon_client,
        payload,
        error=failure_message,
        completion_evidence=direct_completion_evidence,
        failure_source="missing_relay_lease",
    )


def _receipt_from_status_query(
    daemon_client,
    intent_id: str,
) -> TransferReceipt:
    waiter = getattr(daemon_client, "wait_transfer_receipt", None)
    if not callable(waiter):
        raise TypeError("daemon client must support wait_transfer_receipt")
    response = waiter(str(intent_id), timeout_seconds=0.0)
    require_ok(response, "daemon receipt wait failed")
    payload = response.payload if isinstance(response.payload, Mapping) else {}
    receipt_payload = payload.get("receipt")
    if not isinstance(receipt_payload, Mapping):
        raise ValueError("daemon response missing receipt")
    return TransferReceipt(**dict(receipt_payload))


def _trace_runtime_stage(name: str, **fields) -> None:
    if os.environ.get("TURBOBUS_BENCHMARK_TRACE") != "1":
        return
    details = " ".join(f"{key}={value}" for key, value in sorted(fields.items()))
    print(f"turbobus_runtime_stage name={name} {details}".rstrip(), flush=True)


__all__ = ["WorkerIntentTransferExecutor"]
