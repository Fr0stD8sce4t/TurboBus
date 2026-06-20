from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict
import os
from threading import Lock
import time

from ..schema import (
    DaemonResponse,
    ExecutionTicket,
    WorkerTransferAuthorizationRequest,
)
from .models import (
    WorkerServiceRequestEnvelope,
    WorkerServiceResponseEnvelope,
    WorkerTransferLifecycleRecord,
    WorkerTransferRequest,
    WorkerTransferResult,
    WorkerTransferState,
    daemon_status_update_for_result,
    worker_request_lease_ids,
)
from .lifecycle_evidence import (
    cleanup_completion_evidence as _cleanup_completion_evidence,
    execution_contract_evidence_from_metadata as _execution_contract_evidence_from_metadata,
    status_evidence_for_result as _status_evidence_for_result,
    worker_block_progress_evidence as _worker_block_progress_evidence,
    worker_pool_record as _worker_pool_record,
)
from .resources import (
    WorkerDataPlaneResourceBinder,
    WorkerDataPlaneResources,
)
from .staging_pool import WorkerStagingPool, WorkerStagingSlot
from . import validation as worker_validation


class WorkerAuthorizationError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        daemon_payload: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.daemon_payload = (
            dict(daemon_payload) if isinstance(daemon_payload, Mapping) else None
        )


class WorkerStatusReportError(RuntimeError):
    pass


class WorkerCleanupError(RuntimeError):
    pass


class WorkerAsyncExecutionPoolError(RuntimeError):
    pass


class WorkerTransferAuthorizer:
    def __init__(self, daemon_client) -> None:
        self.daemon_client = daemon_client

    def authorize(
        self,
        request: WorkerTransferAuthorizationRequest,
    ) -> WorkerTransferRequest:
        return self._authorize(request)

    def _authorize(
        self,
        request: WorkerTransferAuthorizationRequest,
    ) -> WorkerTransferRequest:
        response: DaemonResponse = self.daemon_client.authorize_worker_transfer(request)
        if not response.ok:
            raise WorkerAuthorizationError(
                response.error or "worker transfer authorization failed",
                daemon_payload=(
                    response.payload if isinstance(response.payload, Mapping) else None
                ),
            )
        try:
            validate_at = (
                time.time()
                if isinstance(response.payload, Mapping)
                and response.payload.get("authorized_at") is not None
                else None
            )
            worker_request = WorkerTransferRequest.from_authorization_payload(
                response.payload,
                now=validate_at,
            )
            require_daemon_worker_plan(worker_request)
            return worker_request
        except (KeyError, TypeError, ValueError) as exc:
            raise WorkerAuthorizationError(
                f"invalid worker authorization response: {exc}",
                daemon_payload=(
                    response.payload if isinstance(response.payload, Mapping) else None
                ),
            ) from exc


class WorkerTransferStatusReporter:
    def __init__(self, daemon_client) -> None:
        self.daemon_client = daemon_client

    def report(self, result: WorkerTransferResult) -> DaemonResponse:
        if not isinstance(result, WorkerTransferResult):
            raise TypeError("result must be a WorkerTransferResult")
        status_update = daemon_status_update_for_result(result)
        response: DaemonResponse = self.daemon_client.transfer_status(
            status_update["transfer_id"],
            state=status_update["state"],
            bytes_completed=status_update["bytes_completed"],
            error=status_update["error"],
            completion_source="worker",
            completion_evidence=_status_evidence_for_result(result),
        )
        if not response.ok:
            raise WorkerStatusReportError(
                response.error or "worker transfer status report failed"
            )
        return response

    def report_running(
        self,
        worker_request: WorkerTransferRequest,
        staging_slot: WorkerStagingSlot,
    ) -> tuple[dict[str, object], DaemonResponse]:
        result = _worker_running_result(worker_request, staging_slot)
        status_update = daemon_status_update_for_result(result)
        response = self.report(result)
        return status_update, response


class WorkerTransferCleanupCoordinator:
    def __init__(self, daemon_client) -> None:
        self.daemon_client = daemon_client

    def cleanup_authorization_failure(
        self,
        request: WorkerTransferAuthorizationRequest,
        authorization_payload: Mapping[str, object] | None = None,
        target_kind: str = "reservation",
        reason: str = "worker_authorization_failed",
        force: bool = True,
    ) -> DaemonResponse:
        if not isinstance(request, WorkerTransferAuthorizationRequest):
            raise TypeError("request must be a WorkerTransferAuthorizationRequest")
        try:
            cleanup_contract = self._authorized_cleanup_contract(
                request,
                authorization_payload,
            )
        except WorkerCleanupError as exc:
            return DaemonResponse(
                ok=True,
                payload={
                    "cleanup_skipped": True,
                    "cleanup_mode": "skipped_untrusted_authorization_failure",
                    "target_kind": target_kind,
                    "requested_lease_id": request.lease_id,
                    "requested_session_id": request.session_id,
                    "reason": reason,
                    "skip_reason": str(exc),
                },
            )


        return self._cleanup_authorized_scope(
            target_kind=target_kind,
            session_id=str(cleanup_contract["session_id"]),
            lease_ids=tuple(str(item) for item in cleanup_contract["lease_ids"]),
            reason=reason,
            force=force,
            owner_binding=cleanup_contract["owner_binding"],
        )

    def _authorized_cleanup_contract(
        self,
        request: WorkerTransferAuthorizationRequest,
        authorization_payload: Mapping[str, object] | None,
    ) -> dict[str, object]:
        if authorization_payload is None:
            raise WorkerCleanupError(
                "authorization failure cleanup requires daemon-issued ticket payload"
            )
        if not isinstance(authorization_payload, Mapping):
            raise WorkerCleanupError("authorization payload must be a mapping")
        try:
            ticket_payload = authorization_payload.get("ticket")
            if isinstance(ticket_payload, Mapping):
                ticket = ExecutionTicket(**dict(ticket_payload))
                worker_validation.validate_daemon_issued_ticket(
                    ticket,
                    plan_generation=authorization_payload.get("plan_generation"),
                )
                owner_binding = worker_validation.owner_binding_for_ticket(
                    ticket,
                    transfer_id=request.transfer_id,
                    job_id=request.job_id,
                    session_id=request.session_id,
                    lease_id=authorization_payload.get("lease_id"),
                    lease_ids=authorization_payload.get("lease_ids"),
                    relay_gpu=authorization_payload.get("relay_gpu"),
                    relay_gpus=authorization_payload.get("relay_gpus"),
                )
            else:
                owner_binding = worker_validation.owner_binding_for_payload(
                    authorization_payload,
                    transfer_id=request.transfer_id,
                    job_id=request.job_id,
                    session_id=request.session_id,
                    lease_id=authorization_payload.get("lease_id"),
                    lease_ids=authorization_payload.get("lease_ids"),
                    relay_gpu=authorization_payload.get("relay_gpu"),
                    relay_gpus=authorization_payload.get("relay_gpus"),
                )
        except (TypeError, ValueError) as exc:
            raise WorkerCleanupError(
                f"invalid daemon authorization cleanup payload: {exc}"
            ) from exc
        cleanup_scope = owner_binding["cleanup_scope"]
        lease_ids = tuple(str(item) for item in cleanup_scope["target_ids"])
        if not lease_ids:
            raise WorkerCleanupError("daemon authorization cleanup has no lease ids")
        return {
            "session_id": str(owner_binding["session_id"]),
            "lease_ids": lease_ids,
            "owner_binding": owner_binding,
        }

    def cleanup_execution_failure(
        self,
        request: WorkerTransferRequest,
        result: WorkerTransferResult,
        target_kind: str = "reservation",
        reason: str | None = None,
        force: bool = True,
    ) -> DaemonResponse:
        if not isinstance(request, WorkerTransferRequest):
            raise TypeError("request must be a WorkerTransferRequest")
        if not isinstance(result, WorkerTransferResult):
            raise TypeError("result must be a WorkerTransferResult")
        cleanup_contract = self._cleanup_contract_for_worker_request(request)
        return self._cleanup_authorized_scope(
            target_kind=target_kind,
            session_id=str(cleanup_contract["session_id"]),
            lease_ids=tuple(str(item) for item in cleanup_contract["lease_ids"]),
            reason=(
                reason
                or (
                    "worker_complete"
                    if result.state == WorkerTransferState.COMPLETE
                    else f"worker_{result.state.value}"
                )
            ),
            force=force,
            owner_binding=cleanup_contract["owner_binding"],
            retention_evidence=_worker_resource_lifecycle_retention_evidence(
                request,
                result,
            ),
        )

    def cleanup_status_report_failure(
        self,
        request: WorkerTransferRequest,
        target_kind: str = "reservation",
        reason: str = "worker_status_report_failed",
        force: bool = True,
    ) -> DaemonResponse:
        if not isinstance(request, WorkerTransferRequest):
            raise TypeError("request must be a WorkerTransferRequest")
        cleanup_contract = self._cleanup_contract_for_worker_request(request)
        return self._cleanup_authorized_scope(
            target_kind=target_kind,
            session_id=str(cleanup_contract["session_id"]),
            lease_ids=tuple(str(item) for item in cleanup_contract["lease_ids"]),
            reason=reason,
            force=force,
            owner_binding=cleanup_contract["owner_binding"],
            retention_evidence=None,
        )

    def _cleanup_contract_for_worker_request(
        self,
        request: WorkerTransferRequest,
    ) -> dict[str, object]:
        owner_binding = worker_validation.owner_binding_for_ticket(
            request.ticket,
            transfer_id=request.authorization.transfer_id,
            job_id=request.authorization.job_id,
            session_id=request.authorization.session_id,
            lease_id=request.authorization.lease_id,
            lease_ids=worker_request_lease_ids(request),
            relay_gpu=request.authorization.relay_gpu,
            relay_gpus=request.data_plane.metadata.get("relay_gpus"),
        )
        cleanup_scope = owner_binding["cleanup_scope"]
        return {
            "session_id": str(owner_binding["session_id"]),
            "lease_ids": tuple(str(item) for item in cleanup_scope["target_ids"]),
            "owner_binding": owner_binding,
        }

    def _cleanup_authorized_scope(
        self,
        *,
        target_kind: str,
        session_id: str,
        lease_ids: tuple[str, ...],
        reason: str,
        force: bool,
        owner_binding: Mapping[str, object],
        retention_evidence: Mapping[str, object] | None = None,
    ) -> DaemonResponse:
        if target_kind == "session":
            return self._cleanup(
                target_kind=target_kind,
                target_id=cleanup_target_id(
                    target_kind,
                    lease_id=lease_ids[0],
                    session_id=session_id,
                ),
                reason=reason,
                force=force,
                retention_evidence=retention_evidence,
            )
        if len(lease_ids) == 1:
            return self._cleanup(
                target_kind=target_kind,
                target_id=cleanup_target_id(
                    target_kind,
                    lease_id=lease_ids[0],
                    session_id=session_id,
                ),
                reason=reason,
                force=force,
                authorized_target_ids=lease_ids,
                owner_binding=owner_binding,
                retention_evidence=retention_evidence,
            )
        return self._cleanup_worker_leases(
            lease_ids=lease_ids,
            target_kind=target_kind,
            reason=reason,
            force=force,
            owner_binding=owner_binding,
            retention_evidence=retention_evidence,
        )

    def _cleanup(
        self,
        target_kind: str,
        target_id: str,
        reason: str,
        force: bool,
        authorized_target_ids: tuple[str, ...] | None = None,
        owner_binding: Mapping[str, object] | None = None,
        retention_evidence: Mapping[str, object] | None = None,
    ) -> DaemonResponse:
        response: DaemonResponse = self.daemon_client.cleanup(
            target_kind=target_kind,
            target_id=target_id,
            reason=reason,
            force=force,
            owner_binding=(
                None if owner_binding is None else dict(owner_binding)
            ),
            retention_evidence=(
                None if retention_evidence is None else dict(retention_evidence)
            ),
        )
        if not response.ok:
            raise WorkerCleanupError(response.error or "worker cleanup failed")
        if target_kind != "reservation" or authorized_target_ids is None:
            return response
        return self._validated_cleanup_response(
            response,
            expected_target_id=str(target_id),
            authorized_target_ids=authorized_target_ids,
            target_kind=target_kind,
            owner_binding=owner_binding,
        )

    def _cleanup_worker_leases(
        self,
        *,
        lease_ids: tuple[str, ...],
        target_kind: str,
        reason: str,
        force: bool,
        owner_binding: Mapping[str, object],
        retention_evidence: Mapping[str, object] | None = None,
    ) -> DaemonResponse:
        return self._cleanup_lease_ids(
            lease_ids=lease_ids,
            target_kind=target_kind,
            reason=reason,
            force=force,
            owner_binding=owner_binding,
            retention_evidence=retention_evidence,
        )

    def _cleanup_lease_ids(
        self,
        *,
        lease_ids: tuple[str, ...],
        target_kind: str,
        reason: str,
        force: bool,
        owner_binding: Mapping[str, object],
        retention_evidence: Mapping[str, object] | None = None,
    ) -> DaemonResponse:
        cleanup = getattr(self.daemon_client, "cleanup", None)
        if not callable(cleanup):
            raise WorkerCleanupError("daemon client cannot clean worker transfer")
        responses: list[dict[str, object]] = []
        cleaned_ids: list[str] = []
        errors: list[str] = []
        cleanup_modes: list[str] = []
        for lease_id in lease_ids:
            response = cleanup(
                target_kind=target_kind,
                target_id=lease_id,
                reason=reason,
                force=force,
                owner_binding=dict(owner_binding),
                retention_evidence=(
                    None if retention_evidence is None else dict(retention_evidence)
                ),
            )
            if response.ok:
                normalized_response = self._validated_cleanup_response(
                    response,
                    expected_target_id=str(lease_id),
                    authorized_target_ids=lease_ids,
                    target_kind=target_kind,
                    owner_binding=owner_binding,
                )
                responses.append(asdict(normalized_response))
                payload = (
                    normalized_response.payload
                    if isinstance(normalized_response.payload, Mapping)
                    else {}
                )
                cleanup_modes.append(str(payload.get("cleanup_mode", "cleanup")))
                if bool(payload.get("retention_evidence_recorded", False)):
                    cleanup_modes.append("retention_recorded")
                cleaned_ids.extend(
                    str(item)
                    for item in payload.get("cleaned_reservation_ids", ()) or ()
                )
                continue
            responses.append(asdict(response))
            errors.append(f"{lease_id}: {response.error or 'worker cleanup failed'}")
        if errors:
            raise WorkerCleanupError("; ".join(errors))
        payload = {
            "reservation_id": lease_ids[0],
            "lease_ids": lease_ids,
            "cleaned_reservation_ids": tuple(cleaned_ids),
            "released_reservation_ids": tuple(cleaned_ids),
            "cleanup_scope_target_ids": lease_ids,
            "lease_responses": tuple(responses),
            "cleanup_kind": target_kind,
            "reason": reason,
            "cleanup_mode": (
                "noop"
                if cleanup_modes and all(mode == "noop" for mode in cleanup_modes)
                else "cleanup"
            ),
            "owner_binding": dict(owner_binding),
            **(
                {}
                if not any(mode == "retention_recorded" for mode in cleanup_modes)
                else {"retention_evidence_recorded": True}
            ),
        }
        return DaemonResponse(ok=True, payload=payload)

    def _validated_cleanup_response(
        self,
        response: DaemonResponse,
        *,
        expected_target_id: str,
        authorized_target_ids: tuple[str, ...],
        target_kind: str,
        owner_binding: Mapping[str, object] | None,
    ) -> DaemonResponse:
        payload = response.payload if isinstance(response.payload, Mapping) else {}
        normalized_payload = dict(payload)
        cleanup_kind = str(
            normalized_payload.get(
                "cleanup_kind",
                normalized_payload.get("target_kind", target_kind),
            )
        )
        if cleanup_kind != str(target_kind):
            raise WorkerCleanupError(
                "daemon cleanup response target kind does not match request"
            )
        normalized_reservation_id = str(
            normalized_payload.get("reservation_id", expected_target_id)
        )
        if normalized_reservation_id != str(expected_target_id):
            raise WorkerCleanupError(
                "daemon cleanup response reservation_id does not match request"
            )
        cleaned_reservation_ids = tuple(
            str(item)
            for item in normalized_payload.get("cleaned_reservation_ids", ()) or ()
        )
        unauthorized_cleaned_ids = sorted(
            set(cleaned_reservation_ids) - {str(expected_target_id)}
        )
        if unauthorized_cleaned_ids:
            raise WorkerCleanupError(
                "daemon cleanup response cleaned reservation ids escaped requested target"
            )
        unauthorized_scope_ids = sorted(
            set((normalized_reservation_id, *cleaned_reservation_ids))
            - set(str(item) for item in authorized_target_ids)
        )
        if unauthorized_scope_ids:
            raise WorkerCleanupError(
                "daemon cleanup response reservation ids escaped authorized cleanup scope"
            )
        normalized_payload["reservation_id"] = normalized_reservation_id
        normalized_payload["cleaned_reservation_ids"] = cleaned_reservation_ids
        normalized_payload["lease_ids"] = tuple(str(item) for item in authorized_target_ids)
        normalized_payload["cleanup_kind"] = str(target_kind)
        normalized_payload["cleanup_scope_target_ids"] = tuple(
            str(item) for item in authorized_target_ids
        )
        if owner_binding is not None:
            normalized_payload["owner_binding"] = dict(owner_binding)
        return DaemonResponse(ok=response.ok, payload=normalized_payload, error=response.error)


_WorkerTransferAuthorizer = WorkerTransferAuthorizer
_WorkerTransferStatusReporter = WorkerTransferStatusReporter
_WorkerTransferCleanupCoordinator = WorkerTransferCleanupCoordinator


class WorkerAsyncExecutionPool:
    def __init__(
        self,
        executor,
        *,
        resource_binder: WorkerDataPlaneResourceBinder | None,
        worker_startup_evidence: Mapping[str, object] | None = None,
        max_workers: int | None = None,
    ) -> None:
        if executor is None:
            raise ValueError("worker async execution pool requires an executor")
        worker_count = 1 if max_workers is None else int(max_workers)
        if worker_count <= 0:
            raise ValueError("worker async execution pool max_workers must be positive")
        self._executor = executor
        self._resource_binder = resource_binder
        self._worker_startup_evidence = (
            None
            if worker_startup_evidence is None
            else dict(worker_startup_evidence)
        )
        self._executor_pool = ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="turbobus-worker-transfer",
        )
        self._lock = Lock()
        self._next_pool_sequence = 1
        self._queued: dict[str, dict[str, object]] = {}
        self._running: dict[str, dict[str, object]] = {}
        self._futures: dict[str, Future] = {}
        self._terminal: OrderedDict[str, dict[str, object]] = OrderedDict()
        self._terminal_history_limit = _worker_terminal_history_limit(executor)
        self._terminal_history_evictions = 0
        self._closed = False

    def submit(
        self,
        worker_request: WorkerTransferRequest,
        staging_slot: WorkerStagingSlot,
    ) -> "WorkerAsyncExecution":
        if not isinstance(worker_request, WorkerTransferRequest):
            raise TypeError("worker_request must be a WorkerTransferRequest")
        if not isinstance(staging_slot, WorkerStagingSlot):
            raise TypeError("staging_slot must be a WorkerStagingSlot")
        worker_validation.validate_daemon_issued_ticket(worker_request.ticket)
        worker_validation.validate_ticket_matches_worker_request(
            worker_request.ticket,
            worker_request.authorization,
            worker_request.data_plane,
        )
        pool_ticket = self._next_pool_ticket(worker_request)
        queued_at = time.time()
        queued_record = _worker_pool_record(
            pool_ticket=pool_ticket,
            state="queued",
            worker_request=worker_request,
            staging_slot=staging_slot,
            queued_at=queued_at,
        )
        with self._lock:
            if self._closed:
                raise WorkerAsyncExecutionPoolError(
                    "worker async execution pool is closed"
                )
            if (
                worker_request.transfer_id in self._queued
                or worker_request.transfer_id in self._running
            ):
                raise WorkerAsyncExecutionPoolError(
                    "worker transfer is already queued or running"
                )
            self._queued[worker_request.transfer_id] = queued_record
            try:
                future = self._executor_pool.submit(
                    self._run_transfer,
                    worker_request,
                    staging_slot,
                    pool_ticket,
                    queued_at,
                )
                queued_record["future_registered"] = True
                self._futures[worker_request.transfer_id] = future
            except Exception:
                self._queued.pop(worker_request.transfer_id, None)
                self._futures.pop(worker_request.transfer_id, None)
                raise
        return WorkerAsyncExecution(
            pool=self,
            pool_ticket=pool_ticket,
            transfer_id=worker_request.transfer_id,
            future=future,
        )

    def wait(self, execution: "WorkerAsyncExecution") -> WorkerTransferResult:
        if not isinstance(execution, WorkerAsyncExecution):
            raise TypeError("execution must be WorkerAsyncExecution")
        if execution.pool is not self:
            raise WorkerAsyncExecutionPoolError(
                "worker async execution belongs to another pool"
            )
        result = execution.future.result()
        if not isinstance(result, WorkerTransferResult):
            raise TypeError("worker async execution returned an invalid result")
        return result

    def describe(self) -> dict[str, object]:
        with self._lock:
            return self._describe_locked()

    def cancel_transfer(
        self,
        execution: "WorkerAsyncExecution",
        *,
        reason: str = "worker_async_execution_cancelled",
    ) -> dict[str, object]:
        if not isinstance(execution, WorkerAsyncExecution):
            raise TypeError("execution must be WorkerAsyncExecution")
        if execution.pool is not self:
            raise WorkerAsyncExecutionPoolError(
                "worker async execution belongs to another pool"
            )
        canceled = execution.future.cancel()
        record = {
            "pool": "worker_async_execution_pool",
            "pool_ticket": execution.pool_ticket,
            "transfer_id": execution.transfer_id,
            "state": "canceled" if canceled else "cancel_requested",
            "reason": str(reason),
            "canceled": bool(canceled),
            "recorded_at": time.time(),
        }
        with self._lock:
            queued = (
                self._queued.pop(execution.transfer_id, None)
                if canceled
                else self._queued.get(execution.transfer_id)
            )
            if isinstance(queued, Mapping):
                record["queued_record"] = dict(queued)
            running = self._running.get(execution.transfer_id)
            if isinstance(running, Mapping):
                record["running_record"] = dict(running)
            record["future"] = _future_state_record(execution.future)
            if canceled:
                self._futures.pop(execution.transfer_id, None)
                self._record_terminal_locked(execution.transfer_id, record)
                record["terminal_recorded"] = True
            else:
                record["terminal_recorded"] = False
        return dict(record)

    def drain_terminal(self) -> dict[str, dict[str, object]]:
        with self._lock:
            terminal = {
                key: dict(value) for key, value in sorted(self._terminal.items())
            }
            self._terminal.clear()
        return terminal

    def close(self, *, cancel_queued: bool = True) -> dict[str, object]:
        closed_at = time.time()
        with self._lock:
            self._closed = True
            queued_snapshot = {
                key: dict(value) for key, value in sorted(self._queued.items())
            }
            running_snapshot = {
                key: dict(value) for key, value in sorted(self._running.items())
            }
            futures_to_cancel = {
                key: self._futures[key]
                for key in queued_snapshot
                if key in self._futures
            }
        cancelled_futures: dict[str, bool] = {}
        if cancel_queued:
            for transfer_id, future in futures_to_cancel.items():
                cancelled_futures[transfer_id] = bool(future.cancel())
        self._executor_pool.shutdown(wait=True, cancel_futures=bool(cancel_queued))
        with self._lock:
            close_terminal_records = self._record_close_cancelled_queued_locked(
                queued_snapshot=queued_snapshot,
                futures=futures_to_cancel,
                cancelled_futures=cancelled_futures,
                closed_at=closed_at,
                cancel_queued=bool(cancel_queued),
            )
            terminal_snapshot = {
                key: dict(value) for key, value in sorted(self._terminal.items())
            }
        return {
            "pool": "worker_async_execution_pool",
            "closed_at": closed_at,
            "cancel_queued": bool(cancel_queued),
            "queued_at_close": queued_snapshot,
            "running_at_close": running_snapshot,
            "queued_cancelled_at_close": close_terminal_records,
            "terminal_at_close": terminal_snapshot,
        }

    def _describe_locked(self) -> dict[str, object]:
        return {
            "closed": bool(self._closed),
            "terminal_history_limit": int(self._terminal_history_limit),
            "terminal_history_evictions": int(self._terminal_history_evictions),
            "queued": {
                key: dict(value) for key, value in sorted(self._queued.items())
            },
            "running": {
                key: dict(value) for key, value in sorted(self._running.items())
            },
            "submitted_future_count": len(self._futures),
            "terminal": {
                key: dict(value) for key, value in sorted(self._terminal.items())
            },
        }

    def _next_pool_ticket(self, worker_request: WorkerTransferRequest) -> str:
        with self._lock:
            sequence = self._next_pool_sequence
            self._next_pool_sequence += 1
        return f"worker-pool-{sequence}-{worker_request.transfer_id}"

    def _run_transfer(
        self,
        worker_request: WorkerTransferRequest,
        staging_slot: WorkerStagingSlot,
        pool_ticket: str,
        queued_at: float,
    ) -> WorkerTransferResult:
        started_at = time.time()
        running_record = _worker_pool_record(
            pool_ticket=pool_ticket,
            state="running",
            worker_request=worker_request,
            staging_slot=staging_slot,
            queued_at=queued_at,
            started_at=started_at,
        )
        with self._lock:
            self._queued.pop(worker_request.transfer_id, None)
            self._running[worker_request.transfer_id] = running_record
        result: WorkerTransferResult | None = None
        completed_at = 0.0
        terminal_history_evictions = 0
        try:
            result = _execute_worker_transfer_once(
                executor=self._executor,
                resource_binder=self._resource_binder,
                worker_startup_evidence=self._worker_startup_evidence,
                worker_request=worker_request,
                staging_slot=staging_slot,
            )
        finally:
            completed_at = time.time()
            state = (
                result.state.value
                if isinstance(result, WorkerTransferResult)
                else "failed"
            )
            terminal_record = _worker_pool_record(
                pool_ticket=pool_ticket,
                state=state,
                worker_request=worker_request,
                staging_slot=staging_slot,
                queued_at=queued_at,
                started_at=started_at,
                completed_at=completed_at,
            )
            with self._lock:
                self._running.pop(worker_request.transfer_id, None)
                self._futures.pop(worker_request.transfer_id, None)
                self._record_terminal_locked(
                    worker_request.transfer_id,
                    terminal_record,
                )
                terminal_history_evictions = self._terminal_history_evictions
        if result is None:
            raise RuntimeError("worker execution did not produce a result")
        return _worker_result_with_async_pool_evidence(
            result,
            pool_ticket=pool_ticket,
            queued_at=queued_at,
            started_at=started_at,
            completed_at=completed_at,
            terminal_history_limit=self._terminal_history_limit,
            terminal_history_evictions=terminal_history_evictions,
        )

    def _record_close_cancelled_queued_locked(
        self,
        *,
        queued_snapshot: Mapping[str, Mapping[str, object]],
        futures: Mapping[str, Future],
        cancelled_futures: Mapping[str, bool],
        closed_at: float,
        cancel_queued: bool,
    ) -> dict[str, dict[str, object]]:
        if not cancel_queued:
            return {}
        terminal_records: dict[str, dict[str, object]] = {}
        for transfer_id, queued_record in sorted(queued_snapshot.items()):
            active_queued = self._queued.get(transfer_id)
            if active_queued is None:
                continue
            future = futures.get(transfer_id)
            if future is not None and not future.cancelled():
                continue
            self._queued.pop(transfer_id, None)
            self._futures.pop(transfer_id, None)
            terminal_record = dict(queued_record)
            terminal_record.update(
                {
                    "state": "canceled",
                    "reason": "worker_async_pool_close",
                    "closed_at": float(closed_at),
                    "completed_at": float(closed_at),
                    "cancel_queued": True,
                    "canceled": bool(cancelled_futures.get(transfer_id, True)),
                    "future": _future_state_record(future),
                }
            )
            self._record_terminal_locked(transfer_id, terminal_record)
            terminal_records[transfer_id] = dict(terminal_record)
        return terminal_records

    def _record_terminal_locked(
        self,
        transfer_id: str,
        record: Mapping[str, object],
    ) -> None:
        self._terminal[str(transfer_id)] = dict(record)
        self._terminal.move_to_end(str(transfer_id))
        if self._terminal_history_limit < 0:
            return
        while len(self._terminal) > self._terminal_history_limit:
            self._terminal.popitem(last=False)
            self._terminal_history_evictions += 1


class WorkerAsyncExecution:
    def __init__(
        self,
        *,
        pool: WorkerAsyncExecutionPool,
        pool_ticket: str,
        transfer_id: str,
        future: Future,
    ) -> None:
        self.pool = pool
        self.pool_ticket = str(pool_ticket)
        self.transfer_id = str(transfer_id)
        self.future = future

    def evidence(self, *, state: str = "failed") -> dict[str, object]:
        evidence = {
            "pool": "worker_async_execution_pool",
            "pool_ticket": self.pool_ticket,
            "transfer_id": self.transfer_id,
            "state": str(state),
        }
        evidence["pool_snapshot"] = self.pool.describe()
        evidence["future"] = _future_state_record(self.future)
        return evidence


def _future_state_record(future: Future | None) -> dict[str, object]:
    if future is None:
        return {
            "registered": False,
            "done": False,
            "cancelled": False,
            "running": False,
        }
    return {
        "registered": True,
        "done": bool(future.done()),
        "cancelled": bool(future.cancelled()),
        "running": bool(future.running()),
    }


class WorkerTransferClient:
    def __init__(
        self,
        daemon_client,
        executor: object | None = None,
        status_reporter: _WorkerTransferStatusReporter | None = None,
        cleanup_coordinator: _WorkerTransferCleanupCoordinator | None = None,
        staging_pool: WorkerStagingPool | None = None,
        resource_binder: WorkerDataPlaneResourceBinder | None = None,
        execution_pool: WorkerAsyncExecutionPool | None = None,
        execution_pool_workers: int | None = None,
        worker_startup_evidence: Mapping[str, object] | None = None,
    ) -> None:
        if executor is None:
            executor = default_worker_executor()
            if resource_binder is None:
                resource_binder = WorkerDataPlaneResourceBinder()
        self._authorizer = _WorkerTransferAuthorizer(daemon_client)
        self._executor = executor
        self._status_reporter = status_reporter or _WorkerTransferStatusReporter(
            daemon_client
        )
        self._cleanup_coordinator = cleanup_coordinator or _WorkerTransferCleanupCoordinator(
            daemon_client
        )
        self._staging_pool = staging_pool or WorkerStagingPool()
        self._resource_binder = resource_binder
        self._worker_startup_evidence = (
            None
            if worker_startup_evidence is None
            else dict(worker_startup_evidence)
        )
        self._execution_pool = execution_pool or WorkerAsyncExecutionPool(
            self._executor,
            resource_binder=self._resource_binder,
            worker_startup_evidence=self._worker_startup_evidence,
            max_workers=execution_pool_workers,
        )

    def _authorize(
        self,
        request: WorkerTransferAuthorizationRequest,
    ) -> WorkerTransferRequest:
        return self._authorizer._authorize(request)

    def describe_execution_pool(self) -> dict[str, object]:
        return self._execution_pool.describe()

    def close_execution_pool(self, *, cancel_queued: bool = True) -> dict[str, object]:
        return self._execution_pool.close(cancel_queued=cancel_queued)

    @property
    def executor(self):
        return self._executor

    @property
    def resource_binder(self):
        return self._resource_binder

    def submit(
        self,
        request: WorkerTransferAuthorizationRequest,
    ) -> WorkerTransferResult:
        worker_request = self._authorize(request)
        staging_slot = self._staging_pool.allocate(worker_request.data_plane)
        try:
            return self._execute_lifecycle_transfer(worker_request, staging_slot)
        finally:
            self._staging_pool.release(staging_slot.slot_id)

    def submit_and_report(
        self,
        request: WorkerTransferAuthorizationRequest,
    ) -> WorkerTransferResult:
        result = self.submit(request)
        self._status_reporter.report(result)
        return result

    def submit_report_and_cleanup(
        self,
        request: WorkerTransferAuthorizationRequest,
        cleanup_target_kind: str = "reservation",
    ) -> WorkerTransferResult:
        lifecycle = self.submit_report_cleanup_lifecycle(
            request,
            cleanup_target_kind=cleanup_target_kind,
        )
        if lifecycle.worker_request is None:
            raise WorkerAuthorizationError(lifecycle.error or "worker authorization failed")
        if lifecycle.result is None:
            raise WorkerStatusReportError(lifecycle.error or "worker transfer failed")
        return lifecycle.result

    def submit_report_cleanup_lifecycle(
        self,
        request: WorkerTransferAuthorizationRequest,
        cleanup_target_kind: str = "reservation",
        report_terminal_status: bool = True,
    ) -> WorkerTransferLifecycleRecord:
        _trace_worker_stage(
            "worker_lifecycle_start",
            transfer_id=request.transfer_id,
            relay_gpu=request.relay_gpu,
        )
        lifecycle_context = self._start_worker_lifecycle_context(
            request=request,
            cleanup_target_kind=cleanup_target_kind,
        )
        if isinstance(lifecycle_context, WorkerTransferLifecycleRecord):
            return lifecycle_context
        worker_request = lifecycle_context["worker_request"]
        staging_slot = lifecycle_context["staging_slot"]
        running_update = lifecycle_context["running_update"]
        running_response = lifecycle_context["running_response"]
        result = self._execute_lifecycle_transfer(worker_request, staging_slot)
        status_update = daemon_status_update_for_result(result)
        status_response: DaemonResponse | None = None
        if report_terminal_status:
            try:
                status_response = self._status_reporter.report(result)
            except WorkerStatusReportError as exc:
                return self._terminal_status_failure_lifecycle_record(
                    request=request,
                    worker_request=worker_request,
                    staging_slot=staging_slot,
                    running_update=running_update,
                    running_response=running_response,
                    result=result,
                    status_update=status_update,
                    error=exc,
                    cleanup_target_kind=cleanup_target_kind,
                )
        cleanup_target_id = self._cleanup_target_id_for_lifecycle_result(
            cleanup_target_kind=cleanup_target_kind,
            worker_request=worker_request,
            result=result,
        )
        cleanup_response = self._cleanup_lifecycle_execution_result(
            request=request,
            worker_request=worker_request,
            staging_slot=staging_slot,
            running_update=running_update,
            running_response=running_response,
            result=result,
            status_update=status_update,
            status_response=status_response,
            cleanup_target_kind=cleanup_target_kind,
            cleanup_target_id=cleanup_target_id,
        )
        if isinstance(cleanup_response, WorkerTransferLifecycleRecord):
            return cleanup_response
        result = _worker_result_with_cleanup_completion_evidence(
            worker_request,
            result,
            cleanup_response,
        )
        if not report_terminal_status and result.state in {
            WorkerTransferState.COMPLETE,
            WorkerTransferState.FAILED,
        }:
            return self._terminal_without_status_lifecycle_record(
                request=request,
                worker_request=worker_request,
                staging_slot=staging_slot,
                running_update=running_update,
                running_response=running_response,
                result=result,
                cleanup_target_kind=cleanup_target_kind,
                cleanup_target_id=cleanup_target_id,
                cleanup_response=cleanup_response,
            )
        status_response = self._report_cleanup_evidence(
            worker_request,
            result,
            cleanup_response,
            current_status_response=status_response,
        )
        return self._completed_cleanup_lifecycle_record(
            request=request,
            worker_request=worker_request,
            staging_slot=staging_slot,
            running_update=running_update,
            running_response=running_response,
            result=result,
            status_update=status_update,
            status_response=status_response,
            cleanup_target_kind=cleanup_target_kind,
            cleanup_target_id=cleanup_target_id,
            cleanup_response=cleanup_response,
        )

    def _start_worker_lifecycle_context(
        self,
        *,
        request: WorkerTransferAuthorizationRequest,
        cleanup_target_kind: str,
    ) -> dict[str, object] | WorkerTransferLifecycleRecord:
        try:
            worker_request = self._authorize(request)
        except WorkerAuthorizationError as exc:
            return self._authorization_failure_lifecycle_record(
                request=request,
                error=exc,
                cleanup_target_kind=cleanup_target_kind,
            )
        _trace_worker_stage(
            "worker_lifecycle_authorized",
            transfer_id=worker_request.transfer_id,
        )
        staging_slot = self._staging_pool.allocate(worker_request.data_plane)
        _trace_worker_stage(
            "worker_lifecycle_staging_allocated",
            transfer_id=worker_request.transfer_id,
            slot_id=staging_slot.slot_id,
        )
        running_update: dict[str, object] | None = None
        running_response: DaemonResponse | None = None
        try:
            _trace_worker_stage(
                "worker_lifecycle_report_running_start",
                transfer_id=worker_request.transfer_id,
            )
            running_update, running_response = self._status_reporter.report_running(
                worker_request,
                staging_slot,
            )
            _trace_worker_stage(
                "worker_lifecycle_report_running_done",
                transfer_id=worker_request.transfer_id,
            )
        except WorkerStatusReportError as exc:
            return self._running_status_failure_lifecycle_record(
                request=request,
                worker_request=worker_request,
                staging_slot=staging_slot,
                running_update=running_update,
                running_response=running_response,
                error=exc,
                cleanup_target_kind=cleanup_target_kind,
            )
        return {
            "worker_request": worker_request,
            "staging_slot": staging_slot,
            "running_update": running_update,
            "running_response": running_response,
        }

    def _execute_lifecycle_transfer(
        self,
        worker_request: WorkerTransferRequest,
        staging_slot: WorkerStagingSlot,
    ) -> WorkerTransferResult:
        try:
            _trace_worker_stage(
                "worker_lifecycle_execute_start",
                transfer_id=worker_request.transfer_id,
            )
            result = validate_worker_completion_bytes(
                worker_request,
                self._execute(worker_request, staging_slot),
            )
            _trace_worker_stage(
                "worker_lifecycle_execute_done",
                transfer_id=worker_request.transfer_id,
                state=result.state.value,
                bytes=result.bytes_completed,
            )
            return result
        except Exception as exc:
            result = failed_worker_result_from_exception(
                worker_request,
                staging_slot,
                exc,
            )
            _trace_worker_stage(
                "worker_lifecycle_execute_failed",
                transfer_id=worker_request.transfer_id,
                error=str(exc),
            )
            return result

    def _cleanup_target_id_for_lifecycle_result(
        self,
        *,
        cleanup_target_kind: str,
        worker_request: WorkerTransferRequest,
        result: WorkerTransferResult,
    ) -> str:
        if result.state == WorkerTransferState.COMPLETE:
            return worker_request.authorization.lease_id
        return cleanup_target_id_for_worker_request(
            cleanup_target_kind,
            worker_request,
        )

    def _cleanup_lifecycle_execution_result(
        self,
        *,
        request: WorkerTransferAuthorizationRequest,
        worker_request: WorkerTransferRequest,
        staging_slot: WorkerStagingSlot,
        running_update: dict[str, object] | None,
        running_response: DaemonResponse | None,
        result: WorkerTransferResult,
        status_update: dict[str, object],
        status_response: DaemonResponse | None,
        cleanup_target_kind: str,
        cleanup_target_id: str,
    ) -> DaemonResponse | WorkerTransferLifecycleRecord:
        try:
            return self._cleanup_coordinator.cleanup_execution_failure(
                worker_request,
                result,
                target_kind=cleanup_target_kind,
            )
        except WorkerCleanupError as exc:
            return self._execution_cleanup_failure_lifecycle_record(
                request=request,
                worker_request=worker_request,
                staging_slot=staging_slot,
                running_update=running_update,
                running_response=running_response,
                result=result,
                status_update=status_update,
                status_response=status_response,
                cleanup_target_kind=cleanup_target_kind,
                cleanup_target_id=cleanup_target_id,
                error=exc,
            )

    def _authorization_failure_lifecycle_record(
        self,
        *,
        request: WorkerTransferAuthorizationRequest,
        error: WorkerAuthorizationError,
        cleanup_target_kind: str,
    ) -> WorkerTransferLifecycleRecord:
        cleanup_target_id = cleanup_target_id_for_request(
            cleanup_target_kind,
            request,
        )
        try:
            cleanup_response = self._cleanup_coordinator.cleanup_authorization_failure(
                request,
                authorization_payload=error.daemon_payload,
                target_kind=cleanup_target_kind,
            )
        except WorkerCleanupError as cleanup_exc:
            return WorkerTransferLifecycleRecord(
                authorization_request=request,
                cleanup_target_kind=cleanup_target_kind,
                cleanup_target_id=cleanup_target_id,
                final_state="cleanup_failed",
                error=str(cleanup_exc),
            )
        return WorkerTransferLifecycleRecord(
            authorization_request=request,
            cleanup_target_kind=cleanup_target_kind,
            cleanup_target_id=cleanup_target_id,
            cleanup_response=cleanup_response,
            final_state="authorization_failed",
            error=str(error),
        )

    def _running_status_failure_lifecycle_record(
        self,
        *,
        request: WorkerTransferAuthorizationRequest,
        worker_request: WorkerTransferRequest,
        staging_slot: WorkerStagingSlot,
        running_update: dict[str, object] | None,
        running_response: DaemonResponse | None,
        error: WorkerStatusReportError,
        cleanup_target_kind: str,
    ) -> WorkerTransferLifecycleRecord:
        staging_release = self._staging_pool.release(
            staging_slot.slot_id,
            worker_request.data_plane,
        )
        cleanup_target_id = cleanup_target_id_for_worker_request(
            cleanup_target_kind,
            worker_request,
        )
        try:
            cleanup_response = self._cleanup_coordinator.cleanup_status_report_failure(
                worker_request,
                target_kind=cleanup_target_kind,
            )
        except WorkerCleanupError as cleanup_exc:
            return WorkerTransferLifecycleRecord(
                authorization_request=request,
                worker_request=worker_request,
                staging_slot=staging_slot,
                running_update=running_update,
                running_response=running_response,
                staging_release=staging_release,
                cleanup_target_kind=cleanup_target_kind,
                cleanup_target_id=cleanup_target_id,
                final_state="cleanup_failed",
                error=str(cleanup_exc),
            )
        return WorkerTransferLifecycleRecord(
            authorization_request=request,
            worker_request=worker_request,
            staging_slot=staging_slot,
            running_update=running_update,
            running_response=running_response,
            staging_release=staging_release,
            cleanup_target_kind=cleanup_target_kind,
            cleanup_target_id=cleanup_target_id,
            cleanup_response=cleanup_response,
            final_state="status_failed",
            error=str(error),
        )

    def _terminal_status_failure_lifecycle_record(
        self,
        *,
        request: WorkerTransferAuthorizationRequest,
        worker_request: WorkerTransferRequest,
        staging_slot: WorkerStagingSlot,
        running_update: dict[str, object] | None,
        running_response: DaemonResponse | None,
        result: WorkerTransferResult,
        status_update: dict[str, object],
        error: WorkerStatusReportError,
        cleanup_target_kind: str,
    ) -> WorkerTransferLifecycleRecord:
        staging_release = self._staging_pool.release(
            staging_slot.slot_id,
            worker_request.data_plane,
        )
        cleanup_target_id = cleanup_target_id_for_worker_request(
            cleanup_target_kind,
            worker_request,
        )
        try:
            cleanup_response = self._cleanup_coordinator.cleanup_status_report_failure(
                worker_request,
                target_kind=cleanup_target_kind,
            )
        except WorkerCleanupError as cleanup_exc:
            return WorkerTransferLifecycleRecord(
                authorization_request=request,
                worker_request=worker_request,
                staging_slot=staging_slot,
                running_update=running_update,
                running_response=running_response,
                staging_release=staging_release,
                result=result,
                status_update=status_update,
                cleanup_target_kind=cleanup_target_kind,
                cleanup_target_id=cleanup_target_id,
                final_state="cleanup_failed",
                error=str(cleanup_exc),
            )
        result = _worker_result_with_cleanup_completion_evidence(
            worker_request,
            result,
            cleanup_response,
        )
        try:
            status_response = self._report_cleanup_evidence(
                worker_request,
                result,
                cleanup_response,
                current_status_response=cleanup_response,
            )
        except WorkerStatusReportError as report_exc:
            return WorkerTransferLifecycleRecord(
                authorization_request=request,
                worker_request=worker_request,
                staging_slot=staging_slot,
                running_update=running_update,
                running_response=running_response,
                staging_release=staging_release,
                result=result,
                status_update=status_update,
                cleanup_response=cleanup_response,
                cleanup_target_kind=cleanup_target_kind,
                cleanup_target_id=cleanup_target_id,
                final_state="status_failed",
                error=str(report_exc),
            )
        return WorkerTransferLifecycleRecord(
            authorization_request=request,
            worker_request=worker_request,
            staging_slot=staging_slot,
            running_update=running_update,
            running_response=running_response,
            staging_release=staging_release,
            result=result,
            status_update=status_update,
            status_response=status_response,
            cleanup_target_kind=cleanup_target_kind,
            cleanup_target_id=cleanup_target_id,
            cleanup_response=cleanup_response,
            final_state="status_failed",
            error=str(error),
        )

    def _execution_cleanup_failure_lifecycle_record(
        self,
        *,
        request: WorkerTransferAuthorizationRequest,
        worker_request: WorkerTransferRequest,
        staging_slot: WorkerStagingSlot,
        running_update: dict[str, object] | None,
        running_response: DaemonResponse | None,
        result: WorkerTransferResult,
        status_update: dict[str, object],
        status_response: DaemonResponse | None,
        cleanup_target_kind: str,
        cleanup_target_id: str,
        error: WorkerCleanupError,
    ) -> WorkerTransferLifecycleRecord:
        staging_release = self._staging_pool.release(
            staging_slot.slot_id,
            worker_request.data_plane,
        )
        return WorkerTransferLifecycleRecord(
            authorization_request=request,
            worker_request=worker_request,
            staging_slot=staging_slot,
            running_update=running_update,
            running_response=running_response,
            staging_release=staging_release,
            result=result,
            status_update=status_update,
            status_response=status_response,
            cleanup_target_kind=cleanup_target_kind,
            cleanup_target_id=cleanup_target_id,
            final_state="cleanup_failed",
            error=str(error),
        )

    def _terminal_without_status_lifecycle_record(
        self,
        *,
        request: WorkerTransferAuthorizationRequest,
        worker_request: WorkerTransferRequest,
        staging_slot: WorkerStagingSlot,
        running_update: dict[str, object] | None,
        running_response: DaemonResponse | None,
        result: WorkerTransferResult,
        cleanup_target_kind: str,
        cleanup_target_id: str,
        cleanup_response: DaemonResponse,
    ) -> WorkerTransferLifecycleRecord:
        staging_release = self._staging_pool.release(
            staging_slot.slot_id,
            worker_request.data_plane,
        )
        return WorkerTransferLifecycleRecord(
            authorization_request=request,
            worker_request=worker_request,
            staging_slot=staging_slot,
            running_update=running_update,
            running_response=running_response,
            staging_release=staging_release,
            result=result,
            cleanup_target_kind=cleanup_target_kind,
            cleanup_target_id=cleanup_target_id,
            cleanup_response=cleanup_response,
            final_state=result.state.value,
            error=result.error,
        )

    def _completed_cleanup_lifecycle_record(
        self,
        *,
        request: WorkerTransferAuthorizationRequest,
        worker_request: WorkerTransferRequest,
        staging_slot: WorkerStagingSlot,
        running_update: dict[str, object] | None,
        running_response: DaemonResponse | None,
        result: WorkerTransferResult,
        status_update: dict[str, object],
        status_response: DaemonResponse,
        cleanup_target_kind: str,
        cleanup_target_id: str,
        cleanup_response: DaemonResponse,
    ) -> WorkerTransferLifecycleRecord:
        staging_release = self._staging_pool.release(
            staging_slot.slot_id,
            worker_request.data_plane,
        )
        return WorkerTransferLifecycleRecord(
            authorization_request=request,
            worker_request=worker_request,
            staging_slot=staging_slot,
            running_update=running_update,
            running_response=running_response,
            staging_release=staging_release,
            result=result,
            status_update=status_update,
            status_response=status_response,
            cleanup_target_kind=cleanup_target_kind,
            cleanup_target_id=cleanup_target_id,
            cleanup_response=cleanup_response,
            final_state=result.state.value,
            error=result.error,
        )

    def _report_cleanup_evidence(
        self,
        worker_request: WorkerTransferRequest,
        result: WorkerTransferResult,
        cleanup_response: DaemonResponse,
        *,
        current_status_response: DaemonResponse,
    ) -> DaemonResponse:
        if result.state not in {WorkerTransferState.COMPLETE, WorkerTransferState.FAILED}:
            return current_status_response
        evidence = _cleanup_completion_evidence_for_status(
            worker_request,
            result,
            cleanup_response,
        )
        status_update = daemon_status_update_for_result(result)
        try:
            return self._status_reporter.daemon_client.transfer_status(
                result.transfer_id,
                state=status_update["state"],
                bytes_completed=status_update["bytes_completed"],
                error=status_update["error"],
                completion_source="worker",
                completion_evidence=evidence,
            )
        except WorkerStatusReportError:
            raise
        except Exception as exc:
            raise WorkerStatusReportError(str(exc)) from exc

    def _execute(
        self,
        worker_request: WorkerTransferRequest,
        staging_slot: WorkerStagingSlot,
    ) -> WorkerTransferResult:
        if "ticket_authorized_at" in worker_request.data_plane.metadata:
            worker_validation.validate_daemon_issued_ticket(
                worker_request.ticket,
                now=time.time(),
            )
        _trace_worker_stage(
            "worker_async_pool_submit_start",
            transfer_id=worker_request.transfer_id,
        )
        execution = self._execution_pool.submit(worker_request, staging_slot)
        _trace_worker_stage(
            "worker_async_pool_submit_done",
            transfer_id=worker_request.transfer_id,
            pool_ticket=execution.pool_ticket,
        )
        try:
            result = self._execution_pool.wait(execution)
        except Exception as exc:
            result = _failed_worker_result_from_async_pool_exception(
                worker_request,
                staging_slot,
                execution,
                exc,
            )
        _trace_worker_stage(
            "worker_async_pool_wait_done",
            transfer_id=worker_request.transfer_id,
            pool_ticket=execution.pool_ticket,
            state=result.state.value,
        )
        return result


class WorkerTransferService:
    def __init__(
        self,
        daemon_client,
        transfer_client: WorkerTransferClient | None = None,
    ) -> None:
        self.transfer_client = transfer_client or WorkerTransferClient(daemon_client)

    def handle_lifecycle(
        self,
        request: WorkerTransferAuthorizationRequest,
        cleanup_target_kind: str = "reservation",
        report_terminal_status: bool = True,
    ) -> WorkerTransferLifecycleRecord:
        if not isinstance(request, WorkerTransferAuthorizationRequest):
            raise TypeError("request must be a WorkerTransferAuthorizationRequest")
        if str(cleanup_target_kind) != "reservation":
            raise ValueError("worker service cleanup target must be reservation")
        return self.transfer_client.submit_report_cleanup_lifecycle(
            request,
            cleanup_target_kind=cleanup_target_kind,
            report_terminal_status=bool(report_terminal_status),
        )

    def parse_authorization_request(
        self,
        payload: Mapping[str, object],
    ) -> WorkerTransferAuthorizationRequest:
        return parse_worker_authorization_request_payload(payload)

    def handle_envelope(
        self,
        envelope: WorkerServiceRequestEnvelope | Mapping[str, object],
    ) -> WorkerServiceResponseEnvelope:
        try:
            request_envelope = (
                envelope
                if isinstance(envelope, WorkerServiceRequestEnvelope)
                else WorkerServiceRequestEnvelope(
                    payload=envelope.get("payload", envelope),
                    cleanup_target_kind=str(
                        envelope.get("cleanup_target_kind", "reservation")
                    ),
                    report_terminal_status=bool(
                        envelope.get("report_terminal_status", True)
                    ),
                )
            )
            lifecycle = self.handle_lifecycle(
                self.parse_authorization_request(request_envelope.payload),
                cleanup_target_kind=request_envelope.cleanup_target_kind,
                report_terminal_status=request_envelope.report_terminal_status,
            )
            return WorkerServiceResponseEnvelope.from_lifecycle(lifecycle)
        except (KeyError, TypeError, ValueError) as exc:
            return WorkerServiceResponseEnvelope.from_error(str(exc))

    def submit_report_cleanup_lifecycle(
        self,
        request: WorkerTransferAuthorizationRequest,
        cleanup_target_kind: str = "reservation",
        report_terminal_status: bool = True,
    ) -> WorkerTransferLifecycleRecord:
        return self.transfer_client.submit_report_cleanup_lifecycle(
            request,
            cleanup_target_kind=cleanup_target_kind,
            report_terminal_status=bool(report_terminal_status),
        )

    def handle_envelope_payload(
        self,
        envelope: WorkerServiceRequestEnvelope | Mapping[str, object],
    ) -> dict[str, object]:
        return self.handle_envelope(envelope).as_dict()


def parse_worker_authorization_request_payload(
    payload: Mapping[str, object],
) -> WorkerTransferAuthorizationRequest:
    if not isinstance(payload, Mapping):
        raise ValueError("worker authorization payload must be a mapping")
    authorization_payload = payload.get("authorization_request", payload)
    if not isinstance(authorization_payload, Mapping):
        raise ValueError("worker authorization payload must be a mapping")
    try:
        return WorkerTransferAuthorizationRequest(
            transfer_id=str(authorization_payload["transfer_id"]),
            lease_id=str(authorization_payload["lease_id"]),
            token=str(authorization_payload["token"]),
            session_id=str(authorization_payload["session_id"]),
            job_id=str(authorization_payload["job_id"]),
            src_buffer_id=str(authorization_payload["src_buffer_id"]),
            dst_buffer_id=str(authorization_payload["dst_buffer_id"]),
            direction=str(authorization_payload["direction"]),
            ranges=tuple(authorization_payload.get("ranges", ())),
            relay_gpu=authorization_payload.get("relay_gpu"),
        )
    except KeyError as exc:
        raise ValueError(f"missing worker authorization field: {exc.args[0]}") from exc


def validate_worker_completion_bytes(
    request: WorkerTransferRequest,
    result: WorkerTransferResult,
) -> WorkerTransferResult:
    if not isinstance(request, WorkerTransferRequest):
        raise TypeError("request must be a WorkerTransferRequest")
    if not isinstance(result, WorkerTransferResult):
        raise TypeError("result must be a WorkerTransferResult")
    if result.state != WorkerTransferState.COMPLETE:
        return _worker_result_with_ticket_binding(request, result)
    expected_bytes = expected_worker_completion_bytes(request)
    if result.bytes_completed == expected_bytes:
        return _worker_result_with_ticket_binding(request, result)
    reported_bytes = int(result.bytes_completed)
    safe_completed = min(reported_bytes, expected_bytes)
    failed = WorkerTransferResult(
        transfer_id=result.transfer_id,
        state=WorkerTransferState.FAILED,
        error=(
            "worker completed "
            f"{reported_bytes} of {expected_bytes} daemon-planned bytes"
        ),
        bytes_completed=safe_completed,
        metadata={
            **dict(result.metadata),
            "completion_validation": "planned_bytes_mismatch",
            "expected_bytes": expected_bytes,
            "reported_bytes": reported_bytes,
        },
    )
    return _worker_result_with_ticket_binding(request, failed)


def _worker_running_result(
    request: WorkerTransferRequest,
    staging_slot: WorkerStagingSlot,
) -> WorkerTransferResult:
    return _worker_result_with_ticket_binding(
        request,
        WorkerTransferResult(
            transfer_id=request.transfer_id,
            state=WorkerTransferState.RUNNING,
            bytes_completed=0,
            metadata={
                "relay_gpu": request.authorization.relay_gpu,
                "relay_gpus": worker_validation.authorized_relay_gpus_for_request(
                    request
                ),
                "src_buffer_id": request.authorization.src_buffer.buffer_id,
                "dst_buffer_id": request.authorization.dst_buffer.buffer_id,
                "staging_slot_id": staging_slot.slot_id,
            },
        ),
    )


def _worker_result_with_ticket_binding(
    request: WorkerTransferRequest,
    result: WorkerTransferResult,
) -> WorkerTransferResult:
    metadata = dict(result.metadata)
    metadata.setdefault("ticket_id", request.ticket.ticket_id)
    transfer_id = request.ticket.metadata.get("transfer_id")
    if transfer_id is not None:
        metadata.setdefault("transfer_id", str(transfer_id))
    plan_generation = request.ticket.metadata.get("plan_generation")
    if plan_generation is not None:
        metadata.setdefault("plan_generation", int(plan_generation))
    owner_binding = request.data_plane.metadata.get("owner_binding")
    if isinstance(owner_binding, Mapping):
        metadata.setdefault("owner_binding", dict(owner_binding))
    block_runtime = request.ticket.metadata.get("block_runtime")
    if isinstance(block_runtime, Mapping):
        metadata.setdefault("block_runtime", dict(block_runtime))
    block_progress = _worker_block_progress_evidence(request, result)
    if block_progress is not None:
        metadata.setdefault("block_progress", block_progress)
    evidence = metadata.get("completion_evidence")
    if isinstance(evidence, Mapping):
        completion_evidence = dict(evidence)
        for key in (
            "ticket_id",
            "transfer_id",
            "plan_generation",
            "owner_binding",
            "block_runtime",
            "block_progress",
        ):
            if key in metadata:
                completion_evidence.setdefault(key, metadata[key])
        metadata["completion_evidence"] = completion_evidence
    return WorkerTransferResult(
        transfer_id=result.transfer_id,
        state=result.state,
        error=result.error,
        bytes_completed=result.bytes_completed,
        metadata=metadata,
    )


def _worker_result_with_resource_close_evidence(
    result: WorkerTransferResult,
    resources: WorkerDataPlaneResources | None,
) -> WorkerTransferResult:
    if resources is None:
        return result
    metadata = dict(result.metadata)
    resource_evidence = dict(metadata.get("resource_evidence") or {})
    close_evidence = resources.close_evidence()
    cuda_ipc_lifecycle = resources.cuda_ipc_lifecycle()
    resource_evidence["close_evidence"] = close_evidence
    resource_evidence["cuda_ipc_lifecycle"] = cuda_ipc_lifecycle
    metadata["resource_evidence"] = resource_evidence
    metadata["cuda_ipc_lifecycle"] = cuda_ipc_lifecycle
    evidence = metadata.get("completion_evidence")
    if isinstance(evidence, Mapping):
        completion_evidence = dict(evidence)
        completion_resource_evidence = dict(
            completion_evidence.get("resource_evidence") or {}
        )
        completion_resource_evidence["close_evidence"] = close_evidence
        completion_resource_evidence["cuda_ipc_lifecycle"] = cuda_ipc_lifecycle
        completion_evidence["resource_evidence"] = completion_resource_evidence
        completion_evidence["cuda_ipc_lifecycle"] = cuda_ipc_lifecycle
        metadata["completion_evidence"] = completion_evidence
    return WorkerTransferResult(
        transfer_id=result.transfer_id,
        state=result.state,
        error=result.error,
        bytes_completed=result.bytes_completed,
        metadata=metadata,
    )


def _worker_resource_lifecycle_retention_evidence(
    request: WorkerTransferRequest,
    result: WorkerTransferResult,
) -> dict[str, object] | None:
    metadata = dict(result.metadata)
    resource_evidence = metadata.get("resource_evidence")
    cuda_ipc_lifecycle = metadata.get("cuda_ipc_lifecycle")
    completion_evidence = metadata.get("completion_evidence")
    if isinstance(completion_evidence, Mapping):
        if not isinstance(resource_evidence, Mapping):
            resource_evidence = completion_evidence.get("resource_evidence")
        if not isinstance(cuda_ipc_lifecycle, Mapping):
            cuda_ipc_lifecycle = completion_evidence.get("cuda_ipc_lifecycle")
    if not isinstance(resource_evidence, Mapping) and not isinstance(
        cuda_ipc_lifecycle,
        Mapping,
    ):
        return None
    retention = {
        "source": "worker_resource_lifecycle_retention",
        "transfer_id": request.transfer_id,
        "ticket_id": request.ticket.ticket_id,
        "plan_generation": int(request.ticket.metadata.get("plan_generation", 0) or 0),
        "state": result.state.value,
        "bytes_completed": int(result.bytes_completed),
        "source_buffer_id": request.ticket.source_buffer_id,
        "destination_buffer_id": request.ticket.destination_buffer_id,
        "owner_binding": dict(request.data_plane.metadata.get("owner_binding", {})),
    }
    if isinstance(resource_evidence, Mapping):
        retention["resource_evidence"] = dict(resource_evidence)
        close_evidence = resource_evidence.get("close_evidence")
        if isinstance(close_evidence, Mapping):
            retention["resource_close_evidence"] = dict(close_evidence)
    if isinstance(cuda_ipc_lifecycle, Mapping):
        retention["cuda_ipc_lifecycle"] = dict(cuda_ipc_lifecycle)
    cleanup = metadata.get("cleanup")
    if isinstance(cleanup, Mapping):
        retention["cleanup"] = dict(cleanup)
    return retention


def _worker_result_with_cleanup_completion_evidence(
    request: WorkerTransferRequest,
    result: WorkerTransferResult,
    cleanup_response: DaemonResponse,
) -> WorkerTransferResult:
    if result.state not in {WorkerTransferState.COMPLETE, WorkerTransferState.FAILED}:
        return result
    evidence = _cleanup_completion_evidence_for_status(
        request,
        result,
        cleanup_response,
    )
    metadata = dict(result.metadata)
    metadata["completion_evidence"] = evidence
    cleanup = evidence.get("cleanup")
    if isinstance(cleanup, Mapping):
        metadata["cleanup"] = dict(cleanup)
    return WorkerTransferResult(
        transfer_id=result.transfer_id,
        state=result.state,
        error=result.error,
        bytes_completed=result.bytes_completed,
        metadata=metadata,
    )


def _cleanup_completion_evidence_for_status(
    request: WorkerTransferRequest,
    result: WorkerTransferResult,
    cleanup_response: DaemonResponse,
) -> dict[str, object]:
    metadata = dict(result.metadata)
    evidence = metadata.get("completion_evidence")
    if isinstance(evidence, Mapping) and isinstance(evidence.get("cleanup"), Mapping):
        return dict(evidence)
    return _cleanup_completion_evidence(
        request,
        result,
        cleanup_response,
    )


def _worker_result_with_startup_evidence(
    result: WorkerTransferResult,
    worker_startup_evidence: Mapping[str, object] | None,
) -> WorkerTransferResult:
    if worker_startup_evidence is None:
        return result
    startup = dict(worker_startup_evidence)
    metadata = dict(result.metadata)
    metadata["worker_startup"] = startup
    evidence = metadata.get("completion_evidence")
    if isinstance(evidence, Mapping):
        completion_evidence = dict(evidence)
        completion_evidence["worker_startup"] = startup
        metadata["completion_evidence"] = completion_evidence
    return WorkerTransferResult(
        transfer_id=result.transfer_id,
        state=result.state,
        error=result.error,
        bytes_completed=result.bytes_completed,
        metadata=metadata,
    )


def _execute_worker_transfer_once(
    *,
    executor,
    resource_binder: WorkerDataPlaneResourceBinder | None,
    worker_startup_evidence: Mapping[str, object] | None,
    worker_request: WorkerTransferRequest,
    staging_slot: WorkerStagingSlot,
) -> WorkerTransferResult:
    if resource_binder is None:
        return _worker_result_with_startup_evidence(
            executor.execute(worker_request, staging_slot),
            worker_startup_evidence,
        )
    binding = resource_binder.bind(worker_request)
    _trace_worker_stage(
        "worker_resource_bind_start",
        transfer_id=worker_request.transfer_id,
    )
    resources = None
    result: WorkerTransferResult | None = None
    execution_error: Exception | None = None
    try:
        with binding as bound_resources:
            resources = bound_resources
            _trace_worker_stage(
                "worker_resource_bind_done",
                transfer_id=worker_request.transfer_id,
            )
            try:
                _trace_worker_stage(
                    "worker_executor_submit_start",
                    transfer_id=worker_request.transfer_id,
                )
                submitted = submit_worker_transfer(
                    executor,
                    worker_request,
                    staging_slot,
                    resources,
                )
                _trace_worker_stage(
                    "worker_executor_submit_done",
                    transfer_id=worker_request.transfer_id,
                    state=_submitted_worker_transfer_state(submitted),
                )
                _trace_worker_stage(
                    "worker_executor_wait_start",
                    transfer_id=worker_request.transfer_id,
                )
                result = wait_worker_transfer(executor, submitted)
                _trace_worker_stage(
                    "worker_executor_wait_done",
                    transfer_id=worker_request.transfer_id,
                    state=result.state.value,
                )
            except Exception as exc:
                execution_error = exc
    except Exception as exc:
        failure_metadata = _resource_binding_failure_metadata(binding)
        result = failed_worker_result_from_exception(
            worker_request,
            staging_slot,
            exc,
            metadata=failure_metadata,
        )
    if result is None:
        if execution_error is None:
            raise RuntimeError("worker execution did not produce a result")
        result = failed_worker_result_from_exception(
            worker_request,
            staging_slot,
            execution_error,
        )
    result = _worker_result_with_resource_close_evidence(result, resources)
    return _worker_result_with_startup_evidence(
        result,
        worker_startup_evidence,
    )


def _worker_result_with_async_pool_evidence(
    result: WorkerTransferResult,
    *,
    pool_ticket: str,
    queued_at: float,
    started_at: float,
    completed_at: float,
    terminal_history_limit: int,
    terminal_history_evictions: int,
) -> WorkerTransferResult:
    metadata = dict(result.metadata)
    evidence = {
        "pool": "worker_async_execution_pool",
        "pool_ticket": str(pool_ticket),
        "state": result.state.value,
        "queued_at": float(queued_at),
        "started_at": float(started_at),
        "completed_at": float(completed_at),
        "queue_wait_ms": max(0.0, (float(started_at) - float(queued_at)) * 1000.0),
        "execution_ms": max(0.0, (float(completed_at) - float(started_at)) * 1000.0),
        "terminal_history_limit": int(terminal_history_limit),
        "terminal_history_evictions": int(terminal_history_evictions),
    }
    metadata["worker_async_pool"] = evidence
    completion_evidence = (
        dict(metadata["completion_evidence"])
        if isinstance(metadata.get("completion_evidence"), Mapping)
        else None
    )
    if completion_evidence is not None:
        completion_evidence["worker_async_pool"] = dict(evidence)
        metadata["completion_evidence"] = completion_evidence
    return WorkerTransferResult(
        transfer_id=result.transfer_id,
        state=result.state,
        error=result.error,
        bytes_completed=result.bytes_completed,
        metadata=metadata,
    )


def _worker_terminal_history_limit(executor: object) -> int:
    options = getattr(executor, "options", None)
    return int(getattr(options, "worker_terminal_history_entries", 128))


def _failed_worker_result_from_async_pool_exception(
    worker_request: WorkerTransferRequest,
    staging_slot: WorkerStagingSlot,
    execution: WorkerAsyncExecution,
    exc: Exception,
) -> WorkerTransferResult:
    evidence = execution.evidence(state="failed")
    result = failed_worker_result_from_exception(
        worker_request,
        staging_slot,
        exc,
        metadata={
            "failure_source": "worker_async_execution_pool",
            "worker_async_pool": evidence,
        },
    )
    metadata = dict(result.metadata)
    metadata["worker_async_pool"] = evidence
    completion_evidence = (
        dict(metadata["completion_evidence"])
        if isinstance(metadata.get("completion_evidence"), Mapping)
        else {}
    )
    completion_evidence["worker_async_pool"] = dict(evidence)
    owner_binding = metadata.get("owner_binding")
    if isinstance(owner_binding, Mapping):
        completion_evidence.setdefault("owner_binding", dict(owner_binding))
    metadata["completion_evidence"] = completion_evidence
    return WorkerTransferResult(
        transfer_id=result.transfer_id,
        state=result.state,
        error=result.error,
        bytes_completed=result.bytes_completed,
        metadata=metadata,
    )


def expected_worker_completion_bytes(request: WorkerTransferRequest) -> int:
    total_bytes = sum(int(item["bytes"]) for item in request.data_plane.ranges)
    if total_bytes <= 0:
        raise ValueError("daemon worker plan has no bytes to complete")
    return total_bytes


def require_daemon_worker_plan(request: WorkerTransferRequest) -> None:
    plan = request.data_plane.plan
    if not plan:
        raise ValueError("daemon worker authorization did not include a transfer plan")
    assignments = plan.get("assignments")
    if not assignments:
        raise ValueError("daemon worker authorization plan has no assignments")

    relay_gpus = worker_validation.authorized_relay_gpus_for_request(request)
    direction = request.data_plane.direction
    target_handle = (
        request.data_plane.dst_handle
        if direction == "h2d"
        else request.data_plane.src_handle
    )
    if target_handle.device_index is None:
        raise ValueError("worker target handle requires a CUDA device index")
    target_device = int(target_handle.device_index)
    execution_ranges: list[dict[str, int]] = []
    full_plan_total_bytes = 0
    plan_total_bytes = 0
    for assignment in assignments:
        if not isinstance(assignment, Mapping):
            raise ValueError("daemon plan assignment must be an object")
        path = assignment.get("path")
        if not isinstance(path, Mapping):
            raise ValueError("daemon plan assignment path must be an object")
        path_kind = str(path.get("kind", "")).lower()
        if path_kind not in {"direct", "relay"}:
            raise ValueError("daemon plan path must be direct or relay")
        if str(path.get("direction", "")).lower() != direction:
            raise ValueError("daemon plan direction does not match worker request")
        if int(path.get("target_device", target_device)) != target_device:
            raise ValueError("daemon plan target does not match worker device")
        if not bool(path.get("enabled", True)):
            raise ValueError("daemon plan path is disabled")
        assignment_chunks = assignment.get("chunks", ()) or ()
        for chunk in assignment_chunks:
            if not isinstance(chunk, Mapping):
                raise ValueError("daemon plan chunk must be an object")
            full_plan_total_bytes += int(chunk["bytes"])
        if path_kind == "direct":
            continue
        else:
            if int(path.get("relay_device", -1)) not in relay_gpus:
                raise ValueError("daemon plan relay is not authorized by worker ticket")
            chunks = assignment_chunks
        for chunk in chunks:
            if not isinstance(chunk, Mapping):
                raise ValueError("daemon plan chunk must be an object")
            chunk_payload = {
                "src_offset": int(chunk["src_offset"]),
                "dst_offset": int(chunk["dst_offset"]),
                "bytes": int(chunk["bytes"]),
            }
            plan_total_bytes += int(chunk_payload["bytes"])
            execution_ranges.append(chunk_payload)
    if plan_total_bytes <= 0:
        raise ValueError("daemon plan has no assigned bytes")
    declared_full_bytes = int(plan.get("total_bytes", -1))
    if declared_full_bytes != full_plan_total_bytes:
        raise ValueError("daemon plan total bytes do not match assigned chunks")
    declared_total_bytes = sum(int(item["bytes"]) for item in request.data_plane.ranges)
    if declared_total_bytes != plan_total_bytes:
        raise ValueError("daemon plan total bytes do not match assigned chunks")
    if not execution_ranges:
        raise ValueError("daemon plan has no authorized executable chunks")
    if tuple(execution_ranges) != request.data_plane.ranges:
        raise ValueError("authorized ranges do not match daemon plan")


def failed_worker_result_from_exception(
    worker_request: WorkerTransferRequest,
    staging_slot: WorkerStagingSlot,
    exc: Exception,
    *,
    metadata: Mapping[str, object] | None = None,
) -> WorkerTransferResult:
    failure_metadata = {} if metadata is None else dict(metadata)
    resource_evidence = None if not failure_metadata else dict(failure_metadata)
    failure_source = failure_metadata.get("failure_source")
    return _worker_result_with_ticket_binding(
        worker_request,
        WorkerTransferResult(
            transfer_id=worker_request.transfer_id,
            state=WorkerTransferState.FAILED,
            error=str(exc) or exc.__class__.__name__,
            bytes_completed=0,
            metadata={
                **(
                    {}
                    if failure_source is None
                    else {"failure_source": str(failure_source)}
                ),
                "relay_gpu": worker_request.authorization.relay_gpu,
                "relay_gpus": worker_validation.authorized_relay_gpus_for_request(
                    worker_request
                ),
                "src_buffer_id": worker_request.authorization.src_buffer.buffer_id,
                "dst_buffer_id": worker_request.authorization.dst_buffer.buffer_id,
                "staging_slot_id": staging_slot.slot_id,
                **(
                    {}
                    if resource_evidence is None
                    else {"resource_evidence": resource_evidence}
                ),
            },
        ),
    )


def cleanup_target_id(target_kind: str, lease_id: str, session_id: str) -> str:
    normalized = str(target_kind)
    if normalized == "reservation":
        return str(lease_id)
    if normalized == "session":
        return str(session_id)
    raise ValueError("worker cleanup target must be reservation or session")


def cleanup_target_id_for_request(
    target_kind: str,
    request: WorkerTransferAuthorizationRequest,
) -> str:
    return cleanup_target_id(
        target_kind,
        lease_id=request.lease_id,
        session_id=request.session_id,
    )


def cleanup_target_id_for_worker_request(
    target_kind: str,
    request: WorkerTransferRequest,
) -> str:
    return cleanup_target_id(
        target_kind,
        lease_id=request.authorization.lease_id,
        session_id=request.authorization.session_id,
    )


def execute_worker_transfer(
    executor,
    request: WorkerTransferRequest,
    staging_slot: WorkerStagingSlot,
    resources: WorkerDataPlaneResources,
) -> WorkerTransferResult:
    return wait_worker_transfer(
        executor,
        submit_worker_transfer(executor, request, staging_slot, resources),
    )


def execute_authorized_worker_lifecycle(
    daemon_client,
    request: WorkerTransferAuthorizationRequest | Mapping[str, object],
    *,
    executor: object | None = None,
    resource_binder: WorkerDataPlaneResourceBinder | None = None,
    staging_pool: WorkerStagingPool | None = None,
    execution_pool: WorkerAsyncExecutionPool | None = None,
    worker_startup_evidence: Mapping[str, object] | None = None,
    report_terminal_status: bool = True,
) -> WorkerTransferLifecycleRecord:
    if isinstance(request, WorkerTransferAuthorizationRequest):
        authorization_request = request
    else:
        authorization_request = parse_worker_authorization_request_payload(request)
    client = WorkerTransferClient(
        daemon_client,
        executor=executor,
        resource_binder=resource_binder,
        staging_pool=staging_pool,
        execution_pool=execution_pool,
        worker_startup_evidence=worker_startup_evidence,
    )
    return client.submit_report_cleanup_lifecycle(
        authorization_request,
        cleanup_target_kind="reservation",
        report_terminal_status=bool(report_terminal_status),
    )


def submit_worker_transfer(
    executor,
    request: WorkerTransferRequest,
    staging_slot: WorkerStagingSlot,
    resources: WorkerDataPlaneResources,
):
    submit_bound = getattr(executor, "submit_bound", None)
    if callable(submit_bound):
        return submit_bound(request, staging_slot, resources)
    execute_bound = getattr(executor, "execute_bound", None)
    if callable(execute_bound):
        return execute_bound(request, staging_slot, resources)
    return executor.execute(request, staging_slot)


def wait_worker_transfer(
    executor,
    submitted,
) -> WorkerTransferResult:
    if isinstance(submitted, WorkerTransferResult):
        return submitted
    waiter = getattr(executor, "wait", None)
    if not callable(waiter):
        raise RuntimeError("worker executor returned an asynchronous handle without wait")
    result = waiter(submitted)
    if not isinstance(result, WorkerTransferResult):
        raise TypeError("worker executor wait must return WorkerTransferResult")
    return result


def _submitted_worker_transfer_state(submitted) -> str:
    if isinstance(submitted, WorkerTransferResult):
        return submitted.state.value
    state = getattr(submitted, "state", None)
    if state is None:
        return "submitted"
    try:
        return WorkerTransferState(state).value
    except ValueError:
        return str(state)


def default_worker_executor():
    from .cuda_executor import CudaWorkerExecutor

    return CudaWorkerExecutor()


def _resource_binding_failure_metadata(binding) -> dict[str, object] | None:
    failure_evidence = getattr(binding, "failure_evidence", None)
    if callable(failure_evidence):
        resolved = failure_evidence()
        return None if resolved is None else dict(resolved)
    return None


def _trace_worker_stage(name: str, **fields) -> None:
    if os.environ.get("TURBOBUS_BENCHMARK_TRACE") != "1":
        return
    details = " ".join(f"{key}={value}" for key, value in sorted(fields.items()))
    print(f"turbobus_worker_stage name={name} {details}".rstrip(), flush=True)


__all__ = [
    "WorkerAuthorizationError",
    "WorkerAsyncExecutionPool",
    "WorkerAsyncExecutionPoolError",
    "WorkerCleanupError",
    "WorkerStatusReportError",
    "WorkerTransferClient",
    "WorkerTransferService",
    "cleanup_target_id",
    "default_worker_executor",
    "execute_authorized_worker_lifecycle",
    "execute_worker_transfer",
    "expected_worker_completion_bytes",
    "failed_worker_result_from_exception",
    "parse_worker_authorization_request_payload",
    "require_daemon_worker_plan",
    "submit_worker_transfer",
    "validate_worker_completion_bytes",
    "wait_worker_transfer",
]
