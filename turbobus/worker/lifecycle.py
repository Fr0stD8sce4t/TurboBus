from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict
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


class _WorkerTransferAuthorizer:
    def __init__(self, daemon_client) -> None:
        self.daemon_client = daemon_client

    def _authorize(
        self,
        request: WorkerTransferAuthorizationRequest,
    ) -> WorkerTransferRequest:
        response: DaemonResponse = self.daemon_client.authorize_worker_transfer(request)
        if not response.ok:
            raise WorkerAuthorizationError(
                response.error or "worker transfer authorization failed"
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


class _WorkerTransferStatusReporter:
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


def _status_evidence_for_result(
    result: WorkerTransferResult,
) -> dict[str, object] | None:
    metadata = dict(result.metadata)
    if result.state is not WorkerTransferState.COMPLETE:
        evidence = {
            key: metadata[key]
            for key in (
                "ticket_id",
                "transfer_id",
                "plan_generation",
                "resource_evidence",
            )
            if key in metadata
        }
        return evidence or None
    evidence = metadata.get("completion_evidence")
    if isinstance(evidence, Mapping):
        completion_evidence = dict(evidence)
        for key in ("ticket_id", "transfer_id", "plan_generation"):
            if key in metadata:
                completion_evidence.setdefault(key, metadata[key])
        return completion_evidence
    evidence_keys = {
        "executor",
        "path",
        "plan_source",
        "target_device",
        "verified_bytes",
        "content_match",
        "verification_source",
        "verification_method",
        "source_digest",
        "destination_digest",
        "direct_bytes",
        "direct_chunks",
        "relay_bytes",
        "relay_chunks",
        "relay_gpu",
        "relay_gpus",
        "src_buffer_id",
        "dst_buffer_id",
        "staging_slot_id",
        "resource_evidence",
        "ticket_id",
        "transfer_id",
        "plan_generation",
    }
    if not any(key in metadata for key in evidence_keys):
        return None
    return {key: metadata[key] for key in evidence_keys if key in metadata}


class _WorkerTransferCleanupCoordinator:
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
            lease_ids, session_id = self._authorized_cleanup_targets(
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
        if target_kind == "session":
            return self._cleanup(
                target_kind=target_kind,
                target_id=session_id,
                reason=reason,
                force=force,
            )
        if len(lease_ids) == 1:
            target_id = cleanup_target_id(
                target_kind,
                lease_id=lease_ids[0],
                session_id=session_id,
            )
            return self._cleanup(
                target_kind=target_kind,
                target_id=target_id,
                reason=reason,
                force=force,
            )
        return self._cleanup_lease_ids(
            lease_ids=lease_ids,
            target_kind=target_kind,
            reason=reason,
            force=force,
        )

    def _authorized_cleanup_targets(
        self,
        request: WorkerTransferAuthorizationRequest,
        authorization_payload: Mapping[str, object] | None,
    ) -> tuple[tuple[str, ...], str]:
        if authorization_payload is None:
            raise WorkerCleanupError(
                "authorization failure cleanup requires daemon-issued ticket payload"
            )
        if not isinstance(authorization_payload, Mapping):
            raise WorkerCleanupError("authorization payload must be a mapping")
        ticket_payload = authorization_payload.get("ticket")
        if not isinstance(ticket_payload, Mapping):
            raise WorkerCleanupError(
                "authorization failure cleanup requires daemon-issued ticket"
            )
        try:
            ticket = ExecutionTicket(**dict(ticket_payload))
            worker_validation.validate_daemon_issued_ticket(
                ticket,
                plan_generation=authorization_payload.get("plan_generation"),
            )
            worker_validation.transfer_id_for_ticket(ticket, request.transfer_id)
            if ticket.job_id != request.job_id:
                raise ValueError("ticket job does not match authorization request")
            if ticket.session_id != request.session_id:
                raise ValueError("ticket session does not match authorization request")
            lease_ids = worker_validation.lease_ids_for_ticket(
                ticket,
                lease_id=authorization_payload.get("lease_id"),
                lease_ids=authorization_payload.get("lease_ids"),
            )
        except (TypeError, ValueError) as exc:
            raise WorkerCleanupError(
                f"invalid daemon authorization cleanup payload: {exc}"
            ) from exc
        if not lease_ids:
            raise WorkerCleanupError("daemon authorization cleanup has no lease ids")
        return lease_ids, ticket.session_id

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
        lease_ids = worker_request_lease_ids(request)
        if target_kind == "session":
            return self._cleanup(
                target_kind=target_kind,
                target_id=cleanup_target_id(
                    target_kind,
                    lease_id=request.authorization.lease_id,
                    session_id=request.authorization.session_id,
                ),
                reason=reason or f"worker_{result.state.value}",
                force=force,
            )
        if result.state == WorkerTransferState.COMPLETE:
            if len(lease_ids) == 1:
                return self._cleanup(
                    target_kind=target_kind,
                    target_id=cleanup_target_id(
                        target_kind,
                        lease_id=request.authorization.lease_id,
                        session_id=request.authorization.session_id,
                    ),
                    reason=reason or "worker_complete",
                    force=force,
                )
            return self._cleanup_worker_leases(
                lease_ids=lease_ids,
                target_kind=target_kind,
                reason=reason or "worker_complete",
                force=force,
            )
        if len(lease_ids) == 1:
            return self._cleanup(
                target_kind=target_kind,
                target_id=cleanup_target_id(
                    target_kind,
                    lease_id=request.authorization.lease_id,
                    session_id=request.authorization.session_id,
                ),
                reason=reason or f"worker_{result.state.value}",
                force=force,
            )
        return self._cleanup_worker_leases(
            lease_ids=lease_ids,
            target_kind=target_kind,
            reason=reason or f"worker_{result.state.value}",
            force=force,
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
        lease_ids = worker_request_lease_ids(request)
        if target_kind == "session":
            return self._cleanup(
                target_kind=target_kind,
                target_id=cleanup_target_id(
                    target_kind,
                    lease_id=request.authorization.lease_id,
                    session_id=request.authorization.session_id,
                ),
                reason=reason,
                force=force,
            )
        if len(lease_ids) == 1:
            return self._cleanup(
                target_kind=target_kind,
                target_id=cleanup_target_id(
                    target_kind,
                    lease_id=request.authorization.lease_id,
                    session_id=request.authorization.session_id,
                ),
                reason=reason,
                force=force,
            )
        return self._cleanup_worker_leases(
            lease_ids=lease_ids,
            target_kind=target_kind,
            reason=reason,
            force=force,
        )

    def _cleanup(
        self,
        target_kind: str,
        target_id: str,
        reason: str,
        force: bool,
    ) -> DaemonResponse:
        response: DaemonResponse = self.daemon_client.cleanup(
            target_kind=target_kind,
            target_id=target_id,
            reason=reason,
            force=force,
        )
        if not response.ok:
            raise WorkerCleanupError(response.error or "worker cleanup failed")
        return response

    def _cleanup_worker_leases(
        self,
        *,
        lease_ids: tuple[str, ...],
        target_kind: str,
        reason: str,
        force: bool,
    ) -> DaemonResponse:
        return self._cleanup_lease_ids(
            lease_ids=lease_ids,
            target_kind=target_kind,
            reason=reason,
            force=force,
        )

    def _cleanup_lease_ids(
        self,
        *,
        lease_ids: tuple[str, ...],
        target_kind: str,
        reason: str,
        force: bool,
    ) -> DaemonResponse:
        cleanup = getattr(self.daemon_client, "cleanup", None)
        if not callable(cleanup):
            raise WorkerCleanupError("daemon client cannot clean worker transfer")
        responses: list[dict[str, object]] = []
        cleaned_ids: list[str] = []
        errors: list[str] = []
        for lease_id in lease_ids:
            response = cleanup(
                target_kind=target_kind,
                target_id=lease_id,
                reason=reason,
                force=force,
            )
            responses.append(asdict(response))
            if response.ok:
                payload = response.payload if isinstance(response.payload, Mapping) else {}
                reservation_id = payload.get("reservation_id")
                cleaned_ids.append(
                    str(reservation_id) if reservation_id is not None else str(lease_id)
                )
                continue
            errors.append(f"{lease_id}: {response.error or 'worker cleanup failed'}")
        if errors:
            raise WorkerCleanupError("; ".join(errors))
        payload = {
            "reservation_id": lease_ids[0],
            "lease_ids": lease_ids,
            "cleaned_reservation_ids": tuple(cleaned_ids),
            "lease_responses": tuple(responses),
            "cleanup_kind": target_kind,
            "reason": reason,
            "cleanup_mode": "cleanup",
        }
        return DaemonResponse(ok=True, payload=payload)


class WorkerTransferClient:
    def __init__(
        self,
        daemon_client,
        executor: object | None = None,
        status_reporter: _WorkerTransferStatusReporter | None = None,
        cleanup_coordinator: _WorkerTransferCleanupCoordinator | None = None,
        staging_pool: WorkerStagingPool | None = None,
        resource_binder: WorkerDataPlaneResourceBinder | None = None,
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

    def _authorize(
        self,
        request: WorkerTransferAuthorizationRequest,
    ) -> WorkerTransferRequest:
        return self._authorizer._authorize(request)

    def submit_report_cleanup_lifecycle(
        self,
        request: WorkerTransferAuthorizationRequest,
        cleanup_target_kind: str = "reservation",
        report_terminal_status: bool = True,
    ) -> WorkerTransferLifecycleRecord:
        try:
            worker_request = self._authorize(request)
        except WorkerAuthorizationError as exc:
            cleanup_target_id = cleanup_target_id_for_request(
                cleanup_target_kind,
                request,
            )
            try:
                cleanup_response = self._cleanup_coordinator.cleanup_authorization_failure(
                    request,
                    authorization_payload=exc.daemon_payload,
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
                error=str(exc),
            )
        staging_slot = self._staging_pool.allocate(worker_request.data_plane)
        running_update: dict[str, object] | None = None
        running_response: DaemonResponse | None = None
        try:
            running_update, running_response = self._status_reporter.report_running(
                worker_request,
                staging_slot,
            )
        except WorkerStatusReportError as exc:
            staging_release = self._staging_pool.release(
                staging_slot.slot_id,
                worker_request.data_plane,
            )
            cleanup_target_id = cleanup_target_id_for_worker_request(
                cleanup_target_kind,
                worker_request,
            )
            try:
                cleanup_response = (
                    self._cleanup_coordinator.cleanup_status_report_failure(
                        worker_request,
                        target_kind=cleanup_target_kind,
                    )
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
                error=str(exc),
            )
        try:
            result = validate_worker_completion_bytes(
                worker_request,
                self._execute(worker_request, staging_slot),
            )
        except Exception as exc:
            result = failed_worker_result_from_exception(
                worker_request,
                staging_slot,
                exc,
            )
        status_update = daemon_status_update_for_result(result)
        status_response: DaemonResponse | None = None
        if report_terminal_status:
            try:
                status_response = self._status_reporter.report(result)
            except WorkerStatusReportError as exc:
                staging_release = self._staging_pool.release(
                    staging_slot.slot_id,
                    worker_request.data_plane,
                )
                cleanup_target_id = cleanup_target_id_for_worker_request(
                    cleanup_target_kind,
                    worker_request,
                )
                try:
                    cleanup_response = (
                        self._cleanup_coordinator.cleanup_status_report_failure(
                            worker_request,
                            target_kind=cleanup_target_kind,
                        )
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
                    final_state=result.state.value,
                    error=str(exc),
                )
        elif result.state is WorkerTransferState.FAILED:
            try:
                status_response = self._status_reporter.report(result)
            except WorkerStatusReportError as exc:
                staging_release = self._staging_pool.release(
                    staging_slot.slot_id,
                    worker_request.data_plane,
                )
                cleanup_target_id = cleanup_target_id_for_worker_request(
                    cleanup_target_kind,
                    worker_request,
                )
                try:
                    cleanup_response = (
                        self._cleanup_coordinator.cleanup_status_report_failure(
                            worker_request,
                            target_kind=cleanup_target_kind,
                        )
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
                    cleanup_response=cleanup_response,
                    final_state="status_failed",
                    error=str(exc),
                )
        cleanup_target_id = (
            worker_request.authorization.lease_id
            if result.state == WorkerTransferState.COMPLETE
            else cleanup_target_id_for_worker_request(
                cleanup_target_kind,
                worker_request,
            )
        )
        try:
            cleanup_response = self._cleanup_coordinator.cleanup_execution_failure(
                worker_request,
                result,
                target_kind=cleanup_target_kind,
            )
        except WorkerCleanupError as exc:
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
                error=str(exc),
            )
        if not report_terminal_status and result.state is WorkerTransferState.COMPLETE:
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
        status_response = self._report_cleanup_evidence(
            worker_request,
            result,
            cleanup_response,
            current_status_response=status_response,
        )
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
        evidence = _cleanup_completion_evidence(
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
        if self._resource_binder is None:
            return self._executor.execute(worker_request, staging_slot)
        with self._resource_binder.bind(worker_request) as resources:
            return execute_worker_transfer(
                self._executor,
                worker_request,
                staging_slot,
                resources,
            )


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
    evidence = metadata.get("completion_evidence")
    if isinstance(evidence, Mapping):
        completion_evidence = dict(evidence)
        for key in ("ticket_id", "transfer_id", "plan_generation"):
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


def _cleanup_completion_evidence(
    request: WorkerTransferRequest,
    result: WorkerTransferResult,
    cleanup_response: DaemonResponse,
) -> dict[str, object]:
    metadata = dict(result.metadata)
    evidence = dict(metadata.get("completion_evidence") or {})
    for key in (
        "verified_bytes",
        "content_match",
        "verification_source",
        "verification_method",
        "source_digest",
        "destination_digest",
        "resource_evidence",
        "ticket_id",
        "transfer_id",
        "plan_generation",
    ):
        if key in metadata:
            evidence.setdefault(key, metadata[key])
    evidence.setdefault("ticket_id", request.ticket.ticket_id)
    transfer_id = request.ticket.metadata.get("transfer_id")
    if transfer_id is not None:
        evidence.setdefault("transfer_id", str(transfer_id))
    plan_generation = request.ticket.metadata.get("plan_generation")
    if plan_generation is not None:
        evidence.setdefault("plan_generation", int(plan_generation))
    payload = cleanup_response.payload if isinstance(cleanup_response.payload, Mapping) else {}
    cleanup_payload = dict(payload)
    evidence["cleanup"] = {
        "ok": bool(cleanup_response.ok),
        "target_kind": cleanup_payload.get("cleanup_kind"),
        "target_id": cleanup_payload.get("reservation_id"),
        "mode": cleanup_payload.get("cleanup_mode"),
        "reason": cleanup_payload.get("reason"),
        "lease_ids": tuple(str(item) for item in cleanup_payload.get("lease_ids", ()) or ()),
        "cleaned_reservation_ids": tuple(
            str(item)
            for item in cleanup_payload.get("cleaned_reservation_ids", ()) or ()
        ),
    }
    return evidence


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
    relay_ranges: list[dict[str, int]] = []
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
        if path_kind == "direct":
            chunks = assignment.get("chunks", ()) or ()
        else:
            if int(path.get("relay_device", -1)) not in relay_gpus:
                raise ValueError("daemon plan relay is not authorized by worker ticket")
            chunks = assignment.get("chunks", ()) or ()
        for chunk in chunks:
            if not isinstance(chunk, Mapping):
                raise ValueError("daemon plan chunk must be an object")
            chunk_payload = {
                "src_offset": int(chunk["src_offset"]),
                "dst_offset": int(chunk["dst_offset"]),
                "bytes": int(chunk["bytes"]),
            }
            plan_total_bytes += int(chunk_payload["bytes"])
            if path_kind == "relay":
                relay_ranges.append(chunk_payload)
    if plan_total_bytes <= 0:
        raise ValueError("daemon plan has no assigned bytes")
    declared_total_bytes = int(plan.get("total_bytes", -1))
    if declared_total_bytes != plan_total_bytes:
        raise ValueError("daemon plan total bytes do not match assigned chunks")
    if not relay_ranges:
        raise ValueError("daemon plan has no authorized relay chunks")
    if tuple(relay_ranges) != request.data_plane.ranges:
        raise ValueError("authorized ranges do not match daemon plan")


def failed_worker_result_from_exception(
    worker_request: WorkerTransferRequest,
    staging_slot: WorkerStagingSlot,
    exc: Exception,
) -> WorkerTransferResult:
    return _worker_result_with_ticket_binding(
        worker_request,
        WorkerTransferResult(
            transfer_id=worker_request.transfer_id,
            state=WorkerTransferState.FAILED,
            error=str(exc) or exc.__class__.__name__,
            bytes_completed=0,
            metadata={
                "relay_gpu": worker_request.authorization.relay_gpu,
                "relay_gpus": worker_validation.authorized_relay_gpus_for_request(
                    worker_request
                ),
                "src_buffer_id": worker_request.authorization.src_buffer.buffer_id,
                "dst_buffer_id": worker_request.authorization.dst_buffer.buffer_id,
                "staging_slot_id": staging_slot.slot_id,
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
    execute_bound = getattr(executor, "execute_bound", None)
    if callable(execute_bound):
        return execute_bound(request, staging_slot, resources)
    return executor.execute(request, staging_slot)


def default_worker_executor():
    from .cuda_executor import CudaWorkerExecutor

    return CudaWorkerExecutor()


__all__ = [
    "WorkerAuthorizationError",
    "WorkerCleanupError",
    "WorkerStatusReportError",
    "WorkerTransferClient",
    "WorkerTransferService",
    "cleanup_target_id",
    "default_worker_executor",
    "execute_worker_transfer",
    "expected_worker_completion_bytes",
    "failed_worker_result_from_exception",
    "parse_worker_authorization_request_payload",
    "require_daemon_worker_plan",
    "validate_worker_completion_bytes",
]
