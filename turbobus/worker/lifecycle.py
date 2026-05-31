from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict

from ..schema import DaemonResponse, WorkerTransferAuthorizationRequest
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
    pass


class WorkerStatusReportError(RuntimeError):
    pass


class WorkerCleanupError(RuntimeError):
    pass


class WorkerTransferAuthorizer:
    def __init__(self, daemon_client) -> None:
        self.daemon_client = daemon_client

    def authorize(
        self,
        request: WorkerTransferAuthorizationRequest,
    ) -> WorkerTransferRequest:
        response: DaemonResponse = self.daemon_client.authorize_worker_transfer(request)
        if not response.ok:
            raise WorkerAuthorizationError(
                response.error or "worker transfer authorization failed"
            )
        try:
            worker_request = WorkerTransferRequest.from_authorization_payload(
                response.payload
            )
            require_daemon_worker_plan(worker_request)
            return worker_request
        except (KeyError, TypeError, ValueError) as exc:
            raise WorkerAuthorizationError(
                f"invalid worker authorization response: {exc}"
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
        )
        if not response.ok:
            raise WorkerStatusReportError(
                response.error or "worker transfer status report failed"
            )
        return response


class WorkerTransferCleanupCoordinator:
    def __init__(self, daemon_client) -> None:
        self.daemon_client = daemon_client

    def cleanup_authorization_failure(
        self,
        request: WorkerTransferAuthorizationRequest,
        target_kind: str = "reservation",
        reason: str = "worker_authorization_failed",
        force: bool = True,
    ) -> DaemonResponse:
        if not isinstance(request, WorkerTransferAuthorizationRequest):
            raise TypeError("request must be a WorkerTransferAuthorizationRequest")
        return self._cleanup(
            target_kind=target_kind,
            target_id=cleanup_target_id(
                target_kind,
                lease_id=request.lease_id,
                session_id=request.session_id,
            ),
            reason=reason,
            force=force,
        )

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
                release = getattr(self.daemon_client, "release_transfer", None)
                if not callable(release):
                    raise WorkerCleanupError(
                        "daemon client cannot release completed worker transfer"
                    )
                response: DaemonResponse = release(request.authorization.lease_id)
                if not response.ok:
                    raise WorkerCleanupError(
                        response.error or "worker completion release failed"
                    )
                return response
            return self._cleanup_worker_leases(
                request,
                lease_ids=lease_ids,
                target_kind=target_kind,
                reason=reason or "worker_complete",
                force=force,
                release_completed=True,
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
            request,
            lease_ids=lease_ids,
            target_kind=target_kind,
            reason=reason or f"worker_{result.state.value}",
            force=force,
            release_completed=False,
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
            request,
            lease_ids=lease_ids,
            target_kind=target_kind,
            reason=reason,
            force=force,
            release_completed=False,
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
        request: WorkerTransferRequest,
        *,
        lease_ids: tuple[str, ...],
        target_kind: str,
        reason: str,
        force: bool,
        release_completed: bool,
    ) -> DaemonResponse:
        release = getattr(self.daemon_client, "release_transfer", None)
        cleanup = getattr(self.daemon_client, "cleanup", None)
        if release_completed and not callable(release):
            raise WorkerCleanupError(
                "daemon client cannot release completed worker transfer"
            )
        if not release_completed and not callable(cleanup):
            raise WorkerCleanupError("daemon client cannot clean worker transfer")
        responses: list[dict[str, object]] = []
        released_ids: list[str] = []
        errors: list[str] = []
        for lease_id in lease_ids:
            if release_completed:
                response = release(lease_id)
            else:
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
                released_ids.append(
                    str(reservation_id) if reservation_id is not None else str(lease_id)
                )
                continue
            errors.append(f"{lease_id}: {response.error or 'worker cleanup failed'}")
        if errors:
            raise WorkerCleanupError("; ".join(errors))
        payload = {
            "reservation_id": lease_ids[0],
            "lease_ids": lease_ids,
            "released_reservation_ids": tuple(released_ids),
            "lease_responses": tuple(responses),
            "cleanup_kind": target_kind,
            "reason": reason,
            "cleanup_mode": "release" if release_completed else "cleanup",
        }
        return DaemonResponse(ok=True, payload=payload)


class WorkerTransferClient:
    def __init__(
        self,
        daemon_client,
        executor: object | None = None,
        status_reporter: WorkerTransferStatusReporter | None = None,
        cleanup_coordinator: WorkerTransferCleanupCoordinator | None = None,
        staging_pool: WorkerStagingPool | None = None,
        resource_binder: WorkerDataPlaneResourceBinder | None = None,
    ) -> None:
        if executor is None:
            executor = default_worker_executor()
            if resource_binder is None:
                resource_binder = WorkerDataPlaneResourceBinder()
        self.authorizer = WorkerTransferAuthorizer(daemon_client)
        self.executor = executor
        self.status_reporter = status_reporter or WorkerTransferStatusReporter(
            daemon_client
        )
        self.cleanup_coordinator = cleanup_coordinator or WorkerTransferCleanupCoordinator(
            daemon_client
        )
        self.staging_pool = staging_pool or WorkerStagingPool()
        self.resource_binder = resource_binder

    def authorize(
        self,
        request: WorkerTransferAuthorizationRequest,
    ) -> WorkerTransferRequest:
        return self.authorizer.authorize(request)

    def submit(
        self,
        request: WorkerTransferAuthorizationRequest,
    ) -> WorkerTransferResult:
        worker_request = self.authorize(request)
        staging_slot = self.staging_pool.allocate(worker_request.data_plane)
        try:
            try:
                result = self._execute(worker_request, staging_slot)
            except Exception as exc:
                result = failed_worker_result_from_exception(
                    worker_request,
                    staging_slot,
                    exc,
                )
            return validate_worker_completion_bytes(worker_request, result)
        finally:
            self.staging_pool.release(staging_slot.slot_id, worker_request.data_plane)

    def submit_and_report(
        self,
        request: WorkerTransferAuthorizationRequest,
    ) -> WorkerTransferResult:
        result = self.submit(request)
        self.status_reporter.report(result)
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
        if lifecycle.final_state == "authorization_failed":
            raise WorkerAuthorizationError(
                lifecycle.error or "worker transfer authorization failed"
            )
        if lifecycle.final_state == "status_failed":
            raise WorkerStatusReportError(
                lifecycle.error or "worker transfer status report failed"
            )
        if lifecycle.final_state == "cleanup_failed":
            raise WorkerCleanupError(lifecycle.error or "worker cleanup failed")
        if lifecycle.result is None:
            raise RuntimeError("worker lifecycle completed without a result")
        return lifecycle.result

    def submit_report_cleanup_lifecycle(
        self,
        request: WorkerTransferAuthorizationRequest,
        cleanup_target_kind: str = "reservation",
    ) -> WorkerTransferLifecycleRecord:
        try:
            worker_request = self.authorize(request)
        except WorkerAuthorizationError as exc:
            cleanup_target_id = cleanup_target_id_for_request(
                cleanup_target_kind,
                request,
            )
            try:
                cleanup_response = self.cleanup_coordinator.cleanup_authorization_failure(
                    request,
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
        staging_slot = self.staging_pool.allocate(worker_request.data_plane)
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
        try:
            status_response = self.status_reporter.report(result)
        except WorkerStatusReportError as exc:
            staging_release = self.staging_pool.release(
                staging_slot.slot_id,
                worker_request.data_plane,
            )
            cleanup_target_id = cleanup_target_id_for_worker_request(
                cleanup_target_kind,
                worker_request,
            )
            try:
                cleanup_response = (
                    self.cleanup_coordinator.cleanup_status_report_failure(
                        worker_request,
                        target_kind=cleanup_target_kind,
                    )
                )
            except WorkerCleanupError as cleanup_exc:
                return WorkerTransferLifecycleRecord(
                    authorization_request=request,
                    worker_request=worker_request,
                    staging_slot=staging_slot,
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
            cleanup_response = self.cleanup_coordinator.cleanup_execution_failure(
                worker_request,
                result,
                target_kind=cleanup_target_kind,
            )
        except WorkerCleanupError as exc:
            staging_release = self.staging_pool.release(
                staging_slot.slot_id,
                worker_request.data_plane,
            )
            return WorkerTransferLifecycleRecord(
                authorization_request=request,
                worker_request=worker_request,
                staging_slot=staging_slot,
                staging_release=staging_release,
                result=result,
                status_update=status_update,
                status_response=status_response,
                cleanup_target_kind=cleanup_target_kind,
                cleanup_target_id=cleanup_target_id,
                final_state="cleanup_failed",
                error=str(exc),
            )
        staging_release = self.staging_pool.release(
            staging_slot.slot_id,
            worker_request.data_plane,
        )
        return WorkerTransferLifecycleRecord(
            authorization_request=request,
            worker_request=worker_request,
            staging_slot=staging_slot,
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

    def _execute(
        self,
        worker_request: WorkerTransferRequest,
        staging_slot: WorkerStagingSlot,
    ) -> WorkerTransferResult:
        if self.resource_binder is None:
            return self.executor.execute(worker_request, staging_slot)
        with self.resource_binder.bind(worker_request.data_plane) as resources:
            return execute_worker_transfer(
                self.executor,
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
    ) -> WorkerTransferLifecycleRecord:
        if not isinstance(request, WorkerTransferAuthorizationRequest):
            raise TypeError("request must be a WorkerTransferAuthorizationRequest")
        return self.transfer_client.submit_report_cleanup_lifecycle(
            request,
            cleanup_target_kind=cleanup_target_kind,
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
                )
            )
            lifecycle = self.handle_lifecycle(
                self.parse_authorization_request(request_envelope.payload),
                cleanup_target_kind=request_envelope.cleanup_target_kind,
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
        return result
    expected_bytes = expected_worker_completion_bytes(request)
    if result.bytes_completed == expected_bytes:
        return result
    reported_bytes = int(result.bytes_completed)
    safe_completed = min(reported_bytes, expected_bytes)
    return WorkerTransferResult(
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


def expected_worker_completion_bytes(request: WorkerTransferRequest) -> int:
    plan = request.data_plane.plan
    total_bytes = 0
    for assignment in plan.get("assignments", ()) or ():
        if not isinstance(assignment, Mapping):
            raise ValueError("daemon plan assignment must be an object")
        for chunk in assignment.get("chunks", ()) or ():
            if not isinstance(chunk, Mapping):
                raise ValueError("daemon plan chunk must be an object")
            total_bytes += int(chunk["bytes"])
    if total_bytes <= 0:
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
    return WorkerTransferResult(
        transfer_id=worker_request.transfer_id,
        state=WorkerTransferState.FAILED,
        error=str(exc) or exc.__class__.__name__,
        bytes_completed=0,
        metadata={
            "relay_gpu": worker_request.authorization.relay_gpu,
            "relay_gpus": worker_validation.authorized_relay_gpus_for_request(worker_request),
            "src_buffer_id": worker_request.authorization.src_buffer.buffer_id,
            "dst_buffer_id": worker_request.authorization.dst_buffer.buffer_id,
            "staging_slot_id": staging_slot.slot_id,
        },
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
    "WorkerTransferAuthorizer",
    "WorkerTransferCleanupCoordinator",
    "WorkerTransferClient",
    "WorkerTransferService",
    "WorkerTransferStatusReporter",
    "cleanup_target_id",
    "default_worker_executor",
    "execute_worker_transfer",
    "expected_worker_completion_bytes",
    "failed_worker_result_from_exception",
    "parse_worker_authorization_request_payload",
    "require_daemon_worker_plan",
    "validate_worker_completion_bytes",
]
