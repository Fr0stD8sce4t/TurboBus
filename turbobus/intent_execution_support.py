from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from .schema import (
    DaemonResponse,
    TransferReceipt,
    WorkerTransferAuthorizationRequest,
)
from .worker import (
    WorkerDataPlaneCompletionEnvelope,
    WorkerServiceRequestEnvelope,
    WorkerTransferLifecycleRecord,
)


class WorkerCompletionEnvelopeError(RuntimeError):
    pass


def require_ok(response: DaemonResponse, message: str) -> None:
    if not isinstance(response, DaemonResponse):
        raise TypeError("daemon response must be a DaemonResponse")
    if not response.ok:
        raise RuntimeError(response.error or message)


def worker_lease_tokens(
    daemon_client,
    response: DaemonResponse,
) -> tuple[Mapping[str, object], ...]:
    lease_tokens = response.payload.get("lease_tokens") or ()
    if not lease_tokens:
        raise RuntimeError("worker-managed transfer requires relay leases")
    return tuple(dict(lease_token) for lease_token in lease_tokens)


def require_worker_plan_matches_leases(
    plan_payload: Mapping[str, object],
    lease_tokens: Iterable[Mapping[str, object]],
    *,
    direction: str,
) -> None:
    plan = plan_payload.get("plan")
    if not isinstance(plan, Mapping):
        raise RuntimeError("daemon response did not include a transfer plan")
    lease_relays = {int(lease_token["relay_gpu"]) for lease_token in lease_tokens}
    if not lease_relays:
        raise RuntimeError("worker-managed transfer requires relay leases")
    expected_direction = str(direction).lower()
    found_relay_chunks = False
    plan_relays: set[int] = set()
    for assignment in plan.get("assignments", ()) or ():
        if not isinstance(assignment, Mapping):
            raise RuntimeError("daemon transfer plan assignment must be a mapping")
        path = assignment.get("path")
        if not isinstance(path, Mapping):
            raise RuntimeError("daemon transfer plan assignment has no path")
        path_kind = str(path.get("kind", "")).lower()
        path_direction = str(path.get("direction", "")).lower()
        assignment_relay = int(path.get("relay_device", -1))
        if path_direction != expected_direction:
            raise RuntimeError(
                f"worker-managed transfer requires daemon {expected_direction} plans"
            )
        if path_kind == "direct":
            continue
        if path_kind != "relay" or assignment_relay not in lease_relays:
            raise RuntimeError(
                "worker-managed transfer requires daemon lease coverage for "
                "every relay path"
            )
        if assignment.get("chunks"):
            plan_relays.add(assignment_relay)
            found_relay_chunks = True
    if not found_relay_chunks:
        raise RuntimeError("daemon relay plan did not include worker chunks")
    if plan_relays != lease_relays:
        raise RuntimeError("daemon relay leases do not match worker plan")


def cleanup_planned_relay_lease(
    daemon_client,
    lease_token: Mapping[str, object],
    *,
    reason: str = "unsupported_worker_plan",
    strict: bool = True,
) -> None:
    cleanup = getattr(daemon_client, "cleanup", None)
    if not callable(cleanup):
        return
    response = cleanup(
        target_kind="reservation",
        target_id=str(lease_token["lease_id"]),
        reason=reason,
        force=True,
    )
    if strict:
        require_ok(response, "daemon reservation cleanup failed")


def cleanup_planned_relay_leases(
    daemon_client,
    lease_tokens: Iterable[Mapping[str, object]],
    *,
    reason: str = "unsupported_worker_plan",
    strict: bool = True,
) -> None:
    for lease_token in lease_tokens:
        cleanup_planned_relay_lease(
            daemon_client,
            lease_token,
            reason=reason,
            strict=strict,
        )


def require_daemon_transfer_complete(
    final_status: Mapping[str, object],
    *,
    expected_bytes: int,
) -> None:
    if not isinstance(final_status, Mapping):
        raise TypeError("final_status must be a mapping")
    expected = int(expected_bytes)
    state = final_status.get("state", "unknown")
    state_text = str(getattr(state, "value", state))
    if state_text != "complete":
        error = final_status.get("error")
        suffix = f": {error}" if error else ""
        raise RuntimeError(
            f"daemon transfer status did not complete: {state_text}{suffix}"
        )
    bytes_total = int(final_status.get("bytes_total", expected))
    if bytes_total != expected:
        raise RuntimeError(
            f"daemon transfer byte total mismatch: {bytes_total} != {expected}"
        )
    bytes_completed = int(final_status.get("bytes_completed", -1))
    if bytes_completed != expected:
        raise RuntimeError(
            "daemon transfer completed an unexpected byte count: "
            f"{bytes_completed} != {expected}"
        )


@dataclass(frozen=True)
class WorkerExecutionResult:
    final_state: str | None
    error: str | None
    lifecycle: WorkerTransferLifecycleRecord | None
    completion: WorkerDataPlaneCompletionEnvelope | None


def submit_worker_execution(
    worker_client,
    request: WorkerTransferAuthorizationRequest,
    *,
    expected_bytes: int,
) -> WorkerExecutionResult:
    lifecycle_submitter = getattr(worker_client, "submit_report_cleanup_lifecycle", None)
    if callable(lifecycle_submitter):
        lifecycle = lifecycle_submitter(request, cleanup_target_kind="reservation")
        completion = lifecycle.completion_envelope()
        require_worker_completion_matches_request(
            completion,
            request,
            expected_bytes=expected_bytes,
        )
        return WorkerExecutionResult(
            final_state=lifecycle.final_state,
            error=lifecycle.error,
            lifecycle=lifecycle,
            completion=completion,
        )
    envelope_submitter = getattr(worker_client, "submit_envelope", None)
    if callable(envelope_submitter):
        completion = envelope_submitter(
            WorkerServiceRequestEnvelope(
                payload={
                    "transfer_id": request.transfer_id,
                    "lease_id": request.lease_id,
                    "token": request.token,
                    "session_id": request.session_id,
                    "job_id": request.job_id,
                    "src_buffer_id": request.src_buffer_id,
                    "dst_buffer_id": request.dst_buffer_id,
                    "direction": request.direction,
                    "ranges": list(request.ranges),
                    "relay_gpu": request.relay_gpu,
                },
                cleanup_target_kind="reservation",
            )
        )
        require_worker_completion_matches_request(
            completion,
            request,
            expected_bytes=expected_bytes,
        )
        return WorkerExecutionResult(
            final_state=completion.final_state,
            error=completion.error,
            lifecycle=None,
            completion=completion,
        )
    raise TypeError("worker_client must submit worker-managed transfers")


def require_worker_completion_matches_request(
    completion: WorkerDataPlaneCompletionEnvelope,
    request: WorkerTransferAuthorizationRequest,
    *,
    expected_bytes: int,
) -> None:
    if not isinstance(completion, WorkerDataPlaneCompletionEnvelope):
        raise WorkerCompletionEnvelopeError(
            "worker completion must be a WorkerDataPlaneCompletionEnvelope"
        )
    if completion.transfer_id is not None and completion.transfer_id != request.transfer_id:
        raise WorkerCompletionEnvelopeError("worker completion transfer mismatch")
    if completion.lease_id is not None and completion.lease_id != request.lease_id:
        raise WorkerCompletionEnvelopeError("worker completion lease mismatch")
    require_worker_mapping_matches_request(
        completion.worker_result,
        request,
        label="worker result",
    )
    require_worker_mapping_matches_request(
        completion.daemon_status_update,
        request,
        label="worker daemon status update",
    )
    require_worker_daemon_response_matches_request(
        completion.daemon_status_response,
        request,
    )
    final_state = "" if completion.final_state is None else str(completion.final_state)
    if final_state == "complete":
        if not completion.ok:
            raise WorkerCompletionEnvelopeError("worker completion was not ok")
        if completion.transfer_id is None:
            raise WorkerCompletionEnvelopeError("worker completion missing transfer id")
        if completion.lease_id is None:
            raise WorkerCompletionEnvelopeError("worker completion missing lease id")
        if completion.worker_result is None:
            raise WorkerCompletionEnvelopeError("worker completion missing worker result")
        result_state = state_text(completion.worker_result.get("state", ""))
        if result_state != "complete":
            raise WorkerCompletionEnvelopeError("worker result did not complete")
        require_worker_completed_bytes(
            completion.worker_result,
            int(expected_bytes),
            label="worker result",
        )
        if completion.daemon_status_update is None:
            raise WorkerCompletionEnvelopeError(
                "worker completion missing daemon status update"
            )
        if completion.daemon_status_response is None:
            raise WorkerCompletionEnvelopeError(
                "worker completion missing daemon status response"
            )
        update_state = state_text(completion.daemon_status_update.get("state", ""))
        if update_state != "complete":
            raise WorkerCompletionEnvelopeError(
                "worker daemon status update did not complete"
            )
        require_worker_completed_bytes(
            completion.daemon_status_update,
            int(expected_bytes),
            label="worker daemon status update",
        )
        if not bool(completion.daemon_status_response.get("ok", False)):
            raise WorkerCompletionEnvelopeError(
                "worker daemon status response was not ok"
            )
        require_worker_daemon_response_completed_bytes(
            completion.daemon_status_response,
            int(expected_bytes),
        )
        if completion.daemon_cleanup_response is None:
            raise WorkerCompletionEnvelopeError(
                "worker completion missing daemon release response"
            )
        worker_metadata = _worker_result_metadata(completion.worker_result)
        require_worker_release_response_matches_request(
            completion.daemon_cleanup_response,
            request,
            expected_ticket_id=worker_metadata.get("ticket_id"),
            expected_plan_generation=worker_metadata.get("plan_generation"),
        )
        require_worker_staging_slot_matches_request(
            completion.staging_slot,
            request,
        )
        require_worker_staging_release_matches_request(
            completion.staging_release,
            request,
            slot=completion.staging_slot,
        )


def require_worker_mapping_matches_request(
    payload: Mapping[str, object] | None,
    request: WorkerTransferAuthorizationRequest,
    *,
    label: str,
) -> None:
    if payload is None:
        return
    transfer_id = payload.get("transfer_id")
    if transfer_id is not None and str(transfer_id) != request.transfer_id:
        raise WorkerCompletionEnvelopeError(f"{label} transfer mismatch")
    lease_id = payload.get("lease_id")
    if lease_id is not None and str(lease_id) != request.lease_id:
        raise WorkerCompletionEnvelopeError(f"{label} lease mismatch")


def _worker_result_metadata(
    worker_result: Mapping[str, object] | None,
) -> Mapping[str, object]:
    if not isinstance(worker_result, Mapping):
        return {}
    metadata = worker_result.get("metadata")
    return metadata if isinstance(metadata, Mapping) else {}


def require_worker_daemon_response_matches_request(
    response: Mapping[str, object] | None,
    request: WorkerTransferAuthorizationRequest,
) -> None:
    if response is None:
        return
    payload = response.get("payload")
    if not isinstance(payload, Mapping):
        return
    status = payload.get("status")
    if not isinstance(status, Mapping):
        return
    require_worker_mapping_matches_request(
        status,
        request,
        label="worker daemon status response",
    )


def require_worker_daemon_response_completed_bytes(
    response: Mapping[str, object] | None,
    expected_bytes: int,
) -> None:
    if response is None:
        return
    payload = response.get("payload")
    if not isinstance(payload, Mapping):
        return
    status = payload.get("status")
    if not isinstance(status, Mapping):
        return
    status_state = state_text(status.get("state", ""))
    if status_state and status_state != "complete":
        raise WorkerCompletionEnvelopeError(
            "worker daemon status response did not complete"
        )
    require_worker_completed_bytes(
        status,
        expected_bytes,
        label="worker daemon status response",
    )


def require_worker_release_response_matches_request(
    response: Mapping[str, object],
    request: WorkerTransferAuthorizationRequest,
    *,
    expected_ticket_id: object | None = None,
    expected_plan_generation: object | None = None,
) -> None:
    if not bool(response.get("ok", False)):
        raise WorkerCompletionEnvelopeError(
            "worker daemon release response was not ok"
        )
    payload = response.get("payload")
    if not isinstance(payload, Mapping):
        raise WorkerCompletionEnvelopeError(
            "worker daemon release response missing payload"
        )
    if bool(payload.get("cleanup_skipped", False)):
        raise WorkerCompletionEnvelopeError(
            "worker daemon release response skipped cleanup"
        )
    reservation_id = payload.get("reservation_id")
    if reservation_id is None:
        raise WorkerCompletionEnvelopeError(
            "worker daemon release response missing reservation id"
        )
    if str(reservation_id) != request.lease_id:
        raise WorkerCompletionEnvelopeError(
            "worker daemon release response reservation mismatch"
        )
    cleanup_mode = payload.get("cleanup_mode")
    if cleanup_mode != "release":
        raise WorkerCompletionEnvelopeError(
            "worker daemon release response was not a release"
        )
    released_reservation_ids = payload.get("released_reservation_ids")
    if released_reservation_ids is None:
        raise WorkerCompletionEnvelopeError(
            "worker daemon release response missing released reservation ids"
        )
    released_ids = require_released_reservation_ids(
        released_reservation_ids,
        request,
        label="worker daemon release response",
    )
    require_worker_release_evidence_matches_request(
        payload,
        request,
        label="worker daemon release response",
        expected_ticket_id=expected_ticket_id,
        expected_plan_generation=expected_plan_generation,
    )
    lease_responses = payload.get("lease_responses")
    if lease_responses is not None:
        if isinstance(lease_responses, (str, bytes)) or not isinstance(
            lease_responses,
            Iterable,
        ):
            raise WorkerCompletionEnvelopeError(
                "worker daemon release response lease responses must be iterable"
            )
        for lease_response in lease_responses:
            if not isinstance(lease_response, Mapping):
                raise WorkerCompletionEnvelopeError(
                    "worker daemon release response lease response must be a mapping"
                )
            if not bool(lease_response.get("ok", False)):
                raise WorkerCompletionEnvelopeError(
                    "worker daemon release response lease response was not ok"
                )
            lease_payload = lease_response.get("payload")
            if lease_payload is None:
                continue
            if not isinstance(lease_payload, Mapping):
                raise WorkerCompletionEnvelopeError(
                    "worker daemon release response lease response payload must be a mapping"
                )
            if lease_payload.get("cleanup_mode") != "release":
                raise WorkerCompletionEnvelopeError(
                    "worker daemon release response lease response was not a release"
                )
            lease_response_reservation_id = lease_payload.get("reservation_id")
            if lease_response_reservation_id is None:
                raise WorkerCompletionEnvelopeError(
                    "worker daemon release response lease response missing reservation id"
                )
            if str(lease_response_reservation_id) not in released_ids:
                raise WorkerCompletionEnvelopeError(
                    "worker daemon release response lease response reservation was not released"
                )
            require_worker_release_evidence_matches_request(
                lease_payload,
                request,
                label="worker daemon release response lease response",
                expected_ticket_id=expected_ticket_id,
                expected_plan_generation=expected_plan_generation,
            )


def require_worker_release_evidence_matches_request(
    payload: Mapping[str, object],
    request: WorkerTransferAuthorizationRequest,
    *,
    label: str,
    expected_ticket_id: object | None = None,
    expected_plan_generation: object | None = None,
) -> None:
    transfer_id = payload.get("transfer_id")
    if transfer_id is None or str(transfer_id) != request.transfer_id:
        raise WorkerCompletionEnvelopeError(f"{label} transfer evidence mismatch")
    ticket_id = payload.get("ticket_id")
    if ticket_id is None or not str(ticket_id).strip():
        raise WorkerCompletionEnvelopeError(f"{label} missing ticket evidence")
    if expected_ticket_id is not None and str(ticket_id) != str(expected_ticket_id):
        raise WorkerCompletionEnvelopeError(f"{label} ticket evidence mismatch")
    generation = payload.get("plan_generation")
    if generation is None:
        raise WorkerCompletionEnvelopeError(f"{label} missing plan generation evidence")
    try:
        normalized_generation = int(generation)
        if normalized_generation <= 0:
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise WorkerCompletionEnvelopeError(
            f"{label} invalid plan generation evidence"
        ) from exc
    if expected_plan_generation is not None:
        try:
            expected_generation = int(expected_plan_generation)
        except (TypeError, ValueError) as exc:
            raise WorkerCompletionEnvelopeError(
                f"{label} invalid expected plan generation evidence"
            ) from exc
        if normalized_generation != expected_generation:
            raise WorkerCompletionEnvelopeError(
                f"{label} plan generation evidence mismatch"
            )
    lease_ids = payload.get("lease_ids")
    if lease_ids is None:
        return
    if isinstance(lease_ids, (str, bytes)) or not isinstance(lease_ids, Iterable):
        raise WorkerCompletionEnvelopeError(f"{label} lease_ids evidence is invalid")
    if request.lease_id not in {str(item) for item in lease_ids}:
        raise WorkerCompletionEnvelopeError(f"{label} lease_ids evidence mismatch")


def require_released_reservation_ids(
    released_reservation_ids: object,
    request: WorkerTransferAuthorizationRequest,
    *,
    label: str,
) -> tuple[str, ...]:
    if isinstance(released_reservation_ids, (str, bytes)) or not isinstance(
        released_reservation_ids,
        Iterable,
    ):
        raise WorkerCompletionEnvelopeError(
            f"{label} released reservation ids must be iterable"
        )
    released_ids = tuple(str(item) for item in released_reservation_ids)
    if not released_ids:
        raise WorkerCompletionEnvelopeError(
            f"{label} missing released reservation ids"
        )
    if request.lease_id not in released_ids:
        raise WorkerCompletionEnvelopeError(
            f"{label} missing primary lease"
        )
    return released_ids


def require_worker_staging_slot_matches_request(
    slot: Mapping[str, object] | None,
    request: WorkerTransferAuthorizationRequest,
) -> None:
    if slot is None:
        raise WorkerCompletionEnvelopeError("worker completion missing staging slot")
    if not bool(slot.get("active", False)):
        raise WorkerCompletionEnvelopeError("worker staging slot was not active")
    transfer_id = slot.get("transfer_id")
    if transfer_id is not None and str(transfer_id) != request.transfer_id:
        raise WorkerCompletionEnvelopeError("worker staging slot transfer mismatch")
    lease_id = slot.get("lease_id")
    if lease_id is not None and str(lease_id) != request.lease_id:
        raise WorkerCompletionEnvelopeError("worker staging slot lease mismatch")


def require_worker_staging_release_matches_request(
    release: Mapping[str, object] | None,
    request: WorkerTransferAuthorizationRequest,
    *,
    slot: Mapping[str, object] | None,
) -> None:
    if release is None:
        raise WorkerCompletionEnvelopeError(
            "worker completion missing staging release"
        )
    if bool(release.get("active", True)):
        raise WorkerCompletionEnvelopeError("worker staging release is still active")
    transfer_id = release.get("transfer_id")
    if transfer_id is not None and str(transfer_id) != request.transfer_id:
        raise WorkerCompletionEnvelopeError("worker staging release transfer mismatch")
    lease_id = release.get("lease_id")
    if lease_id is not None and str(lease_id) != request.lease_id:
        raise WorkerCompletionEnvelopeError("worker staging release lease mismatch")
    if slot is None:
        return
    slot_id = slot.get("slot_id")
    release_slot_id = release.get("slot_id")
    if slot_id is None or release_slot_id is None:
        raise WorkerCompletionEnvelopeError("worker staging slot id missing")
    if str(release_slot_id) != str(slot_id):
        raise WorkerCompletionEnvelopeError("worker staging release slot mismatch")


def require_worker_completed_bytes(
    payload: Mapping[str, object],
    expected_bytes: int,
    *,
    label: str,
) -> None:
    if "bytes_completed" not in payload:
        raise WorkerCompletionEnvelopeError(f"{label} missing completed bytes")
    try:
        bytes_completed = int(payload["bytes_completed"])
    except (TypeError, ValueError) as exc:
        raise WorkerCompletionEnvelopeError(
            f"{label} completed bytes are invalid"
        ) from exc
    if bytes_completed != int(expected_bytes):
        raise WorkerCompletionEnvelopeError(
            f"{label} completed byte mismatch: "
            f"{bytes_completed} != {int(expected_bytes)}"
        )
    if "bytes_total" not in payload:
        return
    try:
        bytes_total = int(payload["bytes_total"])
    except (TypeError, ValueError) as exc:
        raise WorkerCompletionEnvelopeError(
            f"{label} total bytes are invalid"
        ) from exc
    if bytes_total != int(expected_bytes):
        raise WorkerCompletionEnvelopeError(
            f"{label} total byte mismatch: {bytes_total} != {int(expected_bytes)}"
        )


def state_text(state: object) -> str:
    return str(getattr(state, "value", state)).lower()


def wait_for_intent_receipt(daemon_client, intent_id: str) -> TransferReceipt:
    waiter = getattr(daemon_client, "wait_transfer_receipt", None)
    if not callable(waiter):
        raise TypeError("daemon client must support wait_transfer_receipt")
    response = waiter(str(intent_id), timeout_seconds=0.0)
    require_ok(response, "daemon receipt wait failed")
    return receipt_from_daemon_payload(
        response.payload,
        expected_intent_id=str(intent_id),
    )


def receipt_from_daemon_payload(
    payload: Mapping[str, object],
    *,
    expected_intent_id: str,
) -> TransferReceipt:
    receipt_payload = payload.get("receipt")
    if not isinstance(receipt_payload, Mapping):
        raise ValueError("daemon response missing receipt")
    receipt = TransferReceipt(**dict(receipt_payload))
    if receipt.intent_id != str(expected_intent_id):
        raise ValueError("daemon receipt intent_id does not match request")
    return receipt


__all__ = [
    "WorkerCompletionEnvelopeError",
    "WorkerExecutionResult",
    "cleanup_planned_relay_lease",
    "cleanup_planned_relay_leases",
    "receipt_from_daemon_payload",
    "require_daemon_transfer_complete",
    "require_ok",
    "require_worker_plan_matches_leases",
    "state_text",
    "submit_worker_execution",
    "wait_for_intent_receipt",
    "worker_lease_tokens",
]
