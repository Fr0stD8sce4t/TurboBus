from __future__ import annotations

from ..schema import (
    DaemonRequest,
    DaemonResponse,
    RequestType,
    TransferIntent,
    WorkerTransferAuthorizationRequest,
)


def handle_request(
    daemon,
    request: DaemonRequest,
    connection_id: str | None = None,
) -> DaemonResponse:
    handler = _REQUEST_HANDLERS.get(request.request_type)
    if handler is not None:
        return handler(daemon, request, connection_id)
    return DaemonResponse(ok=False, error=f"unsupported request: {request.request_type}")


def _handle_register_job(daemon, request: DaemonRequest, connection_id: str | None):
    payload = request.payload
    return daemon.register_job(
        job_id=str(payload["job_id"]),
        user_id=payload.get("user_id"),
        session_id=payload.get("session_id"),
        container_id=payload.get("container_id"),
        process_id=payload.get("process_id"),
        weight=float(payload.get("weight", 1.0)),
        peer_identity=request.peer_identity,
    )


def _handle_register_buffer(daemon, request: DaemonRequest, connection_id: str | None):
    payload = request.payload
    return daemon.register_buffer(
        buffer_id=str(payload["buffer_id"]),
        job_id=str(payload["job_id"]),
        kind=str(payload.get("kind", "cpu_pinned")),
        size_bytes=int(payload.get("size_bytes", 0)),
        device_index=payload.get("device_index"),
        address=payload.get("address"),
        pinned=bool(payload.get("pinned", False)),
        handle_type=str(payload.get("handle_type", "registered_buffer")),
        metadata=payload.get("metadata") or {},
        peer_identity=request.peer_identity,
    )


def _handle_discover_relays(daemon, request: DaemonRequest, connection_id: str | None):
    target_gpu = request.payload.get("target_gpu")
    return daemon.discover_relays(
        target_gpu=None if target_gpu is None else int(target_gpu),
    )


def _handle_get_inventory(daemon, request: DaemonRequest, connection_id: str | None):
    return daemon.get_inventory()


def _handle_reap_expired_leases(
    daemon,
    request: DaemonRequest,
    connection_id: str | None,
):
    expired = daemon.reap_expired_leases(now=request.payload.get("now"))
    return DaemonResponse(
        ok=True,
        payload={"expired_lease_ids": expired, "expired_count": len(expired)},
    )


def _handle_register_session(daemon, request: DaemonRequest, connection_id: str | None):
    payload = request.payload
    return daemon.register_session(
        target_gpu=int(payload["target_gpu"]),
        max_inflight_chunks=int(payload.get("max_inflight_chunks", 8)),
        worker_relay_capable=bool(payload.get("worker_relay_capable", False)),
        peer_identity=request.peer_identity,
        connection_scoped=bool(payload.get("connection_scoped", False)),
        connection_id=connection_id,
    )


def _handle_close_session(daemon, request: DaemonRequest, connection_id: str | None):
    if request.session_id is None:
        return DaemonResponse(ok=False, error="session_id is required")
    return daemon.close_session(request.session_id, peer_identity=request.peer_identity)


def _handle_submit_transfer_intent(
    daemon,
    request: DaemonRequest,
    connection_id: str | None,
):
    intent_payload = request.payload.get("intent")
    if not isinstance(intent_payload, dict):
        return DaemonResponse(ok=False, error="intent is required")
    return daemon.submit_transfer_intent(
        TransferIntent(**intent_payload),
        peer_identity=request.peer_identity,
    )


def _handle_wait_transfer_receipt(
    daemon,
    request: DaemonRequest,
    connection_id: str | None,
):
    payload = request.payload
    return daemon.wait_transfer_receipt(
        intent_id=str(payload["intent_id"]),
        timeout_seconds=payload.get("timeout_seconds"),
        peer_identity=request.peer_identity,
    )


def _handle_recover_transfer_state(
    daemon,
    request: DaemonRequest,
    connection_id: str | None,
):
    payload = request.payload
    return daemon.recover_transfer_state(
        intent_id=payload.get("intent_id"),
        transfer_id=payload.get("transfer_id"),
        peer_identity=request.peer_identity,
    )


def _handle_transfer_status(daemon, request: DaemonRequest, connection_id: str | None):
    payload = request.payload
    return daemon.transfer_status(
        transfer_id=str(payload["transfer_id"]),
        state=payload.get("state"),
        bytes_completed=payload.get("bytes_completed"),
        error=payload.get("error"),
        completion_source=payload.get("completion_source"),
        completion_evidence=payload.get("completion_evidence"),
        peer_identity=request.peer_identity,
    )


def _handle_validate_lease(daemon, request: DaemonRequest, connection_id: str | None):
    payload = request.payload
    return daemon.validate_lease(
        lease_id=str(payload["lease_id"]),
        token=str(payload["token"]),
        session_id=payload.get("session_id"),
        relay_gpu=payload.get("relay_gpu"),
        job_id=payload.get("job_id"),
        buffer_ids=payload.get("buffer_ids"),
        peer_identity=request.peer_identity,
    )


def _handle_authorize_worker_transfer(
    daemon,
    request: DaemonRequest,
    connection_id: str | None,
):
    return daemon.authorize_worker_transfer(
        WorkerTransferAuthorizationRequest(**request.payload),
        peer_identity=request.peer_identity,
    )


def _handle_cleanup(daemon, request: DaemonRequest, connection_id: str | None):
    payload = request.payload
    return daemon.cleanup(
        target_kind=str(payload["target_kind"]),
        target_id=str(payload["target_id"]),
        reason=str(payload.get("reason", "manual")),
        force=bool(payload.get("force", False)),
        owner_binding=payload.get("owner_binding"),
        retention_evidence=payload.get("retention_evidence"),
        peer_identity=request.peer_identity,
    )


def _handle_invalidate_profile(
    daemon,
    request: DaemonRequest,
    connection_id: str | None,
):
    payload = request.payload
    return daemon.invalidate_profile(
        target_gpu=int(payload["target_gpu"]),
        relay_gpus=payload.get("relay_gpus", []),
    )


def _handle_invalidate_topology(
    daemon,
    request: DaemonRequest,
    connection_id: str | None,
):
    return daemon.invalidate_topology()


def _handle_get_profile(daemon, request: DaemonRequest, connection_id: str | None):
    payload = request.payload
    return daemon.get_profile(
        target_gpu=int(payload["target_gpu"]),
        relay_gpus=payload.get("relay_gpus", []),
    )


def _handle_put_profile(daemon, request: DaemonRequest, connection_id: str | None):
    payload = request.payload
    try:
        return daemon.put_profile(
            target_gpu=int(payload["target_gpu"]),
            relay_gpus=payload.get("relay_gpus", []),
            profile=payload.get("profile", {}),
            profile_bytes=int(payload.get("profile_bytes", 0)),
            updated_at=payload.get("updated_at"),
        )
    except Exception as exc:
        return DaemonResponse(ok=False, error=str(exc))


def _handle_profile(daemon, request: DaemonRequest, connection_id: str | None):
    return daemon.describe(peer_identity=request.peer_identity)


def _handle_runtime_telemetry(
    daemon,
    request: DaemonRequest,
    connection_id: str | None,
):
    return daemon.runtime_telemetry(peer_identity=request.peer_identity)


_REQUEST_HANDLERS = {
    RequestType.REGISTER_JOB: _handle_register_job,
    RequestType.REGISTER_BUFFER: _handle_register_buffer,
    RequestType.GET_INVENTORY: _handle_get_inventory,
    RequestType.DISCOVER_RELAYS: _handle_discover_relays,
    RequestType.REAP_EXPIRED_LEASES: _handle_reap_expired_leases,
    RequestType.REGISTER_SESSION: _handle_register_session,
    RequestType.CLOSE_SESSION: _handle_close_session,
    RequestType.SUBMIT_TRANSFER_INTENT: _handle_submit_transfer_intent,
    RequestType.WAIT_TRANSFER_RECEIPT: _handle_wait_transfer_receipt,
    RequestType.RECOVER_TRANSFER_STATE: _handle_recover_transfer_state,
    RequestType.TRANSFER_STATUS: _handle_transfer_status,
    RequestType.VALIDATE_LEASE: _handle_validate_lease,
    RequestType.AUTHORIZE_WORKER_TRANSFER: _handle_authorize_worker_transfer,
    RequestType.CLEANUP: _handle_cleanup,
    RequestType.INVALIDATE_PROFILE: _handle_invalidate_profile,
    RequestType.INVALIDATE_TOPOLOGY: _handle_invalidate_topology,
    RequestType.GET_PROFILE: _handle_get_profile,
    RequestType.PUT_PROFILE: _handle_put_profile,
    RequestType.PROFILE: _handle_profile,
    RequestType.RUNTIME_TELEMETRY: _handle_runtime_telemetry,
}


__all__ = ["handle_request"]
