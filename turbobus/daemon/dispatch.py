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
    if request.request_type == RequestType.REGISTER_JOB:
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
    if request.request_type == RequestType.REGISTER_BUFFER:
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
    if request.request_type == RequestType.GET_INVENTORY:
        return daemon.get_inventory()
    if request.request_type == RequestType.DISCOVER_RELAYS:
        payload = request.payload
        target_gpu = payload.get("target_gpu")
        return daemon.discover_relays(
            target_gpu=None if target_gpu is None else int(target_gpu),
        )
    if request.request_type == RequestType.REAP_EXPIRED_LEASES:
        payload = request.payload
        expired = daemon.reap_expired_leases(now=payload.get("now"))
        return DaemonResponse(
            ok=True,
            payload={
                "expired_lease_ids": expired,
                "expired_count": len(expired),
            },
        )
    if request.request_type == RequestType.REGISTER_SESSION:
        payload = request.payload
        return daemon.register_session(
            target_gpu=int(payload["target_gpu"]),
            max_inflight_chunks=int(payload.get("max_inflight_chunks", 8)),
            peer_identity=request.peer_identity,
            connection_scoped=bool(payload.get("connection_scoped", False)),
            connection_id=connection_id,
        )
    if request.request_type == RequestType.CLOSE_SESSION:
        if request.session_id is None:
            return DaemonResponse(ok=False, error="session_id is required")
        return daemon.close_session(
            request.session_id,
            peer_identity=request.peer_identity,
        )
    if request.request_type == RequestType.SUBMIT_TRANSFER_INTENT:
        payload = request.payload
        intent_payload = payload.get("intent")
        if not isinstance(intent_payload, dict):
            return DaemonResponse(ok=False, error="intent is required")
        return daemon.submit_transfer_intent(
            TransferIntent(**intent_payload),
            peer_identity=request.peer_identity,
        )
    if request.request_type == RequestType.WAIT_TRANSFER_RECEIPT:
        payload = request.payload
        return daemon.wait_transfer_receipt(
            intent_id=str(payload["intent_id"]),
            timeout_seconds=payload.get("timeout_seconds"),
            peer_identity=request.peer_identity,
        )
    if request.request_type == RequestType.TRANSFER_STATUS:
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
    if request.request_type == RequestType.VALIDATE_LEASE:
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
    if request.request_type == RequestType.AUTHORIZE_WORKER_TRANSFER:
        return daemon.authorize_worker_transfer(
            WorkerTransferAuthorizationRequest(**request.payload),
            peer_identity=request.peer_identity,
        )
    if request.request_type == RequestType.CLEANUP:
        payload = request.payload
        return daemon.cleanup(
            target_kind=str(payload["target_kind"]),
            target_id=str(payload["target_id"]),
            reason=str(payload.get("reason", "manual")),
            force=bool(payload.get("force", False)),
            peer_identity=request.peer_identity,
        )
    if request.request_type == RequestType.INVALIDATE_PROFILE:
        payload = request.payload
        return daemon.invalidate_profile(
            target_gpu=int(payload["target_gpu"]),
            relay_gpus=payload.get("relay_gpus", []),
        )
    if request.request_type == RequestType.INVALIDATE_TOPOLOGY:
        return daemon.invalidate_topology()
    if request.request_type == RequestType.GET_PROFILE:
        payload = request.payload
        return daemon.get_profile(
            target_gpu=int(payload["target_gpu"]),
            relay_gpus=payload.get("relay_gpus", []),
        )
    if request.request_type == RequestType.PUT_PROFILE:
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
    if request.request_type == RequestType.PROFILE:
        return daemon.describe()
    return DaemonResponse(ok=False, error=f"unsupported request: {request.request_type}")


__all__ = ["handle_request"]
