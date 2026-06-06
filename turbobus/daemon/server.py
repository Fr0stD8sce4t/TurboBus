from __future__ import annotations

import json
import os
import socket
import threading
import time
import uuid
from dataclasses import asdict
from typing import Iterable, Mapping

from . import dispatch as daemon_dispatch
from . import leases as daemon_leases
from . import peer_auth
from . import profiles as daemon_profiles
from . import receipts as daemon_receipts
from ..schema import (
    BufferRegistration,
    CleanupRequest,
    DaemonRequest,
    DaemonResponse,
    ExecutionTicket,
    JobIdentity,
    LeaseToken,
    PeerIdentity,
    RelayQuota,
    RequestType,
    Session,
    TransferIntent,
    TransferReceipt,
    TransferReservation,
    TransferStatus,
    TransferStatusState,
    WorkerTransferAuthorization,
    WorkerTransferAuthorizationRequest,
)
from ..socket_security import secure_unix_socket, unlink_stale_socket
from ..topology import TopologyProvider
from ..scheduler import (
    DaemonScheduler,
    SchedulingDecision,
    scheduling_decision_leases,
)
from ..scheduler.load_feedback import (
    busy_relays_from_runtime_state,
    relay_load_from_runtime_state,
)


_TERMINAL_TRANSFER_STATES = {
    TransferStatusState.COMPLETE,
    TransferStatusState.FAILED,
    TransferStatusState.CANCELED,
}
_ADMISSION_ADMITTED = "admitted"
_ADMISSION_DELAYED = "delayed"
_ADMISSION_EXPIRED = "expired"
_ADMISSION_CANCELED = "canceled"
_ADMISSION_FAILED = "failed"
_DEFAULT_PLAN_TTL_SECONDS = 30.0
_TOPOLOGY_UNAVAILABLE_ERROR = (
    "topology provider is required; synthetic topology is test fixture only"
)


class TurboBusDaemon:
    """Minimal resource-control daemon.

    The first version deliberately does not move GPU pointers across processes.
    It owns session and relay quota state; client processes still execute CUDA
    transfers locally after obtaining a session.
    """

    def __init__(
        self,
        relay_gpus: Iterable[int],
        max_sessions_per_relay: int = 1,
        max_inflight_chunks_per_relay: int = 8,
        session_timeout_seconds: float = 0.0,
        profile_max_age_seconds: float = 0.0,
        topology_provider: TopologyProvider | None = None,
        require_authenticated_peers: bool = False,
    ) -> None:
        relays = tuple(self._normalize_relays(relay_gpus))
        self._lock = threading.Lock()
        self._jobs: dict[str, JobIdentity] = {}
        self._job_peer_identities: dict[str, PeerIdentity] = {}
        self._session_peer_identities: dict[str, PeerIdentity] = {}
        self._buffers: dict[str, BufferRegistration] = {}
        self._sessions: dict[str, Session] = {}
        self._reservations: dict[str, TransferReservation] = {}
        self._reservation_transfers: dict[str, str] = {}
        self._transfer_intents: dict[str, TransferIntent] = {}
        self._intent_transfers: dict[str, str] = {}
        self._transfer_queue: list[str] = []
        self._transfer_queue_records: dict[str, dict[str, object]] = {}
        self._runtime_state_version = 0
        self._transfer_plan_requests: dict[str, dict[str, object]] = {}
        self._transfer_plan_generations: dict[str, int] = {}
        self._transfer_plan_expirations: dict[str, float] = {}
        self._transfer_admissions: dict[str, dict[str, object]] = {}
        self._lease_plan_generations: dict[str, int] = {}
        self._transfer_plans: dict[str, dict[str, object]] = {}
        self._scheduling_decisions: dict[str, SchedulingDecision] = {}
        self._execution_tickets: dict[str, ExecutionTicket] = {}
        self._transfer_tickets: dict[str, str] = {}
        self._transfer_completion_tickets: dict[str, ExecutionTicket] = {}
        self._lease_tokens: dict[str, LeaseToken] = {}
        self._transfer_statuses: dict[str, TransferStatus] = {}
        self._transfer_completion_sources: dict[str, str] = {}
        self._transfer_completion_evidence: dict[str, dict[str, object]] = {}
        self._transfer_peer_identities: dict[str, PeerIdentity] = {}
        self._transfer_receipt_archive: dict[str, dict[str, object]] = {}
        self._archived_intent_transfers: dict[str, str] = {}
        self._retired_cleanup_targets: dict[tuple[str, str], dict[str, object]] = {}
        self._staging_records: dict[str, dict[str, object]] = {}
        self._audit_records: list[dict[str, object]] = []
        self._connection_scoped_sessions: set[str] = set()
        self._connection_scoped_session_connections: dict[str, str] = {}
        self._cleanup_events: list[CleanupRequest] = []
        self._system_cleanup_events: list[CleanupRequest] = []
        self._profile_cache: dict[str, dict] = {}
        self._scheduler = DaemonScheduler()
        self._topology_provider = topology_provider
        self._session_timeout_seconds = max(0.0, float(session_timeout_seconds))
        self._profile_max_age_seconds = max(0.0, float(profile_max_age_seconds))
        self._require_authenticated_peers = bool(require_authenticated_peers)
        self._relay_quotas = {
            int(gpu): RelayQuota(
                relay_gpu=int(gpu),
                max_sessions=max_sessions_per_relay,
                max_inflight_chunks=max_inflight_chunks_per_relay,
            )
            for gpu in relays
        }

    def get_inventory(self) -> DaemonResponse:
        if self._topology_provider is None:
            return _topology_unavailable_response()
        inventory = self._topology_provider.snapshot()
        return DaemonResponse(
            ok=True,
            payload={
                "inventory": inventory.as_dict(),
                "topology_snapshot": asdict(inventory.to_topology_snapshot()),
            },
        )

    def invalidate_topology(self) -> DaemonResponse:
        if self._topology_provider is None:
            return _topology_unavailable_response()
        invalidate = getattr(self._topology_provider, "invalidate", None)
        if not callable(invalidate):
            return DaemonResponse(
                ok=False,
                error="topology provider does not support invalidation",
            )
        try:
            invalidate()
        except NotImplementedError:
            return DaemonResponse(
                ok=False,
                error="topology provider does not support invalidation",
            )
        inventory = self._topology_provider.snapshot()
        return DaemonResponse(
            ok=True,
            payload={
                "topology_snapshot_id": inventory.topology_snapshot_id(),
                "topology_version": inventory.version,
                "inventory_source": inventory.source,
                "inventory_discovered_at": inventory.discovered_at,
                "inventory": inventory.as_dict(),
                "topology_snapshot": asdict(inventory.to_topology_snapshot()),
            },
        )

    def discover_relays(
        self,
        target_gpu: int | None = None,
    ) -> DaemonResponse:
        now = time.time()
        target = None if target_gpu is None else int(target_gpu)
        with self._lock:
            self._reap_stale_sessions_locked(now)
            self._reap_expired_leases_locked(now)
            if self._topology_provider is None:
                return _topology_unavailable_response()
            inventory = self._topology_provider.snapshot()
            candidates = tuple(sorted(self._relay_quotas))
            return DaemonResponse(
                ok=True,
                payload={
                    "relay_discovery": self._relay_discovery_snapshot_locked(
                        inventory=inventory,
                        target_gpu=target,
                        requested_relays=candidates,
                    )
                },
            )

    def register_session(
        self,
        target_gpu: int,
        max_inflight_chunks: int = 8,
        worker_relay_capable: bool = False,
        peer_identity: PeerIdentity | None = None,
        connection_scoped: bool = False,
        connection_id: str | None = None,
    ) -> DaemonResponse:
        max_inflight = int(max_inflight_chunks)
        if max_inflight <= 0:
            return DaemonResponse(ok=False, error="max_inflight_chunks must be positive")
        target = int(target_gpu)
        now = time.time()
        with self._lock:
            self._reap_stale_sessions_locked(now)
            self._reap_expired_leases_locked(now)
            try:
                relays = self._relays_for_new_session_locked(target)
            except ValueError as exc:
                return DaemonResponse(ok=False, error=str(exc))
            if bool(worker_relay_capable):
                busy = [
                    gpu for gpu in relays if not self._relay_quotas[gpu].can_attach()
                ]
                if busy:
                    return DaemonResponse(
                        ok=False,
                        error=f"relay GPUs are unavailable: {busy}",
                    )

            session_id = str(uuid.uuid4())
            session = Session(
                session_id=session_id,
                target_gpu=target,
                relay_gpus=relays,
                max_inflight_chunks=max_inflight,
                worker_relay_capable=bool(worker_relay_capable),
                created_at=now,
                last_seen=now,
            )
            self._sessions[session_id] = session
            if peer_identity is not None:
                self._session_peer_identities[session_id] = peer_identity
            if connection_scoped:
                self._connection_scoped_sessions.add(session_id)
                if connection_id is not None:
                    self._connection_scoped_session_connections[session_id] = str(connection_id)
            if bool(worker_relay_capable):
                for gpu in relays:
                    self._relay_quotas[gpu].sessions.add(session_id)
            payload = {"session": asdict(session)}
            if peer_identity is not None:
                payload["peer_identity"] = asdict(peer_identity)
            if connection_scoped:
                payload["connection_scoped"] = True
            return DaemonResponse(ok=True, payload=payload)

    def register_job(
        self,
        job_id: str,
        user_id: str | None = None,
        session_id: str | None = None,
        container_id: str | None = None,
        process_id: int | None = None,
        weight: float = 1.0,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        try:
            user_id, process_id, container_id = peer_auth.bind_job_identity_to_peer(
                user_id=user_id,
                process_id=process_id,
                container_id=container_id,
                peer_identity=peer_identity,
            )
        except ValueError as exc:
            return DaemonResponse(ok=False, error=str(exc))
        job = JobIdentity(
            job_id=job_id,
            user_id=user_id,
            session_id=session_id,
            container_id=container_id,
            process_id=process_id,
            weight=weight,
        )
        with self._lock:
            if job.session_id is not None and job.session_id not in self._sessions:
                return DaemonResponse(ok=False, error="unknown session")
            session_peer = (
                None
                if job.session_id is None
                else self._session_peer_identities.get(job.session_id)
            )
            try:
                peer_auth.validate_peer_owner_match(
                    expected=session_peer,
                    actual=peer_identity,
                    owner_name="session",
                )
            except ValueError as exc:
                return DaemonResponse(ok=False, error=str(exc))
            self._jobs[job.job_id] = job
            if peer_identity is not None:
                self._job_peer_identities[job.job_id] = peer_identity
            payload = {"job": asdict(job)}
            if peer_identity is not None:
                payload["peer_identity"] = asdict(peer_identity)
            return DaemonResponse(ok=True, payload=payload)

    def register_buffer(
        self,
        buffer_id: str,
        job_id: str,
        kind: str,
        size_bytes: int,
        device_index: int | None = None,
        address: int | None = None,
        pinned: bool = False,
        handle_type: str = "registered_buffer",
        metadata: dict[str, object] | None = None,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        buffer = BufferRegistration(
            buffer_id=buffer_id,
            job_id=job_id,
            kind=kind,
            size_bytes=size_bytes,
            device_index=device_index,
            address=address,
            pinned=pinned,
            handle_type=handle_type,
            metadata={} if metadata is None else metadata,
        )
        with self._lock:
            now = time.time()
            self._reap_stale_sessions_locked(now)
            self._reap_expired_leases_locked(now)
            if buffer.job_id not in self._jobs:
                return DaemonResponse(ok=False, error="unknown job")
            try:
                self._validate_peer_owns_job_locked(
                    job_id=buffer.job_id,
                    peer_identity=peer_identity,
                )
            except ValueError as exc:
                return DaemonResponse(ok=False, error=str(exc))
            if self._active_buffer_lease_ids_locked(buffer.buffer_id):
                return DaemonResponse(ok=False, error="buffer has active lease")
            self._buffers[buffer.buffer_id] = buffer
            return DaemonResponse(ok=True, payload={"buffer": asdict(buffer)})

    def cleanup(
        self,
        target_kind: str,
        target_id: str,
        reason: str,
        force: bool = False,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        cleanup = CleanupRequest(
            target_kind=target_kind,
            target_id=target_id,
            reason=reason,
            force=force,
        )
        with self._lock:
            removed = _empty_removed_summary()
            cleanup_result: dict[str, object] = {}
            if cleanup.target_kind == "job":
                archived_target = self._retired_cleanup_target_record_locked(
                    target_kind=cleanup.target_kind,
                    target_id=cleanup.target_id,
                )
                if cleanup.target_id not in self._jobs and not cleanup.force:
                    if archived_target is None:
                        return DaemonResponse(ok=False, error="unknown job")
                    try:
                        self._validate_peer_owns_missing_cleanup_target_locked(
                            target_kind=cleanup.target_kind,
                            target_id=cleanup.target_id,
                            peer_identity=peer_identity,
                        )
                    except ValueError as exc:
                        return DaemonResponse(ok=False, error=str(exc))
                    return DaemonResponse(
                        ok=True,
                        payload={
                            "cleanup": asdict(cleanup),
                            "removed": removed,
                        },
                    )
                try:
                    if cleanup.target_id in self._jobs:
                        self._validate_peer_owns_job_locked(
                            job_id=cleanup.target_id,
                            peer_identity=peer_identity,
                        )
                    else:
                        self._validate_peer_owns_missing_cleanup_target_locked(
                            target_kind=cleanup.target_kind,
                            target_id=cleanup.target_id,
                            peer_identity=peer_identity,
                        )
                except ValueError as exc:
                    return DaemonResponse(ok=False, error=str(exc))
                _merge_removed(
                    removed,
                    self._cleanup_job_locked(
                        cleanup.target_id,
                        reason=cleanup.reason,
                    ),
                )
            elif cleanup.target_kind == "buffer":
                archived_target = self._retired_cleanup_target_record_locked(
                    target_kind=cleanup.target_kind,
                    target_id=cleanup.target_id,
                )
                if cleanup.target_id not in self._buffers and not cleanup.force:
                    if archived_target is None:
                        return DaemonResponse(ok=False, error="unknown buffer")
                    try:
                        self._validate_peer_owns_missing_cleanup_target_locked(
                            target_kind=cleanup.target_kind,
                            target_id=cleanup.target_id,
                            peer_identity=peer_identity,
                        )
                    except ValueError as exc:
                        return DaemonResponse(ok=False, error=str(exc))
                    return DaemonResponse(
                        ok=True,
                        payload={
                            "cleanup": asdict(cleanup),
                            "removed": removed,
                        },
                    )
                try:
                    if cleanup.target_id in self._buffers:
                        self._validate_peer_owns_buffer_locked(
                            buffer_id=cleanup.target_id,
                            peer_identity=peer_identity,
                        )
                    else:
                        self._validate_peer_owns_missing_cleanup_target_locked(
                            target_kind=cleanup.target_kind,
                            target_id=cleanup.target_id,
                            peer_identity=peer_identity,
                        )
                except ValueError as exc:
                    return DaemonResponse(ok=False, error=str(exc))
                buffer = self._buffers.get(cleanup.target_id)
                if buffer is not None:
                    self._archive_cleanup_target_locked(
                        target_kind=cleanup.target_kind,
                        target_id=cleanup.target_id,
                        peer_identity=peer_identity,
                        reason=cleanup.reason,
                        transfer_ids=self._transfer_ids_for_buffer_locked(
                            cleanup.target_id
                        ),
                    )
                transfer_ids = self._transfer_ids_for_buffer_locked(cleanup.target_id)
                for lease_id in self._active_buffer_lease_ids_locked(cleanup.target_id):
                    _merge_removed(
                        removed,
                        self._release_reservation_and_count_locked(
                            lease_id,
                            final_state=TransferStatusState.CANCELED,
                            cleanup_reason=cleanup.reason,
                        ),
                    )
                buffer = self._buffers.pop(cleanup.target_id, None)
                if buffer is not None:
                    removed["buffers"] = int(removed["buffers"]) + 1
                for transfer_id in transfer_ids:
                    status = self._transfer_statuses.get(transfer_id)
                    if (
                        status is not None
                        and status.state not in _TERMINAL_TRANSFER_STATES
                    ):
                        self._mark_transfer_terminal_locked(
                            transfer_id,
                            TransferStatusState.CANCELED,
                            error=cleanup.reason,
                        )
                        removed["transfers"] = int(removed["transfers"]) + 1
                    self._retire_transfer_runtime_state_locked(transfer_id)
            elif cleanup.target_kind == "session":
                archived_target = self._retired_cleanup_target_record_locked(
                    target_kind=cleanup.target_kind,
                    target_id=cleanup.target_id,
                )
                try:
                    if cleanup.target_id in self._sessions:
                        self._validate_peer_owns_session_locked(
                            session_id=cleanup.target_id,
                            peer_identity=peer_identity,
                        )
                    elif cleanup.force:
                        self._validate_peer_owns_missing_cleanup_target_locked(
                            target_kind=cleanup.target_kind,
                            target_id=cleanup.target_id,
                            peer_identity=peer_identity,
                        )
                except ValueError as exc:
                    return DaemonResponse(ok=False, error=str(exc))
                if cleanup.target_id not in self._sessions and not cleanup.force:
                    if archived_target is None:
                        return DaemonResponse(ok=False, error="unknown session")
                    try:
                        self._validate_peer_owns_missing_cleanup_target_locked(
                            target_kind=cleanup.target_kind,
                            target_id=cleanup.target_id,
                            peer_identity=peer_identity,
                        )
                    except ValueError as exc:
                        return DaemonResponse(ok=False, error=str(exc))
                    return DaemonResponse(
                        ok=True,
                        payload={
                            "cleanup": asdict(cleanup),
                            "removed": removed,
                        },
                    )
                session = self._close_session_locked(
                    cleanup.target_id,
                    reason=cleanup.reason,
                    removed=removed,
                )
                if session is None and not cleanup.force:
                    return DaemonResponse(ok=False, error="unknown session")
            elif cleanup.target_kind == "reservation":
                archived_target = self._retired_cleanup_target_record_locked(
                    target_kind=cleanup.target_kind,
                    target_id=cleanup.target_id,
                )
                try:
                    self._validate_peer_owns_lease_locked(
                        lease_id=cleanup.target_id,
                        peer_identity=peer_identity,
                    )
                except ValueError as exc:
                    if str(exc) != "unknown lease":
                        return DaemonResponse(ok=False, error=str(exc))
                    staging_record = self._staging_records.get(cleanup.target_id)
                    if staging_record is not None and cleanup.force:
                        try:
                            self._validate_peer_owns_staging_record_locked(
                                staging_record=staging_record,
                                peer_identity=peer_identity,
                            )
                        except ValueError as staging_exc:
                            return DaemonResponse(ok=False, error=str(staging_exc))
                    elif archived_target is None or not cleanup.force:
                        if archived_target is None:
                            return DaemonResponse(ok=False, error=str(exc))
                        try:
                            self._validate_peer_owns_missing_cleanup_target_locked(
                                target_kind=cleanup.target_kind,
                                target_id=cleanup.target_id,
                                peer_identity=peer_identity,
                            )
                        except ValueError as owner_exc:
                            return DaemonResponse(ok=False, error=str(owner_exc))
                        return DaemonResponse(
                            ok=True,
                            payload={
                                "cleanup": asdict(cleanup),
                                "removed": removed,
                                "reservation_id": cleanup.target_id,
                                "cleaned_reservation_ids": (),
                                "cleanup_kind": cleanup.target_kind,
                                "cleanup_mode": "noop",
                            },
                        )
                if (
                    cleanup.target_id not in self._reservations
                    and cleanup.target_id not in self._staging_records
                ):
                    if archived_target is None:
                        return DaemonResponse(ok=False, error="unknown reservation")
                    try:
                        self._validate_peer_owns_missing_cleanup_target_locked(
                            target_kind=cleanup.target_kind,
                            target_id=cleanup.target_id,
                            peer_identity=peer_identity,
                        )
                    except ValueError as owner_exc:
                        return DaemonResponse(ok=False, error=str(owner_exc))
                    return DaemonResponse(
                        ok=True,
                        payload={
                            "cleanup": asdict(cleanup),
                            "removed": removed,
                            "reservation_id": cleanup.target_id,
                            "cleaned_reservation_ids": (),
                            "cleanup_kind": cleanup.target_kind,
                            "cleanup_mode": "noop",
                        },
                    )
                if (
                    cleanup.target_id not in self._reservations
                    and cleanup.target_id not in self._staging_records
                    and not cleanup.force
                ):
                    if archived_target is None:
                        return DaemonResponse(ok=False, error="unknown reservation")
                    try:
                        self._validate_peer_owns_missing_cleanup_target_locked(
                            target_kind=cleanup.target_kind,
                            target_id=cleanup.target_id,
                            peer_identity=peer_identity,
                        )
                    except ValueError as exc:
                        return DaemonResponse(ok=False, error=str(exc))
                    cleanup_result = {
                        "reservation_id": cleanup.target_id,
                        "cleaned_reservation_ids": (),
                        "cleanup_kind": cleanup.target_kind,
                        "cleanup_mode": "noop",
                    }
                    return DaemonResponse(
                        ok=True,
                        payload={
                            "cleanup": asdict(cleanup),
                            "removed": removed,
                            **cleanup_result,
                        },
                    )
                released = self._release_reservation_and_count_locked(
                    cleanup.target_id,
                    final_state=TransferStatusState.CANCELED,
                    cleanup_reason=cleanup.reason,
                )
                if (
                    int(released["reservations"]) == 0
                    and int(released["staging_records"]) == 0
                    and not cleanup.force
                ):
                    return DaemonResponse(ok=False, error="unknown reservation")
                _merge_removed(removed, released)
                cleaned = (
                    int(released["reservations"]) > 0
                    or int(released["staging_records"]) > 0
                )
                cleanup_result = {
                    "reservation_id": cleanup.target_id,
                    "cleaned_reservation_ids": (
                        (cleanup.target_id,) if cleaned else ()
                    ),
                    "cleanup_kind": cleanup.target_kind,
                    "cleanup_mode": "cleanup" if cleaned else "noop",
                }
            else:
                return DaemonResponse(ok=False, error="unsupported cleanup target")
            self._cleanup_events.append(cleanup)
            promoted = self._promote_delayed_transfers_locked(now=time.time())
            return DaemonResponse(
                ok=True,
                payload={
                    "cleanup": asdict(cleanup),
                    "removed": removed,
                    "promoted_transfers": promoted,
                    **cleanup_result,
                },
            )

    def close_session(
        self,
        session_id: str,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        with self._lock:
            self._reap_stale_sessions_locked(time.time())
            archived_target = self._retired_cleanup_target_record_locked(
                target_kind="session",
                target_id=str(session_id),
            )
            try:
                if str(session_id) in self._sessions:
                    self._validate_peer_owns_session_locked(
                        session_id=str(session_id),
                        peer_identity=peer_identity,
                    )
                elif archived_target is not None:
                    self._validate_peer_owns_missing_cleanup_target_locked(
                        target_kind="session",
                        target_id=str(session_id),
                        peer_identity=peer_identity,
                    )
            except ValueError as exc:
                return DaemonResponse(ok=False, error=str(exc))
            removed = _empty_removed_summary()
            session = self._close_session_locked(
                session_id,
                reason="session_closed",
                removed=removed,
            )
            if session is None:
                if archived_target is None:
                    return DaemonResponse(ok=False, error="unknown session")
                return DaemonResponse(
                    ok=True,
                    payload={"session_id": session_id, "removed": removed},
                )
            return DaemonResponse(
                ok=True,
                payload={"session_id": session_id, "removed": removed},
            )

    def transfer_status(
        self,
        transfer_id: str,
        state: str | None = None,
        bytes_completed: int | None = None,
        error: str | None = None,
        completion_source: str | None = None,
        completion_evidence: Mapping[str, object] | None = None,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        with self._lock:
            status = self._transfer_statuses.get(str(transfer_id))
            archived = self._transfer_receipt_archive.get(str(transfer_id), {})
            if status is None and isinstance(archived.get("status"), TransferStatus):
                status = archived["status"]
            if status is None:
                return DaemonResponse(ok=False, error="unknown transfer")
            try:
                if status.job_id in self._jobs:
                    self._validate_peer_owns_job_locked(
                        job_id=status.job_id,
                        peer_identity=peer_identity,
                    )
                else:
                    self._validate_peer_owns_receipt_transfer_locked(
                        transfer_id=status.transfer_id,
                        job_id=status.job_id,
                        peer_identity=peer_identity,
                    )
            except ValueError as exc:
                return DaemonResponse(ok=False, error=str(exc))
            if state is None and bytes_completed is None and error is None:
                return DaemonResponse(ok=True, payload={"status": asdict(status)})
            try:
                requested_state = (
                    status.state if state is None else TransferStatusState(state)
                )
            except ValueError as exc:
                return DaemonResponse(ok=False, error=str(exc))
            if status.state in _TERMINAL_TRANSFER_STATES:
                if (
                    requested_state == status.state
                    and _status_bytes_match(status, bytes_completed)
                    and (error is None or error == status.error)
                ):
                    supplemental = self._supplement_terminal_completion_evidence_locked(
                        status,
                        completion_source=completion_source,
                        completion_evidence=completion_evidence,
                    )
                    if not supplemental.ok:
                        return supplemental
                    if status.state is TransferStatusState.COMPLETE:
                        evidence_error = (
                            self._completion_release_blocked_reason_locked(
                                status.transfer_id
                            )
                        )
                        if evidence_error is not None:
                            return DaemonResponse(ok=False, error=evidence_error)
                    return DaemonResponse(ok=True, payload={"status": asdict(status)})
                return DaemonResponse(
                    ok=False,
                    error="terminal transfer status cannot be updated",
                )
            checked_at = time.time()
            admission_error = self._transfer_status_update_blocked_reason_locked(
                status.transfer_id,
                requested_state,
                now=checked_at,
            )
            if admission_error is not None:
                return DaemonResponse(ok=False, error=admission_error)
            try:
                updated = TransferStatus(
                    transfer_id=status.transfer_id,
                    job_id=status.job_id,
                    state=requested_state,
                    bytes_total=status.bytes_total,
                    bytes_completed=(
                        status.bytes_completed
                        if bytes_completed is None
                        else int(bytes_completed)
                    ),
                    session_id=status.session_id,
                    error=status.error if error is None else error,
                )
            except ValueError as exc:
                if requested_state is TransferStatusState.COMPLETE:
                    mismatch = str(exc)
                    failed = self._mark_transfer_terminal_locked(
                        status.transfer_id,
                        TransferStatusState.FAILED,
                        error=mismatch,
                    )
                    self._append_transfer_audit_records_locked(
                        event_type="detected_mismatch",
                        transfer_id=status.transfer_id,
                        state=TransferStatusState.FAILED,
                        reason="transfer_status_mismatch",
                        failure_reason=mismatch,
                    )
                    removed = self._release_reservations_for_transfer_locked(
                        status.transfer_id,
                        final_state=TransferStatusState.FAILED,
                        cleanup_reason="transfer_status_mismatch",
                    )
                    self._refresh_transfer_queue_record_locked(status.transfer_id)
                    return DaemonResponse(
                        ok=False,
                        error=mismatch,
                        payload={"status": asdict(failed), "removed": removed},
                    )
                return DaemonResponse(ok=False, error=str(exc))
            normalized_completion_source = str(completion_source or "").lower()
            normalized_completion_evidence: dict[str, object] | None = None
            completion_ticket: ExecutionTicket | None = None
            requires_execution_evidence = (
                self._intent_requires_execution_evidence_locked(updated.transfer_id)
            )
            if updated.state is TransferStatusState.COMPLETE:
                if requires_execution_evidence:
                    if not _is_execution_completion_source(normalized_completion_source):
                        return DaemonResponse(
                            ok=False,
                            error=(
                                "intent transfer completion requires worker/backend "
                                "execution evidence"
                            ),
                        )
                    try:
                        ticket = self._current_execution_ticket_for_transfer_locked(
                            updated.transfer_id
                        )
                        completion_ticket = ticket
                        normalized_completion_evidence = _normalize_completion_evidence(
                            completion_evidence,
                            expected_bytes=updated.bytes_total,
                            completion_source=normalized_completion_source,
                            expected_ticket=ticket,
                        )
                    except ValueError as exc:
                        return DaemonResponse(ok=False, error=str(exc))
                elif completion_evidence is not None:
                    if isinstance(completion_evidence, Mapping):
                        try:
                            normalized_completion_evidence = (
                                _normalize_completion_evidence(
                                    completion_evidence,
                                    expected_bytes=updated.bytes_total,
                                    completion_source=normalized_completion_source,
                                )
                            )
                        except ValueError:
                            normalized_completion_evidence = dict(completion_evidence)
                    else:
                        normalized_completion_evidence = {
                            "raw_completion_evidence": str(completion_evidence)
                        }
            elif (
                requires_execution_evidence
                and updated.state
                in {
                    TransferStatusState.RUNNING,
                    TransferStatusState.FAILED,
                    TransferStatusState.CANCELED,
                }
            ):
                if not _is_execution_completion_source(normalized_completion_source):
                    return DaemonResponse(
                        ok=False,
                        error=(
                            "intent transfer status update requires worker/backend "
                            "execution evidence"
                        ),
                    )
                try:
                    ticket = self._current_execution_ticket_for_transfer_locked(
                        updated.transfer_id
                    )
                    normalized_completion_evidence = (
                        _normalize_status_ticket_evidence(
                            completion_evidence,
                            expected_ticket=ticket,
                        )
                    )
                    if updated.state in {
                        TransferStatusState.FAILED,
                        TransferStatusState.CANCELED,
                    }:
                        completion_ticket = ticket
                except ValueError as exc:
                    return DaemonResponse(ok=False, error=str(exc))
            self._transfer_statuses[updated.transfer_id] = updated
            if updated.state in {
                TransferStatusState.FAILED,
                TransferStatusState.CANCELED,
            }:
                if completion_ticket is not None:
                    self._transfer_completion_tickets[updated.transfer_id] = (
                        completion_ticket
                    )
                self._mark_transfer_admission_terminal_locked(
                    updated.transfer_id,
                    updated.state,
                    reason=updated.error,
                )
                self._drop_execution_ticket_for_transfer_locked(updated.transfer_id)
            if updated.state is TransferStatusState.COMPLETE:
                self._transfer_completion_sources[updated.transfer_id] = (
                    normalized_completion_source
                )
                if completion_ticket is not None:
                    self._transfer_completion_tickets[updated.transfer_id] = (
                        completion_ticket
                    )
                    self._drop_execution_ticket_for_transfer_locked(updated.transfer_id)
            if normalized_completion_evidence is not None:
                self._transfer_completion_sources[updated.transfer_id] = (
                    normalized_completion_source
                )
                existing_evidence = self._transfer_completion_evidence.get(
                    updated.transfer_id
                )
                self._transfer_completion_evidence[updated.transfer_id] = (
                    _merge_completion_evidence(
                        existing_evidence,
                        normalized_completion_evidence,
                    )
                )
            self._refresh_transfer_queue_record_locked(updated.transfer_id)
            removed = _empty_removed_summary()
            promoted = ()
            if updated.state is TransferStatusState.COMPLETE:
                self._append_transfer_audit_records_locked(
                    event_type="worker_completion",
                    transfer_id=updated.transfer_id,
                    state=updated.state,
                    bytes_completed=updated.bytes_completed,
                )
            elif updated.state is TransferStatusState.FAILED:
                self._append_transfer_audit_records_locked(
                    event_type="worker_failure",
                    transfer_id=updated.transfer_id,
                    state=updated.state,
                    reason=updated.error or "worker_failed",
                    failure_reason=updated.error or "worker_failed",
                    bytes_completed=updated.bytes_completed,
                )
                _merge_removed(
                    removed,
                    self._release_reservations_for_transfer_locked(
                        updated.transfer_id,
                        final_state=TransferStatusState.FAILED,
                        cleanup_reason=updated.error or "worker_failed",
                    ),
                )
                promoted = self._promote_delayed_transfers_locked(now=time.time())
            elif updated.state is TransferStatusState.CANCELED:
                self._append_transfer_audit_records_locked(
                    event_type="transfer_canceled",
                    transfer_id=updated.transfer_id,
                    state=updated.state,
                    reason=updated.error or "transfer_canceled",
                    failure_reason=updated.error or "transfer_canceled",
                    bytes_completed=updated.bytes_completed,
                )
                _merge_removed(
                    removed,
                    self._release_reservations_for_transfer_locked(
                        updated.transfer_id,
                        final_state=TransferStatusState.CANCELED,
                        cleanup_reason=updated.error or "transfer_canceled",
                    ),
                )
                promoted = self._promote_delayed_transfers_locked(now=time.time())
            return DaemonResponse(
                ok=True,
                payload={
                    "status": asdict(updated),
                    "removed": removed,
                    "promoted_transfers": promoted,
                },
            )

    def _supplement_terminal_completion_evidence_locked(
        self,
        status: TransferStatus,
        *,
        completion_source: str | None,
        completion_evidence: Mapping[str, object] | None,
    ) -> DaemonResponse:
        if completion_evidence is None:
            return DaemonResponse(ok=True)
        normalized_completion_source = str(completion_source or "").lower()
        if not _is_execution_completion_source(normalized_completion_source):
            return DaemonResponse(
                ok=False,
                error="terminal evidence update requires worker/backend source",
            )
        if not self._intent_requires_execution_evidence_locked(status.transfer_id):
            return DaemonResponse(ok=True)
        ticket = self._completion_ticket_for_transfer_locked(status.transfer_id)
        if ticket is None:
            return DaemonResponse(
                ok=False,
                error="terminal evidence update requires daemon ticket",
            )
        try:
            if status.state is TransferStatusState.COMPLETE:
                supplemental = _normalize_completion_evidence(
                    completion_evidence,
                    expected_bytes=status.bytes_total,
                    completion_source=normalized_completion_source,
                    expected_ticket=ticket,
                )
            else:
                supplemental = _normalize_status_ticket_evidence(
                    completion_evidence,
                    expected_ticket=ticket,
                )
        except ValueError as exc:
            return DaemonResponse(ok=False, error=str(exc))
        existing = dict(self._transfer_completion_evidence.get(status.transfer_id, {}))
        self._transfer_completion_sources[status.transfer_id] = normalized_completion_source
        self._transfer_completion_evidence[status.transfer_id] = (
            _merge_completion_evidence(existing, supplemental)
        )
        self._archive_transfer_receipt_state_locked(status.transfer_id)
        self._refresh_transfer_queue_record_locked(status.transfer_id)
        return DaemonResponse(ok=True)

    def submit_transfer_intent(
        self,
        intent: TransferIntent,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        if not isinstance(intent, TransferIntent):
            return DaemonResponse(ok=False, error="intent must be a TransferIntent")
        try:
            chunk_bytes = _intent_chunk_bytes(intent)
        except (TypeError, ValueError) as exc:
            return DaemonResponse(ok=False, error=str(exc))
        with self._lock:
            terminal_receipt = self._terminal_receipt_response_for_intent_locked(
                intent,
                peer_identity=peer_identity,
            )
            if terminal_receipt is not None:
                return terminal_receipt
            existing_transfer_id = self._intent_transfers.get(intent.intent_id)
            if existing_transfer_id is not None:
                existing = self._transfer_intents.get(intent.intent_id)
                if existing != intent:
                    return DaemonResponse(
                        ok=False,
                        error="intent_id already belongs to a different transfer intent",
                    )
                try:
                    self._validate_transfer_buffers_locked(
                        (intent.source_buffer_id, intent.destination_buffer_id),
                        job_id=intent.job_id,
                        session_id=intent.session_id,
                        peer_identity=peer_identity,
                    )
                except ValueError as exc:
                    return DaemonResponse(ok=False, error=str(exc))
                return self._intent_execution_payload_response_locked(
                    intent=intent,
                    transfer_id=existing_transfer_id,
                    now=time.time(),
                )

        planned = self._plan_transfer(
            session_id=intent.session_id,
            total_bytes=intent.total_bytes,
            chunk_bytes=chunk_bytes,
            mode="auto",
            direction=intent.direction,
            job_id=intent.job_id,
            buffer_ids=[intent.source_buffer_id, intent.destination_buffer_id],
            ranges=intent.ranges,
            intent_id=intent.intent_id,
            workload_kind=intent.workload_kind.value,
            priority=intent.priority,
            peer_identity=peer_identity,
            allow_delayed_admission=True,
        )
        if not planned.ok:
            return planned

        transfer_id = str(planned.payload["transfer_id"])
        now = time.time()
        with self._lock:
            decision = self._scheduling_decisions.get(transfer_id)
            if decision is None:
                return DaemonResponse(ok=False, error="scheduling decision is unavailable")
            self._transfer_intents[intent.intent_id] = intent
            self._intent_transfers[intent.intent_id] = transfer_id
            ticket = None
            admission = self._transfer_admissions.get(transfer_id, {})
            if admission.get("state") == _ADMISSION_ADMITTED:
                ticket = self._execution_ticket_for_intent_locked(
                    intent=intent,
                    transfer_id=transfer_id,
                    decision=decision,
                    now=now,
                )
                self._execution_tickets[ticket.ticket_id] = ticket
                self._transfer_tickets[transfer_id] = ticket.ticket_id
            self._refresh_transfer_queue_record_locked(transfer_id, now=now)
            receipt = self._receipt_for_intent_locked(intent.intent_id)
            return DaemonResponse(
                ok=True,
                payload={
                    "receipt": asdict(receipt),
                    "transfer_id": transfer_id,
                    "decision": asdict(decision),
                    "ticket": None if ticket is None else asdict(ticket),
                    "admission": planned.payload.get("admission", {}),
                    "plan_generation": planned.payload.get("plan_generation", 0),
                    "plan_expires_at": planned.payload.get("plan_expires_at"),
                    "reservations": planned.payload.get("reservations", []),
                    "lease_tokens": planned.payload.get("lease_tokens", []),
                    "planning": planned.payload.get("planning", {}),
                },
            )

    def _terminal_receipt_response_for_intent_locked(
        self,
        intent: TransferIntent,
        *,
        peer_identity: PeerIdentity | None,
    ) -> DaemonResponse | None:
        normalized_intent_id = str(intent.intent_id)
        transfer_id = self._intent_transfers.get(normalized_intent_id)
        if transfer_id is None:
            transfer_id = self._archived_intent_transfers.get(normalized_intent_id)
        if transfer_id is None:
            return None
        archived = self._transfer_receipt_archive.get(str(transfer_id), {})
        existing_intent = self._transfer_intents.get(normalized_intent_id)
        if existing_intent is None and isinstance(archived.get("intent"), TransferIntent):
            existing_intent = archived["intent"]
        if existing_intent != intent:
            return DaemonResponse(
                ok=False,
                error="intent_id already belongs to a different transfer intent",
            )
        status = self._transfer_statuses.get(str(transfer_id))
        if status is None and isinstance(archived.get("status"), TransferStatus):
            status = archived["status"]
        if status is None or status.state not in _TERMINAL_TRANSFER_STATES:
            return None
        try:
            self._validate_peer_owns_receipt_transfer_locked(
                transfer_id=transfer_id,
                job_id=intent.job_id,
                peer_identity=peer_identity,
            )
        except ValueError as exc:
            return DaemonResponse(ok=False, error=str(exc))
        try:
            receipt = self._receipt_for_intent_locked(normalized_intent_id)
        except ValueError as exc:
            return DaemonResponse(ok=False, error=str(exc))
        return DaemonResponse(ok=True, payload={"receipt": asdict(receipt)})

    def _intent_execution_payload_response_locked(
        self,
        *,
        intent: TransferIntent,
        transfer_id: str,
        now: float,
    ) -> DaemonResponse:
        try:
            receipt = self._receipt_for_intent_locked(intent.intent_id)
        except ValueError as exc:
            return DaemonResponse(ok=False, error=str(exc))
        status = self._transfer_statuses.get(str(transfer_id))
        decision = self._scheduling_decisions.get(str(transfer_id))
        session = self._sessions.get(intent.session_id)
        if status is None:
            return DaemonResponse(ok=False, error="transfer status is unavailable")
        if decision is None:
            return DaemonResponse(ok=False, error="scheduling decision is unavailable")
        if session is None:
            return DaemonResponse(ok=False, error="transfer session is unavailable")

        if (
            self._transfer_admissions.get(str(transfer_id), {}).get("state")
            == _ADMISSION_DELAYED
        ):
            self._promote_delayed_transfers_locked(now=now)
            receipt = self._receipt_for_intent_locked(intent.intent_id)
            status = self._transfer_statuses.get(str(transfer_id))
            decision = self._scheduling_decisions.get(str(transfer_id))
            if status is None:
                return DaemonResponse(ok=False, error="transfer status is unavailable")
            if decision is None:
                return DaemonResponse(ok=False, error="scheduling decision is unavailable")

        relay_eligibility = self._relay_eligibility_for_session_locked(session)
        planning_relays = tuple(
            int(item["relay_gpu"]) for item in relay_eligibility["eligible_relays"]
        )
        reservations = self._reservations_for_transfer_locked(str(transfer_id))
        payload = self._planned_transfer_payload_locked(
            transfer_id=str(transfer_id),
            decision=decision,
            status=status,
            session=session,
            planning_relays=planning_relays,
            relay_eligibility=relay_eligibility,
            reservations=reservations,
        )
        payload["receipt"] = asdict(receipt)
        return DaemonResponse(ok=True, payload=payload)

    def wait_transfer_receipt(
        self,
        intent_id: str,
        timeout_seconds: float | None = None,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        normalized_intent_id = str(intent_id)
        deadline = (
            None
            if timeout_seconds is None
            else time.time() + max(0.0, float(timeout_seconds))
        )
        while True:
            with self._lock:
                try:
                    receipt = self._receipt_for_intent_locked(normalized_intent_id)
                    transfer_id = self._intent_transfers.get(normalized_intent_id)
                    if transfer_id is None:
                        transfer_id = self._archived_intent_transfers.get(
                            normalized_intent_id
                        )
                    self._validate_peer_owns_receipt_transfer_locked(
                        transfer_id=transfer_id,
                        job_id=receipt.job_id,
                        peer_identity=peer_identity,
                    )
                except ValueError as exc:
                    return DaemonResponse(ok=False, error=str(exc))
                if receipt.state in _TERMINAL_TRANSFER_STATES:
                    return DaemonResponse(ok=True, payload={"receipt": asdict(receipt)})
                if deadline is not None and time.time() >= deadline:
                    return DaemonResponse(ok=True, payload={"receipt": asdict(receipt)})
            if deadline is None:
                time.sleep(0.01)
                continue
            time.sleep(min(0.01, max(0.0, deadline - time.time())))

    def validate_lease(
        self,
        lease_id: str,
        token: str,
        session_id: str | None = None,
        relay_gpu: int | None = None,
        job_id: str | None = None,
        buffer_ids: Iterable[str] | None = None,
        now: float | None = None,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        checked_at = time.time() if now is None else float(now)
        with self._lock:
            self._reap_stale_sessions_locked(checked_at)
            lease = self._lease_tokens.get(str(lease_id))
            if lease is None:
                return DaemonResponse(ok=False, error="unknown lease")
            if lease.token != str(token):
                return DaemonResponse(ok=False, error="invalid lease token")
            if session_id is not None and lease.session_id != str(session_id):
                return DaemonResponse(ok=False, error="lease session mismatch")
            if relay_gpu is not None and lease.relay_gpu != int(relay_gpu):
                return DaemonResponse(ok=False, error="lease relay mismatch")
            if job_id is not None and lease.job_id != str(job_id):
                return DaemonResponse(ok=False, error="lease job mismatch")
            owner_job_id = lease.job_id if lease.job_id is not None else job_id
            if owner_job_id is not None:
                try:
                    self._validate_peer_owns_job_locked(
                        job_id=str(owner_job_id),
                        peer_identity=peer_identity,
                    )
                except ValueError as exc:
                    return DaemonResponse(ok=False, error=str(exc))
            if buffer_ids is not None:
                requested_buffers = tuple(str(buffer_id) for buffer_id in buffer_ids)
                if requested_buffers != lease.buffer_ids:
                    return DaemonResponse(ok=False, error="lease buffer mismatch")
                for buffer_id in requested_buffers:
                    buffer = self._buffers.get(buffer_id)
                    if buffer is None:
                        return DaemonResponse(ok=False, error="unknown buffer")
                    if lease.job_id is not None and buffer.job_id != lease.job_id:
                        return DaemonResponse(ok=False, error="lease buffer owner mismatch")
                    try:
                        self._validate_peer_owns_buffer_locked(
                            buffer_id=buffer_id,
                            peer_identity=peer_identity,
                        )
                    except ValueError as exc:
                        return DaemonResponse(ok=False, error=str(exc))
            if lease.expires_at and checked_at > lease.expires_at:
                self._release_expired_lease_locked(lease.lease_id)
                return DaemonResponse(ok=False, error="lease expired")
            if lease.lease_id not in self._reservations:
                return DaemonResponse(ok=False, error="lease is not active")
            transfer_id = self._reservation_transfers.get(lease.lease_id)
            if transfer_id is not None:
                ticket_id = self._transfer_tickets.get(transfer_id)
                if ticket_id is not None:
                    ticket = self._execution_tickets.get(ticket_id)
                    if ticket is not None and checked_at > ticket.expires_at:
                        return DaemonResponse(ok=False, error="execution ticket expired")
                admission_error = self._validate_transfer_admission_locked(
                    transfer_id,
                    lease_id=lease.lease_id,
                    now=checked_at,
                )
                if admission_error is not None:
                    return DaemonResponse(ok=False, error=admission_error)
                status = self._transfer_statuses.get(transfer_id)
                if status is not None and status.state in _TERMINAL_TRANSFER_STATES:
                    return DaemonResponse(ok=False, error="transfer is terminal")
            return DaemonResponse(ok=True, payload={"lease_token": asdict(lease)})

    def authorize_worker_transfer(
        self,
        request: WorkerTransferAuthorizationRequest,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        now = time.time()
        with self._lock:
            self._reap_stale_sessions_locked(now)
            status = self._transfer_statuses.get(request.transfer_id)
            if status is None:
                return DaemonResponse(ok=False, error="unknown transfer")
            if status.session_id != request.session_id:
                return DaemonResponse(ok=False, error="transfer session mismatch")
            if status.job_id != request.job_id:
                return DaemonResponse(ok=False, error="transfer job mismatch")
            try:
                self._validate_peer_owns_job_locked(
                    job_id=request.job_id,
                    peer_identity=peer_identity,
                )
            except ValueError as exc:
                return DaemonResponse(ok=False, error=str(exc))
            if status.state in _TERMINAL_TRANSFER_STATES:
                return DaemonResponse(ok=False, error="transfer is terminal")
            lease = self._lease_tokens.get(request.lease_id)
            if lease is None:
                return DaemonResponse(ok=False, error="unknown lease")
            if lease.token != request.token:
                return DaemonResponse(ok=False, error="invalid lease token")
            if lease.session_id != request.session_id:
                return DaemonResponse(ok=False, error="lease session mismatch")
            if lease.job_id != request.job_id:
                return DaemonResponse(ok=False, error="lease job mismatch")
            if request.relay_gpu is not None and lease.relay_gpu != request.relay_gpu:
                return DaemonResponse(ok=False, error="lease relay mismatch")
            if lease.expires_at and now > lease.expires_at:
                self._release_expired_lease_locked(lease.lease_id)
                return DaemonResponse(ok=False, error="lease expired")
            if lease.lease_id not in self._reservations:
                return DaemonResponse(ok=False, error="lease is not active")
            admission_error = self._validate_transfer_admission_locked(
                request.transfer_id,
                lease_id=lease.lease_id,
                now=now,
            )
            if admission_error is not None:
                return DaemonResponse(ok=False, error=admission_error)
            reservation = self._reservations[lease.lease_id]
            if reservation.direction not in {"unknown", request.direction}:
                return DaemonResponse(ok=False, error="reservation direction mismatch")
            plan = self._transfer_plans.get(request.transfer_id)
            if plan is None:
                return DaemonResponse(ok=False, error="transfer plan is unavailable")
            related_leases = self._leases_for_worker_plan_locked(
                request,
                primary_lease=lease,
            )
            if len(related_leases) > 1:
                related_lease_ids = {item.lease_id for item in related_leases}
                admission_error = self._validate_transfer_admission_locked(
                    request.transfer_id,
                    lease_id=None,
                    now=now,
                )
                if admission_error is not None:
                    return DaemonResponse(ok=False, error=admission_error)
                for related_lease in related_leases:
                    if related_lease.lease_id == lease.lease_id:
                        continue
                    admission_error = self._validate_transfer_admission_locked(
                        request.transfer_id,
                        lease_id=related_lease.lease_id,
                        now=now,
                    )
                    if admission_error is not None:
                        return DaemonResponse(ok=False, error=admission_error)
                    if related_lease.expires_at and now > related_lease.expires_at:
                        self._release_expired_lease_locked(related_lease.lease_id)
                        return DaemonResponse(ok=False, error="lease expired")
                    if related_lease.lease_id not in self._reservations:
                        return DaemonResponse(ok=False, error="lease is not active")
                    if related_lease.session_id != request.session_id:
                        return DaemonResponse(ok=False, error="lease session mismatch")
                    if related_lease.job_id != request.job_id:
                        return DaemonResponse(ok=False, error="lease job mismatch")
                    if related_lease.buffer_ids != lease.buffer_ids:
                        return DaemonResponse(ok=False, error="lease buffer mismatch")
                admission = self._transfer_admissions.get(request.transfer_id, {})
                admission_lease_ids = set(
                    str(item) for item in admission.get("lease_ids", ()) or ()
                )
                if admission_lease_ids and admission_lease_ids != related_lease_ids:
                    return DaemonResponse(ok=False, error="worker lease set mismatch")
            else:
                related_leases = (lease,)
            try:
                authorized_ranges = _relay_ranges_from_plan(
                    plan,
                    relay_gpu=tuple(item.relay_gpu for item in related_leases),
                    direction=request.direction,
                )
            except ValueError as exc:
                return DaemonResponse(ok=False, error=str(exc))
            if request.ranges and request.ranges != authorized_ranges:
                return DaemonResponse(ok=False, error="worker ranges do not match daemon plan")
            requested_bytes = sum(item["bytes"] for item in authorized_ranges)
            reservation_bytes = sum(
                int(self._reservations[item.lease_id].bytes)
                for item in related_leases
                if item.lease_id in self._reservations
            )
            if requested_bytes > reservation_bytes:
                return DaemonResponse(ok=False, error="authorization exceeds reservation bytes")
            required_buffers = (request.src_buffer_id, request.dst_buffer_id)
            if required_buffers != lease.buffer_ids:
                return DaemonResponse(ok=False, error="lease buffer mismatch")
            src_buffer = self._buffers.get(request.src_buffer_id)
            dst_buffer = self._buffers.get(request.dst_buffer_id)
            if src_buffer is None or dst_buffer is None:
                return DaemonResponse(ok=False, error="unknown buffer")
            session = self._sessions.get(request.session_id)
            if session is None:
                return DaemonResponse(ok=False, error="transfer session is unavailable")
            relay_eligibility = self._relay_eligibility_for_session_locked(session)
            planning_relays = tuple(
                int(item["relay_gpu"]) for item in relay_eligibility["eligible_relays"]
            )
            profile_entry = self._profile_cache.get(
                self._profile_key(session.target_gpu, planning_relays)
            )
            if profile_entry is None and planning_relays != tuple(session.relay_gpus):
                profile_entry = self._profile_cache.get(
                    self._profile_key(session.target_gpu, session.relay_gpus)
                )
            try:
                self._validate_peer_owns_buffer_locked(
                    buffer_id=src_buffer.buffer_id,
                    peer_identity=peer_identity,
                )
                self._validate_peer_owns_buffer_locked(
                    buffer_id=dst_buffer.buffer_id,
                    peer_identity=peer_identity,
                )
            except ValueError as exc:
                return DaemonResponse(ok=False, error=str(exc))
            authorization = WorkerTransferAuthorization(
                transfer_id=request.transfer_id,
                lease_id=request.lease_id,
                session_id=request.session_id,
                job_id=request.job_id,
                src_buffer=src_buffer,
                dst_buffer=dst_buffer,
                direction=request.direction,
                ranges=authorized_ranges,
                relay_gpu=lease.relay_gpu,
                plan=plan,
            )
            ticket = self._execution_ticket_for_worker_locked(
                authorization,
                leases=related_leases,
                transfer_id=request.transfer_id,
                now=now,
            )
            self._execution_tickets[ticket.ticket_id] = ticket
            self._transfer_tickets[request.transfer_id] = ticket.ticket_id
            staging_records = self._register_worker_staging_records_locked(
                leases=related_leases,
                transfer_id=request.transfer_id,
                direction=request.direction,
                plan=plan,
                now=now,
            )
            for related_lease in related_leases:
                related_reservation = self._reservations.get(related_lease.lease_id)
                if related_reservation is None:
                    continue
                self._append_audit_record_locked(
                    event_type="relay_authorized",
                    transfer_id=request.transfer_id,
                    reservation=related_reservation,
                    lease=related_lease,
                    staging_record=staging_records[related_lease.lease_id],
                ticket=ticket,
                state=status.state,
                reason="worker_authorized",
                bytes_completed=status.bytes_completed,
                now=now,
                )
            decision = self._scheduling_decisions.get(request.transfer_id)
            return DaemonResponse(
                ok=True,
                payload={
                    "ticket": asdict(ticket),
                    "decision": None if decision is None else asdict(decision),
                    "src_buffer": asdict(src_buffer),
                    "dst_buffer": asdict(dst_buffer),
                    "relay_gpu": lease.relay_gpu,
                    "relay_gpus": tuple(item.relay_gpu for item in related_leases),
                    "lease_id": request.lease_id,
                    "lease_ids": tuple(item.lease_id for item in related_leases),
                    "transfer_id": request.transfer_id,
                    "authorized_at": now,
                    "plan_generation": self._transfer_plan_generations.get(
                        request.transfer_id,
                        0,
                    ),
                    "planning": {
                        "target_gpu": session.target_gpu,
                        "profile_key": self._profile_key(
                            session.target_gpu,
                            planning_relays,
                        ),
                        "profile_entry": (
                            None if profile_entry is None else dict(profile_entry)
                        ),
                        "relay_eligibility": relay_eligibility,
                    },
                    "staging_record": dict(staging_records[lease.lease_id]),
                    "staging_records": [
                        dict(staging_records[item.lease_id])
                        for item in related_leases
                    ],
                },
            )

    def _plan_transfer(
        self,
        session_id: str,
        total_bytes: int,
        chunk_bytes: int,
        mode: str = "pool",
        direction: str = "h2d",
        job_id: str | None = None,
        buffer_ids: Iterable[str] | None = None,
        ranges: Iterable[dict[str, int]] | None = None,
        intent_id: str | None = None,
        topology_snapshot_id: str | None = None,
        workload_kind: str = "generic",
        priority: int = 0,
        peer_identity: PeerIdentity | None = None,
        allow_delayed_admission: bool = False,
    ) -> DaemonResponse:
        now = time.time()
        try:
            normalized_ranges = _normalize_transfer_ranges(ranges)
            if normalized_ranges is not None:
                range_bytes = sum(item["bytes"] for item in normalized_ranges)
                if range_bytes != int(total_bytes):
                    return DaemonResponse(
                        ok=False,
                        error="range bytes must match total_bytes",
                    )
        except (KeyError, TypeError, ValueError) as exc:
            return DaemonResponse(ok=False, error=str(exc))
        with self._lock:
            self._reap_stale_sessions_locked(now)
            self._reap_expired_leases_locked(now)
            self._purge_stale_profiles_locked(now)
            try:
                (
                    session,
                    decision,
                    buffer_ids_tuple,
                    plan_job_id,
                    relay_eligibility,
                    planning_relays,
                    snapshot_id,
                ) = self._scheduler_decision_for_transfer_locked(
                    session_id=session_id,
                    total_bytes=total_bytes,
                    chunk_bytes=chunk_bytes,
                    mode=mode,
                    direction=direction,
                    job_id=job_id,
                    buffer_ids=buffer_ids,
                    normalized_ranges=normalized_ranges,
                    intent_id=intent_id,
                    topology_snapshot_id=topology_snapshot_id,
                    workload_kind=workload_kind,
                    priority=priority,
                    peer_identity=peer_identity,
                    now=now,
                    defer_relay_admission=allow_delayed_admission,
                )
            except ValueError as exc:
                if str(exc) == _TOPOLOGY_UNAVAILABLE_ERROR:
                    return _topology_unavailable_response()
                return DaemonResponse(ok=False, error=str(exc))
            transfer_id = str(uuid.uuid4())
            self._transfer_plan_generations[transfer_id] = 1
            admission = self._admission_for_decision_locked(
                decision,
                session=session,
                allow_delayed=allow_delayed_admission,
                now=now,
            )
            reservations = []
            if admission["state"] == _ADMISSION_ADMITTED:
                reservations = self._commit_scheduler_leases_locked(
                    session,
                    decision,
                    transfer_id=transfer_id,
                    buffer_ids=buffer_ids_tuple,
                )
                admission["lease_ids"] = tuple(
                    reservation.reservation_id for reservation in reservations
                )
            status = TransferStatus(
                transfer_id=transfer_id,
                job_id=str(plan_job_id or session.session_id),
                state=TransferStatusState.SUBMITTED,
                bytes_total=int(total_bytes),
                bytes_completed=0,
                session_id=session.session_id,
            )
            self._transfer_statuses[transfer_id] = status
            transfer_peer_identity = self._transfer_peer_identity_for_owner_locked(
                job_id=status.job_id,
                session_id=session.session_id,
                peer_identity=peer_identity,
            )
            if transfer_peer_identity is not None:
                self._transfer_peer_identities[transfer_id] = transfer_peer_identity
            self._transfer_plans[transfer_id] = dict(decision.plan)
            self._scheduling_decisions[transfer_id] = decision
            self._transfer_plan_requests[transfer_id] = {
                "session_id": session.session_id,
                "total_bytes": int(total_bytes),
                "chunk_bytes": int(chunk_bytes),
                "mode": str(mode),
                "direction": str(direction).lower(),
                "job_id": None if plan_job_id is None else str(plan_job_id),
                "buffer_ids": buffer_ids_tuple,
                "ranges": normalized_ranges,
                "intent_id": None if intent_id is None else str(intent_id),
                "topology_snapshot_id": topology_snapshot_id,
                "workload_kind": str(workload_kind),
                "priority": int(priority),
            }
            self._transfer_plan_expirations[transfer_id] = (
                self._plan_expires_at_for_decision(decision, now=now)
            )
            admission = {
                **admission,
                "plan_generation": 1,
                "plan_expires_at": self._transfer_plan_expirations[transfer_id],
            }
            self._transfer_admissions[transfer_id] = admission
            self._record_planned_transfer_locked(
                transfer_id=transfer_id,
                status=status,
                intent_id=intent_id,
                buffer_ids=buffer_ids_tuple,
                total_bytes=total_bytes,
                chunk_bytes=chunk_bytes,
                ranges=normalized_ranges,
                direction=direction,
                decision=decision,
                now=now,
            )
            self._touch_session_locked(session.session_id, now)
            if (
                not reservations
                and len(buffer_ids_tuple) >= 2
                and _decision_is_direct_only(decision)
            ):
                ticket = self._execution_ticket_for_plan_locked(
                    transfer_id=transfer_id,
                    decision=decision,
                    source_buffer_id=buffer_ids_tuple[0],
                    destination_buffer_id=buffer_ids_tuple[1],
                    now=now,
                    lease_ids=(),
                )
                self._execution_tickets[ticket.ticket_id] = ticket
                self._transfer_tickets[transfer_id] = ticket.ticket_id
            payload = self._planned_transfer_payload_locked(
                transfer_id=transfer_id,
                decision=decision,
                status=status,
                session=session,
                planning_relays=planning_relays,
                relay_eligibility=relay_eligibility,
                reservations=reservations,
            )
            return DaemonResponse(ok=True, payload=payload)

    def get_profile(self, target_gpu: int, relay_gpus: Iterable[int]) -> DaemonResponse:
        key = self._profile_key(target_gpu, relay_gpus)
        with self._lock:
            self._purge_stale_profiles_locked(time.time())
            return DaemonResponse(
                ok=True,
                payload={"profile": daemon_profiles.cached_profile(self._profile_cache, key)},
            )

    def put_profile(
        self,
        target_gpu: int,
        relay_gpus: Iterable[int],
        profile: dict,
        profile_bytes: int = 0,
        updated_at: float | None = None,
    ) -> DaemonResponse:
        target = int(target_gpu)
        relays = self._normalize_relays(relay_gpus)
        entry = daemon_profiles.profile_entry(
            target_gpu=target,
            relay_gpus=relays,
            profile=profile,
            profile_bytes=int(profile_bytes),
            updated_at=float(time.time() if updated_at is None else updated_at),
        )
        key = self._profile_key(target, relays)
        with self._lock:
            self._purge_stale_profiles_locked(time.time())
            stored = daemon_profiles.put_cached_profile(self._profile_cache, key, entry)
        return DaemonResponse(ok=True, payload={"profile": stored})

    def invalidate_profile(self, target_gpu: int, relay_gpus: Iterable[int]) -> DaemonResponse:
        key = self._profile_key(target_gpu, relay_gpus)
        with self._lock:
            removed = daemon_profiles.invalidate_cached_profile(self._profile_cache, key)
            return DaemonResponse(
                ok=True,
                payload={
                    "profile_key": key,
                    "removed": removed,
                },
            )

    def reap_stale_sessions(self, now: float | None = None) -> list[str]:
        with self._lock:
            return self._reap_stale_sessions_locked(time.time() if now is None else float(now))

    def reap_expired_leases(self, now: float | None = None) -> list[str]:
        with self._lock:
            checked_at = time.time() if now is None else float(now)
            expired = self._reap_expired_leases_locked(checked_at)
            if expired:
                self._promote_delayed_transfers_locked(now=checked_at)
            return expired

    def _release_reservation_locked(
        self,
        reservation_id: str,
        final_state: TransferStatusState = TransferStatusState.COMPLETE,
        cleanup_reason: str | None = None,
        mark_terminal: bool = True,
    ) -> TransferReservation | None:
        reservation_key = str(reservation_id)
        reservation = self._reservations.get(reservation_key)
        if reservation is None:
            staging_record = self._staging_records.pop(reservation_key, None)
            if staging_record is not None and cleanup_reason is not None:
                archived_peer = None
                job_id = staging_record.get("job_id")
                if job_id is not None:
                    archived_peer = self._job_peer_identities.get(str(job_id))
                if archived_peer is None:
                    for buffer_id in staging_record.get("buffer_ids", ()) or ():
                        buffer = self._buffers.get(str(buffer_id))
                        if buffer is None:
                            continue
                        archived_peer = self._job_peer_identities.get(buffer.job_id)
                        if archived_peer is not None:
                            break
                self._archive_cleanup_target_locked(
                    target_kind="reservation",
                    target_id=reservation_key,
                    peer_identity=archived_peer,
                    reason=cleanup_reason,
                    transfer_ids=(() if transfer_id is None else (str(transfer_id),)),
                )
                self._append_audit_record_locked(
                    event_type="cleanup",
                    staging_record=staging_record,
                    state=final_state,
                    reason=cleanup_reason,
                    failure_reason=(
                        cleanup_reason
                        if final_state
                        in {TransferStatusState.FAILED, TransferStatusState.CANCELED}
                        else None
                    ),
                    cleanup_kind="reservation",
                    cleanup_target_id=reservation_key,
                )
                self._system_cleanup_events.append(
                    CleanupRequest(
                        target_kind="reservation",
                        target_id=reservation_id,
                        reason=cleanup_reason,
                        force=True,
                    )
                )
            return None
        transfer_id = self._reservation_transfers.get(reservation_key)
        lease = self._lease_tokens.get(reservation_key)
        staging_record = self._staging_records.get(reservation_key)
        if reservation is not None:
            self._archive_cleanup_target_locked(
                target_kind="reservation",
                target_id=reservation_key,
                peer_identity=(
                    None
                    if lease is None or lease.job_id is None
                    else self._job_peer_identities.get(str(lease.job_id))
                ),
                reason=cleanup_reason,
                transfer_ids=(() if transfer_id is None else (str(transfer_id),)),
            )
        if cleanup_reason is not None:
            self._append_audit_record_locked(
                event_type="cleanup",
                transfer_id=transfer_id,
                reservation=reservation,
                lease=lease,
                staging_record=staging_record,
                state=final_state,
                reason=cleanup_reason,
                failure_reason=(
                    cleanup_reason
                    if final_state in {TransferStatusState.FAILED, TransferStatusState.CANCELED}
                    else None
                ),
                cleanup_kind="reservation",
                cleanup_target_id=reservation_key,
            )
        self._reservations.pop(reservation_key, None)
        self._lease_tokens.pop(reservation_key, None)
        self._lease_plan_generations.pop(reservation_key, None)
        self._staging_records.pop(reservation_key, None)
        transfer_id = self._reservation_transfers.pop(reservation_key, None)
        session = self._sessions.get(reservation.session_id)
        if session is not None:
            session.active_chunks = max(0, session.active_chunks - reservation.chunks)
        quota = self._relay_quotas.get(reservation.relay_gpu)
        if quota is not None:
            quota.active_chunks = max(0, quota.active_chunks - reservation.chunks)
        if transfer_id is not None and mark_terminal:
            self._mark_transfer_terminal_if_unblocked_locked(
                transfer_id,
                final_state,
                error=(
                    cleanup_reason
                    if final_state in {TransferStatusState.FAILED, TransferStatusState.CANCELED}
                    else None
                ),
            )
            status_after = self._transfer_statuses.get(transfer_id)
            if (
                status_after is not None
                and status_after.state in _TERMINAL_TRANSFER_STATES
                and not self._transfer_has_reservations_locked(transfer_id)
            ):
                if status_after.state is TransferStatusState.COMPLETE:
                    self._retire_completed_transfer_lease_state_locked(
                        transfer_id,
                        reason=cleanup_reason,
                    )
                else:
                    self._retire_transfer_runtime_state_locked(transfer_id)
        if cleanup_reason is not None:
            self._system_cleanup_events.append(
                CleanupRequest(
                    target_kind="reservation",
                    target_id=reservation_id,
                    reason=cleanup_reason,
                    force=True,
                )
            )
        return reservation

    def _release_reservation_and_count_locked(
        self,
        reservation_id: str,
        final_state: TransferStatusState,
        cleanup_reason: str | None = None,
        mark_terminal: bool = True,
    ) -> dict[str, int]:
        removed = _empty_removed_summary()
        transfer_id = self._reservation_transfers.get(str(reservation_id))
        status_before = (
            None if transfer_id is None else self._transfer_statuses.get(transfer_id)
        )
        staging_record = self._staging_records.get(str(reservation_id))
        reservation = self._release_reservation_locked(
            str(reservation_id),
            final_state=final_state,
            cleanup_reason=cleanup_reason,
            mark_terminal=mark_terminal,
        )
        if reservation is not None:
            removed["reservations"] += 1
        if staging_record is not None:
            removed["staging_records"] += 1
        if status_before is not None and status_before.state not in _TERMINAL_TRANSFER_STATES:
            status_after = self._transfer_statuses.get(status_before.transfer_id)
            if status_after is None or status_after.state in _TERMINAL_TRANSFER_STATES:
                removed["transfers"] += 1
        return removed

    def _release_reservations_for_transfer_locked(
        self,
        transfer_id: str,
        final_state: TransferStatusState,
        cleanup_reason: str | None = None,
        mark_terminal: bool = True,
    ) -> dict[str, int]:
        removed = _empty_removed_summary()
        for reservation_id, mapped_transfer_id in list(self._reservation_transfers.items()):
            if mapped_transfer_id != str(transfer_id):
                continue
            _merge_removed(
                removed,
                self._release_reservation_and_count_locked(
                    reservation_id,
                    final_state=final_state,
                    cleanup_reason=cleanup_reason,
                    mark_terminal=mark_terminal,
                ),
            )
        return removed

    def _reservations_for_transfer_locked(
        self,
        transfer_id: str,
    ) -> list[TransferReservation]:
        normalized_transfer_id = str(transfer_id)
        return [
            self._reservations[reservation_id]
            for reservation_id, mapped_transfer_id in sorted(
                self._reservation_transfers.items()
            )
            if mapped_transfer_id == normalized_transfer_id
            and reservation_id in self._reservations
        ]

    def _scheduler_decision_for_transfer_locked(
        self,
        *,
        session_id: str,
        total_bytes: int,
        chunk_bytes: int,
        mode: str,
        direction: str,
        job_id: str | None,
        buffer_ids: Iterable[str] | None,
        normalized_ranges: Iterable[dict[str, int]] | None,
        intent_id: str | None,
        topology_snapshot_id: str | None,
        workload_kind: str,
        priority: int,
        peer_identity: PeerIdentity | None,
        now: float,
        exclude_transfer_id: str | None = None,
        defer_relay_admission: bool = False,
    ) -> tuple[
        Session,
        SchedulingDecision,
        tuple[str, ...],
        str | None,
        dict[str, object],
        tuple[int, ...],
        str,
    ]:
        session = self._sessions.get(str(session_id))
        if session is None or not session.active:
            raise ValueError("unknown session")
        buffer_ids_tuple, owner_job_id = self._validate_transfer_buffers_locked(
            buffer_ids,
            job_id=job_id,
            session_id=session.session_id,
            peer_identity=peer_identity,
        )
        if buffer_ids_tuple == () and job_id is not None:
            self._validate_peer_owns_job_locked(
                job_id=str(job_id),
                peer_identity=peer_identity,
            )
        if self._topology_provider is None:
            raise ValueError(_TOPOLOGY_UNAVAILABLE_ERROR)
        topology_inventory = self._topology_provider.snapshot()
        snapshot_id = topology_snapshot_id or topology_inventory.topology_snapshot_id()
        plan_job_id = owner_job_id if owner_job_id is not None else job_id
        intent = (
            None
            if intent_id is None
            else self._transfer_intents.get(str(intent_id))
        )
        relay_eligibility = self._relay_eligibility_for_session_locked(
            session,
            inventory=topology_inventory,
        )
        planning_relays = tuple(
            item["relay_gpu"] for item in relay_eligibility["eligible_relays"]
        )
        profile_entry = self._profile_cache.get(
            self._profile_key(session.target_gpu, planning_relays)
        )
        if profile_entry is None and planning_relays != tuple(session.relay_gpus):
            profile_entry = self._profile_cache.get(
                self._profile_key(session.target_gpu, session.relay_gpus)
            )
        planning_session = (
            session
            if planning_relays == tuple(session.relay_gpus)
            else Session(
                session_id=session.session_id,
                target_gpu=session.target_gpu,
                relay_gpus=list(planning_relays),
                max_inflight_chunks=session.max_inflight_chunks,
                worker_relay_capable=session.worker_relay_capable,
                active_chunks=session.active_chunks,
                active=session.active,
                created_at=session.created_at,
                last_seen=session.last_seen,
                closed_at=session.closed_at,
            )
        )
        runtime_state = self._runtime_resource_state_locked(now=now)
        if exclude_transfer_id is not None:
            runtime_state = _runtime_state_without_transfer(
                runtime_state,
                transfer_id=str(exclude_transfer_id),
            )
        decision = self._scheduler.plan_transfer(
            session=planning_session,
            profile_entry=profile_entry,
            relay_quotas=self._relay_quotas,
            runtime_state=runtime_state,
            total_bytes=total_bytes,
            chunk_bytes=chunk_bytes,
            ranges=(
                None
                if normalized_ranges is None
                else tuple(dict(item) for item in normalized_ranges)
            ),
            mode=mode,
            direction=direction,
            workload_kind=(
                workload_kind if intent is None else intent.workload_kind
            ),
            priority=priority if intent is None else intent.priority,
            now=now,
            job_id=plan_job_id,
            intent_id=intent_id,
            topology_snapshot_id=snapshot_id,
            relay_eligibility=relay_eligibility,
            defer_relay_admission=defer_relay_admission,
        )
        return (
            session,
            decision,
            buffer_ids_tuple,
            plan_job_id,
            relay_eligibility,
            planning_relays,
            snapshot_id,
        )

    def _admission_for_decision_locked(
        self,
        decision: SchedulingDecision,
        *,
        session: Session,
        allow_delayed: bool,
        now: float,
    ) -> dict[str, object]:
        leases = scheduling_decision_leases(decision)
        if not leases:
            fallback_reason = decision.fallback_reason
            return {
                "state": _ADMISSION_ADMITTED,
                "reason": fallback_reason or "direct_or_fallback_plan",
                "decision_state": str(decision.state.value),
                "fallback_reason": fallback_reason,
                "requested_lease_count": 0,
                "requested_chunks": 0,
                "lease_ids": (),
                "admitted_at": float(now),
            }
        requested_chunks = sum(lease.chunk_limit for lease in leases)
        reason = self._relay_admission_blocked_reason_locked(
            session=session,
            leases=leases,
            now=now,
        )
        if reason is None:
            return {
                "state": _ADMISSION_ADMITTED,
                "reason": "relay_resources_available",
                "decision_state": str(decision.state.value),
                "fallback_reason": decision.fallback_reason,
                "requested_lease_count": len(leases),
                "requested_chunks": requested_chunks,
                "lease_ids": (),
                "admitted_at": float(now),
            }
        if allow_delayed:
            return {
                "state": _ADMISSION_DELAYED,
                "reason": reason,
                "decision_state": str(decision.state.value),
                "fallback_reason": decision.fallback_reason,
                "requested_lease_count": len(leases),
                "requested_chunks": requested_chunks,
                "lease_ids": (),
                "delayed_at": float(now),
            }
        return {
            "state": _ADMISSION_ADMITTED,
            "reason": "scheduler_fallback_or_rejection",
            "decision_state": str(decision.state.value),
            "fallback_reason": decision.fallback_reason,
            "requested_lease_count": 0,
            "requested_chunks": 0,
            "lease_ids": (),
            "admitted_at": float(now),
        }

    def _relay_admission_blocked_reason_locked(
        self,
        *,
        session: Session,
        leases,
        now: float,
    ) -> str | None:
        total_chunks = sum(lease.chunk_limit for lease in leases)
        if session.active_chunks + total_chunks > session.max_inflight_chunks:
            return "session relay admission is delayed by chunk quota"
        busy_relays = busy_relays_from_runtime_state(
            self._runtime_resource_state_locked(now=float(now))
        )
        for lease in leases:
            if lease.relay_device not in session.relay_gpus:
                return "relay admission is delayed by session relay ownership"
            quota = self._relay_quotas.get(lease.relay_device)
            if quota is None:
                return "relay admission is delayed by missing relay quota"
            if not quota.can_reserve(lease.chunk_limit):
                return "relay admission is delayed by relay chunk quota"
            if int(lease.relay_device) in busy_relays:
                return "relay admission is delayed by active relay path"
        return None

    def _plan_expires_at_for_decision(
        self,
        decision: SchedulingDecision,
        *,
        now: float,
    ) -> float:
        expires_at = [
            float(lease.expires_at)
            for lease in scheduling_decision_leases(decision)
            if float(lease.expires_at) > float(now)
        ]
        if expires_at:
            return min(expires_at)
        return float(now) + _DEFAULT_PLAN_TTL_SECONDS

    def _planned_transfer_payload_locked(
        self,
        *,
        transfer_id: str,
        decision: SchedulingDecision,
        status: TransferStatus,
        session: Session,
        planning_relays: tuple[int, ...],
        relay_eligibility: dict[str, object],
        reservations: list[TransferReservation],
    ) -> dict[str, object]:
        profile_entry = self._profile_cache.get(
            self._profile_key(session.target_gpu, planning_relays)
        )
        if profile_entry is None and planning_relays != tuple(session.relay_gpus):
            profile_entry = self._profile_cache.get(
                self._profile_key(session.target_gpu, session.relay_gpus)
            )
        admission = dict(self._transfer_admissions.get(str(transfer_id), {}))
        ticket = self._active_execution_ticket_for_transfer_locked(
            transfer_id=str(transfer_id),
            decision=decision,
            now=time.time(),
        )
        return daemon_receipts.planned_transfer_payload(
            transfer_id=transfer_id,
            decision=decision,
            status=status,
            session=session,
            profile_key=self._profile_key(session.target_gpu, planning_relays),
            profile_entry=profile_entry,
            relay_eligibility=relay_eligibility,
            reservations=reservations,
            admission=admission,
            plan_generation=self._transfer_plan_generations.get(str(transfer_id), 0),
            plan_expires_at=self._transfer_plan_expirations.get(str(transfer_id)),
            lease_tokens=self._lease_tokens,
            ticket=ticket,
        )

    def _active_execution_ticket_for_transfer_locked(
        self,
        *,
        transfer_id: str,
        decision: SchedulingDecision,
        now: float,
    ) -> ExecutionTicket | None:
        normalized_transfer_id = str(transfer_id)
        admission = self._transfer_admissions.get(normalized_transfer_id, {})
        if admission.get("state") != _ADMISSION_ADMITTED:
            return None
        ticket_id = self._transfer_tickets.get(normalized_transfer_id)
        ticket = None if ticket_id is None else self._execution_tickets.get(ticket_id)
        expected_generation = int(
            self._transfer_plan_generations.get(normalized_transfer_id, 0)
        )
        if ticket is not None:
            ticket_generation = int(ticket.metadata.get("plan_generation", 0) or 0)
            if ticket_generation == expected_generation and float(ticket.expires_at) > float(now):
                return ticket
        intent_id = None
        request = self._transfer_plan_requests.get(normalized_transfer_id)
        if isinstance(request, Mapping):
            intent_id = request.get("intent_id")
        intent = (
            None
            if intent_id is None
            else self._transfer_intents.get(str(intent_id))
        )
        if intent is not None:
            ticket = self._execution_ticket_for_intent_locked(
                intent=intent,
                transfer_id=normalized_transfer_id,
                decision=decision,
                now=now,
            )
        else:
            buffer_ids = self._buffer_ids_for_transfer_locked(
                normalized_transfer_id,
            )
            if len(buffer_ids) < 2:
                return ticket
            ticket = self._execution_ticket_for_plan_locked(
                transfer_id=normalized_transfer_id,
                decision=decision,
                source_buffer_id=buffer_ids[0],
                destination_buffer_id=buffer_ids[1],
                now=now,
                lease_ids=self._lease_ids_for_transfer_locked(normalized_transfer_id),
            )
        self._execution_tickets[ticket.ticket_id] = ticket
        self._transfer_tickets[normalized_transfer_id] = ticket.ticket_id
        return ticket

    def _lease_ids_for_transfer_locked(self, transfer_id: str) -> tuple[str, ...]:
        normalized_transfer_id = str(transfer_id)
        return tuple(
            reservation_id
            for reservation_id, mapped_transfer_id in sorted(
                self._reservation_transfers.items()
            )
            if mapped_transfer_id == normalized_transfer_id
        )

    def _buffer_ids_for_transfer_locked(
        self,
        transfer_id: str,
    ) -> tuple[str, ...]:
        normalized_transfer_id = str(transfer_id)
        request = self._transfer_plan_requests.get(normalized_transfer_id, {})
        request_buffer_ids = request.get("buffer_ids")
        if isinstance(request_buffer_ids, tuple):
            return tuple(str(item) for item in request_buffer_ids)
        if isinstance(request_buffer_ids, list):
            return tuple(str(item) for item in request_buffer_ids)
        transfer_record = self._transfer_queue_records.get(normalized_transfer_id, {})
        source_buffer_id = transfer_record.get("source_buffer_id")
        destination_buffer_id = transfer_record.get("destination_buffer_id")
        if source_buffer_id is not None and destination_buffer_id is not None:
            return (str(source_buffer_id), str(destination_buffer_id))
        archived = self._transfer_receipt_archive.get(normalized_transfer_id, {})
        archived_ticket = archived.get("ticket")
        if isinstance(archived_ticket, ExecutionTicket):
            return (
                str(archived_ticket.source_buffer_id),
                str(archived_ticket.destination_buffer_id),
            )
        decision = self._scheduling_decisions.get(normalized_transfer_id)
        if decision is not None:
            ticket = self._receipt_execution_ticket_for_transfer_locked(normalized_transfer_id)
            if ticket is not None:
                return (
                    str(ticket.source_buffer_id),
                    str(ticket.destination_buffer_id),
                )
        return ()

    def _validate_transfer_admission_locked(
        self,
        transfer_id: str,
        *,
        lease_id: str | None,
        now: float,
    ) -> str | None:
        normalized_transfer_id = str(transfer_id)
        admission = self._transfer_admissions.get(normalized_transfer_id)
        if admission is None:
            return "transfer admission state is unavailable"
        if admission.get("state") == _ADMISSION_DELAYED:
            return "transfer admission is delayed"
        if admission.get("state") == _ADMISSION_EXPIRED:
            return "transfer plan expired"
        if admission.get("state") == _ADMISSION_CANCELED:
            return "transfer admission is canceled"
        if admission.get("state") == _ADMISSION_FAILED:
            return "transfer admission failed"
        expires_at = self._transfer_plan_expirations.get(normalized_transfer_id)
        if expires_at is not None and float(now) > float(expires_at):
            admission["state"] = _ADMISSION_EXPIRED
            admission["expired_at"] = float(now)
            self._refresh_transfer_queue_record_locked(normalized_transfer_id, now=now)
            return "transfer plan expired"
        if lease_id is not None:
            lease_generation = self._lease_plan_generations.get(str(lease_id))
            plan_generation = self._transfer_plan_generations.get(normalized_transfer_id)
            if lease_generation is None:
                return "lease plan generation is unavailable"
            if lease_generation != plan_generation:
                return "stale execution plan"
        return None

    def _transfer_status_update_blocked_reason_locked(
        self,
        transfer_id: str,
        requested_state: TransferStatusState,
        *,
        now: float,
    ) -> str | None:
        if requested_state not in {
            TransferStatusState.RUNNING,
            TransferStatusState.COMPLETE,
        }:
            return None
        admission_error = self._validate_transfer_admission_locked(
            transfer_id,
            lease_id=None,
            now=now,
        )
        if admission_error is not None:
            return admission_error
        if not self._intent_requires_execution_evidence_locked(transfer_id):
            return None
        ticket_id = self._transfer_tickets.get(str(transfer_id))
        if ticket_id is None or ticket_id not in self._execution_tickets:
            return "intent transfer status update requires daemon-issued execution ticket"
        return None

    def _promote_delayed_transfers_locked(
        self,
        *,
        now: float,
    ) -> tuple[dict[str, object], ...]:
        promoted: list[dict[str, object]] = []
        delayed_transfer_ids = tuple(
            transfer_id
            for transfer_id, admission in sorted(self._transfer_admissions.items())
            if admission.get("state") == _ADMISSION_DELAYED
        )
        for transfer_id in delayed_transfer_ids:
            status = self._transfer_statuses.get(transfer_id)
            if status is None or status.state in _TERMINAL_TRANSFER_STATES:
                continue
            request = self._transfer_plan_requests.get(transfer_id)
            if request is None:
                continue
            try:
                (
                    session,
                    decision,
                    buffer_ids_tuple,
                    _plan_job_id,
                    _relay_eligibility,
                    _planning_relays,
                    _snapshot_id,
                ) = self._scheduler_decision_for_transfer_locked(
                    session_id=str(request["session_id"]),
                    total_bytes=int(request["total_bytes"]),
                    chunk_bytes=int(request["chunk_bytes"]),
                    mode=str(request["mode"]),
                    direction=str(request["direction"]),
                    job_id=(
                        None
                        if request.get("job_id") is None
                        else str(request["job_id"])
                    ),
                    buffer_ids=request.get("buffer_ids"),
                    normalized_ranges=request.get("ranges"),
                    intent_id=request.get("intent_id"),
                    topology_snapshot_id=request.get("topology_snapshot_id"),
                    workload_kind=str(request.get("workload_kind", "generic")),
                    priority=int(request.get("priority", 0) or 0),
                    peer_identity=None,
                    now=now,
                    exclude_transfer_id=transfer_id,
                    defer_relay_admission=True,
                )
            except ValueError as exc:
                admission = dict(self._transfer_admissions.get(transfer_id, {}))
                admission.update(
                    {
                        "state": _ADMISSION_DELAYED,
                        "reason": str(exc),
                        "promotion_failed_at": float(now),
                    }
                )
                self._transfer_admissions[transfer_id] = admission
                self._refresh_transfer_queue_record_locked(transfer_id, now=now)
                continue
            admission = self._admission_for_decision_locked(
                decision,
                session=session,
                allow_delayed=True,
                now=now,
            )
            if admission["state"] != _ADMISSION_ADMITTED:
                admission = {
                    **admission,
                    "plan_generation": self._transfer_plan_generations.get(
                        transfer_id,
                        0,
                    ),
                    "plan_expires_at": self._transfer_plan_expirations.get(transfer_id),
                    "promotion_checked_at": float(now),
                }
                self._transfer_admissions[transfer_id] = admission
                self._refresh_transfer_queue_record_locked(transfer_id, now=now)
                continue

            generation = int(self._transfer_plan_generations.get(transfer_id, 0)) + 1
            self._transfer_plan_generations[transfer_id] = generation
            self._transfer_plans[transfer_id] = dict(decision.plan)
            self._scheduling_decisions[transfer_id] = decision
            self._transfer_plan_expirations[transfer_id] = (
                self._plan_expires_at_for_decision(decision, now=now)
            )
            self._execution_tickets.pop(self._transfer_tickets.pop(transfer_id, ""), None)
            reservations = self._commit_scheduler_leases_locked(
                session,
                decision,
                transfer_id=transfer_id,
                buffer_ids=buffer_ids_tuple,
            )
            admission = {
                **admission,
                "lease_ids": tuple(
                    reservation.reservation_id for reservation in reservations
                ),
                "plan_generation": generation,
                "plan_expires_at": self._transfer_plan_expirations[transfer_id],
                "promoted_at": float(now),
            }
            self._transfer_admissions[transfer_id] = admission
            intent_id = request.get("intent_id")
            intent = (
                None
                if intent_id is None
                else self._transfer_intents.get(str(intent_id))
            )
            ticket = None
            if intent is not None:
                ticket = self._execution_ticket_for_intent_locked(
                    intent=intent,
                    transfer_id=transfer_id,
                    decision=decision,
                    now=now,
                )
                self._execution_tickets[ticket.ticket_id] = ticket
                self._transfer_tickets[transfer_id] = ticket.ticket_id
            self._append_audit_record_locked(
                event_type="admission_promoted",
                transfer_id=transfer_id,
                ticket=ticket,
                state=status.state,
                reason="daemon_resource_state_available",
                bytes_completed=status.bytes_completed,
                now=now,
            )
            self._refresh_transfer_queue_record_locked(transfer_id, now=now)
            self._touch_session_locked(session.session_id, now)
            promoted.append(
                {
                    "transfer_id": transfer_id,
                    "plan_generation": generation,
                    "lease_ids": tuple(
                        reservation.reservation_id for reservation in reservations
                    ),
                    "ticket_id": None if ticket is None else ticket.ticket_id,
                }
            )
        return tuple(promoted)

    def _current_execution_ticket_for_transfer_locked(
        self,
        transfer_id: str,
    ) -> ExecutionTicket:
        ticket_id = self._transfer_tickets.get(str(transfer_id))
        if ticket_id is None:
            raise ValueError(
                "intent transfer completion requires daemon-issued execution ticket"
            )
        ticket = self._execution_tickets.get(str(ticket_id))
        if ticket is None:
            raise ValueError(
                "intent transfer completion requires daemon-issued execution ticket"
            )
        return ticket

    def _completion_ticket_for_transfer_locked(
        self,
        transfer_id: str,
    ) -> ExecutionTicket:
        normalized = str(transfer_id)
        ticket = self._transfer_completion_tickets.get(normalized)
        if ticket is None:
            archived = self._transfer_receipt_archive.get(normalized, {})
            archived_ticket = archived.get("ticket")
            if isinstance(archived_ticket, ExecutionTicket):
                ticket = archived_ticket
        if ticket is None:
            raise ValueError(
                "intent transfer completion requires archived execution ticket"
            )
        return ticket

    def _receipt_execution_ticket_for_transfer_locked(
        self,
        transfer_id: str,
    ) -> ExecutionTicket | None:
        normalized_transfer_id = str(transfer_id)
        completion_ticket = self._transfer_completion_tickets.get(normalized_transfer_id)
        if completion_ticket is not None:
            return completion_ticket
        archived = self._transfer_receipt_archive.get(normalized_transfer_id, {})
        archived_ticket = archived.get("ticket")
        if isinstance(archived_ticket, ExecutionTicket):
            return archived_ticket
        ticket_id = self._transfer_tickets.get(normalized_transfer_id)
        return None if ticket_id is None else self._execution_tickets.get(ticket_id)

    def _completion_release_blocked_reason_locked(self, transfer_id: str) -> str | None:
        normalized_transfer_id = str(transfer_id)
        if not self._intent_requires_execution_evidence_locked(normalized_transfer_id):
            return None
        status = self._transfer_statuses.get(normalized_transfer_id)
        archived = self._transfer_receipt_archive.get(normalized_transfer_id, {})
        if status is None and isinstance(archived.get("status"), TransferStatus):
            status = archived["status"]
        if status is None:
            return "unknown transfer"
        evidence = self._transfer_completion_evidence.get(normalized_transfer_id)
        if evidence is None and isinstance(archived.get("completion_evidence"), Mapping):
            evidence = archived["completion_evidence"]
        if not isinstance(evidence, Mapping):
            return "intent transfer release requires verified completion evidence"
        completion_source = self._transfer_completion_sources.get(
            normalized_transfer_id,
            str(archived.get("completion_source", "worker")),
        )
        try:
            ticket = self._completion_ticket_for_transfer_locked(
                normalized_transfer_id
            )
            _normalize_completion_evidence(
                evidence,
                expected_bytes=status.bytes_total,
                completion_source=str(completion_source or "worker"),
                expected_ticket=ticket,
            )
        except ValueError as exc:
            return str(exc)
        return None

    def _commit_scheduler_leases_locked(
        self,
        session: Session,
        decision: SchedulingDecision,
        transfer_id: str | None = None,
        buffer_ids: tuple[str, ...] = (),
    ) -> list[TransferReservation]:
        reservations: list[TransferReservation] = []
        for lease in scheduling_decision_leases(decision):
            reservation = TransferReservation(
                reservation_id=lease.lease_id,
                session_id=lease.session_id,
                relay_gpu=lease.relay_device,
                chunks=lease.chunk_limit,
                bytes=lease.bytes_limit,
                direction=lease.direction,
            )
            self._reservations[reservation.reservation_id] = reservation
            self._lease_tokens[reservation.reservation_id] = LeaseToken(
                lease_id=reservation.reservation_id,
                session_id=reservation.session_id,
                relay_gpu=reservation.relay_gpu,
                token=str(uuid.uuid4()),
                buffer_ids=buffer_ids,
                job_id=lease.job_id,
                issued_at=lease.granted_at,
                expires_at=lease.expires_at,
            )
            if transfer_id is not None:
                self._lease_plan_generations[reservation.reservation_id] = (
                    self._transfer_plan_generations.get(str(transfer_id), 0)
                )
            session.active_chunks += reservation.chunks
            quota = self._relay_quotas.get(reservation.relay_gpu)
            if quota is not None:
                quota.active_chunks += reservation.chunks
            if transfer_id is not None:
                self._reservation_transfers[reservation.reservation_id] = transfer_id
            reservations.append(reservation)
        return reservations

    def _record_planned_transfer_locked(
        self,
        *,
        transfer_id: str,
        status: TransferStatus,
        intent_id: str | None,
        buffer_ids: tuple[str, ...],
        total_bytes: int,
        chunk_bytes: int,
        ranges: tuple[dict[str, int], ...] | None,
        direction: str,
        decision: SchedulingDecision,
        now: float,
    ) -> None:
        admission = self._transfer_admissions.get(str(transfer_id), {})
        self._transfer_queue.append(str(transfer_id))
        self._transfer_queue_records[str(transfer_id)] = {
            "transfer_id": str(transfer_id),
            "intent_id": None if intent_id is None else str(intent_id),
            "decision_id": decision.decision_id,
            "topology_snapshot_id": decision.topology_snapshot_id,
            "job_id": status.job_id,
            "session_id": status.session_id,
            "state": status.state.value,
            "direction": str(direction).lower(),
            "bytes_total": int(total_bytes),
            "bytes_completed": status.bytes_completed,
            "chunk_bytes": int(chunk_bytes),
            "ranges": tuple(dict(item) for item in ranges) if ranges is not None else (),
            "source_buffer_id": buffer_ids[0] if len(buffer_ids) >= 1 else None,
            "destination_buffer_id": buffer_ids[1] if len(buffer_ids) >= 2 else None,
            "buffer_ids": buffer_ids,
            "workload_kind": None,
            "priority": 0,
            "queued_at": float(now),
            "planned_at": decision.issued_at,
            "admission_state": admission.get("state", _ADMISSION_ADMITTED),
            "admission_reason": admission.get("reason"),
            "plan_generation": self._transfer_plan_generations.get(str(transfer_id), 0),
            "plan_expires_at": self._transfer_plan_expirations.get(str(transfer_id)),
            "started_at": None,
            "completed_at": None,
            "fallback_reason": decision.fallback_reason,
        }
        self._refresh_transfer_queue_record_locked(str(transfer_id), now=now)
        self._runtime_state_version += 1

    def _refresh_transfer_queue_record_locked(
        self,
        transfer_id: str,
        *,
        now: float | None = None,
    ) -> dict[str, object] | None:
        record = self._transfer_queue_records.get(str(transfer_id))
        status = self._transfer_statuses.get(str(transfer_id))
        if record is None or status is None:
            return record
        previous_signature = (
            str(record.get("state", "")),
            int(record.get("bytes_completed", 0) or 0),
            record.get("error"),
            record.get("completion_source"),
            record.get("completion_evidence"),
            record.get("intent_id"),
            record.get("source_buffer_id"),
            record.get("destination_buffer_id"),
            record.get("workload_kind"),
            int(record.get("priority", 0) or 0),
            record.get("admission_state"),
            int(record.get("plan_generation", 0) or 0),
            record.get("plan_expires_at"),
            record.get("started_at"),
            record.get("completed_at"),
        )
        state = status.state.value
        record["state"] = state
        record["bytes_completed"] = status.bytes_completed
        admission = self._transfer_admissions.get(str(transfer_id), {})
        if admission:
            record["admission_state"] = admission.get("state", record.get("admission_state"))
            record["admission_reason"] = admission.get("reason")
        record["plan_generation"] = self._transfer_plan_generations.get(
            str(transfer_id),
            int(record.get("plan_generation", 0) or 0),
        )
        record["plan_expires_at"] = self._transfer_plan_expirations.get(
            str(transfer_id),
            record.get("plan_expires_at"),
        )
        decision = self._scheduling_decisions.get(str(transfer_id))
        if decision is not None:
            record["decision_id"] = decision.decision_id
            record["topology_snapshot_id"] = decision.topology_snapshot_id
            record["fallback_reason"] = decision.fallback_reason
        if status.error is not None:
            record["error"] = status.error
        completion_source = self._transfer_completion_sources.get(str(transfer_id))
        if completion_source is not None:
            record["completion_source"] = completion_source
        completion_evidence = self._transfer_completion_evidence.get(str(transfer_id))
        if completion_evidence is not None:
            record["completion_evidence"] = dict(completion_evidence)
        if status.state is TransferStatusState.RUNNING and record.get("started_at") is None:
            record["started_at"] = float(time.time() if now is None else now)
        if status.state in _TERMINAL_TRANSFER_STATES and record.get("completed_at") is None:
            record["completed_at"] = float(time.time() if now is None else now)
        intent = None
        intent_id = record.get("intent_id")
        if intent_id is not None:
            intent = self._transfer_intents.get(str(intent_id))
        if intent is not None:
            record["intent_id"] = intent.intent_id
            record["source_buffer_id"] = intent.source_buffer_id
            record["destination_buffer_id"] = intent.destination_buffer_id
            record["buffer_ids"] = (intent.source_buffer_id, intent.destination_buffer_id)
            record["workload_kind"] = intent.workload_kind.value
            record["priority"] = intent.priority
        updated_signature = (
            str(record.get("state", "")),
            int(record.get("bytes_completed", 0) or 0),
            record.get("error"),
            record.get("completion_source"),
            record.get("completion_evidence"),
            record.get("intent_id"),
            record.get("source_buffer_id"),
            record.get("destination_buffer_id"),
            record.get("workload_kind"),
            int(record.get("priority", 0) or 0),
            record.get("admission_state"),
            int(record.get("plan_generation", 0) or 0),
            record.get("plan_expires_at"),
            record.get("started_at"),
            record.get("completed_at"),
        )
        if previous_signature != updated_signature:
            self._runtime_state_version += 1
        return record

    def _runtime_resource_state_locked(
        self,
        *,
        now: float | None = None,
    ) -> dict[str, object]:
        captured_at = float(time.time() if now is None else now)
        for transfer_id in tuple(self._transfer_queue):
            self._refresh_transfer_queue_record_locked(transfer_id, now=captured_at)
        transfer_records = [
            dict(self._transfer_queue_records[transfer_id])
            for transfer_id in self._transfer_queue
            if transfer_id in self._transfer_queue_records
        ]
        delayed_transfers = [
            dict(record)
            for record in transfer_records
            if str(record.get("state")) == TransferStatusState.SUBMITTED.value
            and not _record_has_admitted_execution(record)
        ]
        queued_transfers = [
            dict(record)
            for record in transfer_records
            if str(record.get("state")) == TransferStatusState.SUBMITTED.value
        ]
        running_transfers = [
            dict(record)
            for record in transfer_records
            if str(record.get("state")) == TransferStatusState.RUNNING.value
        ]
        active_transfers = [
            dict(record)
            for record in transfer_records
            if _record_has_active_execution(record)
        ]
        active_by_direction: dict[str, dict[str, int]] = {}
        queued_by_direction: dict[str, dict[str, int]] = {}
        for record in active_transfers:
            direction = str(record.get("direction", "unknown"))
            bucket = active_by_direction.setdefault(
                direction,
                {"transfer_count": 0, "bytes_total": 0, "bytes_remaining": 0},
            )
            bucket["transfer_count"] += 1
            bucket["bytes_total"] += int(record.get("bytes_total", 0) or 0)
            bucket["bytes_remaining"] += max(
                0,
                int(record.get("bytes_total", 0) or 0)
                - int(record.get("bytes_completed", 0) or 0),
            )
        for record in queued_transfers:
            direction = str(record.get("direction", "unknown"))
            bucket = queued_by_direction.setdefault(
                direction,
                {"transfer_count": 0, "bytes_total": 0},
            )
            bucket["transfer_count"] += 1
            bucket["bytes_total"] += int(record.get("bytes_total", 0) or 0)
        path_records, path_summary = self._active_path_records_locked(active_transfers)
        active_reservations = [
            self._runtime_reservation_record_locked(reservation_id, reservation)
            for reservation_id, reservation in sorted(self._reservations.items())
        ]
        active_leases = [
            self._runtime_lease_record_locked(lease_id, lease)
            for lease_id, lease in sorted(self._lease_tokens.items())
            if lease_id in self._reservations
        ]
        staging_records = [dict(value) for _, value in sorted(self._staging_records.items())]
        job_runtime_state = self._job_runtime_state_locked(transfer_records)
        relay_path_summary = {
            "path_count": 0,
            "chunk_count": 0,
            "bytes_total": 0,
        }
        completion_source_counts: dict[str, int] = {}
        terminal_completion_source_counts: dict[str, int] = {}
        terminal_execution_evidence = _terminal_execution_evidence_from_records(
            transfer_records
        )
        for key, value in path_summary.items():
            if not key.endswith(":relay"):
                continue
            relay_path_summary["path_count"] += int(value.get("path_count", 0) or 0)
            relay_path_summary["chunk_count"] += int(value.get("chunk_count", 0) or 0)
            relay_path_summary["bytes_total"] += int(value.get("bytes_total", 0) or 0)
        for record in transfer_records:
            completion_source = str(record.get("completion_source", "")).lower()
            if not completion_source:
                continue
            completion_source_counts[completion_source] = (
                completion_source_counts.get(completion_source, 0) + 1
            )
            if str(record.get("state")) in {
                TransferStatusState.COMPLETE.value,
                TransferStatusState.FAILED.value,
                TransferStatusState.CANCELED.value,
            }:
                terminal_completion_source_counts[completion_source] = (
                    terminal_completion_source_counts.get(completion_source, 0) + 1
                )
        active_resource_usage = {
            "h2d": dict(active_by_direction.get("h2d", {})),
            "d2h": dict(active_by_direction.get("d2h", {})),
            "p2p": dict(relay_path_summary),
            "relay_staging": {
                "count": len(staging_records),
                "active_reservation_count": len(active_reservations),
                "active_lease_count": len(active_leases),
            },
        }
        relay_runtime_state = {
            "active_paths": path_records,
            "active_reservations": active_reservations,
            "active_leases": active_leases,
            "relay_staging": staging_records,
        }
        busy_relays = tuple(sorted(busy_relays_from_runtime_state(relay_runtime_state)))
        relay_load = relay_load_from_runtime_state(relay_runtime_state)
        return {
            "version": self._runtime_state_version,
            "captured_at": captured_at,
            "transfer_order": tuple(self._transfer_queue),
            "transfers": transfer_records,
            "queued_transfers": queued_transfers,
            "delayed_transfers": delayed_transfers,
            "running_transfers": running_transfers,
            "active_transfers": active_transfers,
            "active_paths": path_records,
            "active_resource_usage": active_resource_usage,
            "job_runtime_state": job_runtime_state,
            "active_reservations": active_reservations,
            "active_leases": active_leases,
            "relay_staging": staging_records,
            "summary": {
                "queued_transfer_count": len(queued_transfers),
                "delayed_transfer_count": len(delayed_transfers),
                "running_transfer_count": len(running_transfers),
                "active_transfer_count": len(active_transfers),
                "terminal_transfer_count": sum(
                    1
                    for record in transfer_records
                    if str(record.get("state"))
                    in {
                        TransferStatusState.COMPLETE.value,
                        TransferStatusState.FAILED.value,
                        TransferStatusState.CANCELED.value,
                    }
                ),
                "active_reservation_count": len(active_reservations),
                "active_lease_count": len(active_leases),
                "relay_staging_count": len(staging_records),
                "relay_path_count": relay_path_summary["path_count"],
                "relay_path_bytes_total": relay_path_summary["bytes_total"],
                "busy_relays": busy_relays,
                "relay_load": relay_load,
                "completion_source_counts": completion_source_counts,
                "terminal_completion_source_counts": terminal_completion_source_counts,
                "terminal_execution_evidence": terminal_execution_evidence,
                "queued_bytes_by_direction": queued_by_direction,
                "active_bytes_by_direction": active_by_direction,
                "active_paths": path_summary,
                "active_resource_usage": active_resource_usage,
                "job_runtime_state": job_runtime_state,
            },
        }

    def _runtime_reservation_record_locked(
        self,
        reservation_id: str,
        reservation: TransferReservation,
    ) -> dict[str, object]:
        return daemon_leases.runtime_reservation_record(
            reservation_id=reservation_id,
            reservation=reservation,
            reservation_transfers=self._reservation_transfers,
            lease_tokens=self._lease_tokens,
        )

    def _runtime_lease_record_locked(
        self,
        lease_id: str,
        lease: LeaseToken,
    ) -> dict[str, object]:
        return daemon_leases.runtime_lease_record(
            lease_id=lease_id,
            lease=lease,
            reservation_transfers=self._reservation_transfers,
        )

    def _job_runtime_state_locked(
        self,
        transfer_records: list[dict[str, object]],
    ) -> dict[str, dict[str, object]]:
        jobs = {
            job_id: {
                "job_id": job_id,
                "weight": float(job.weight),
                "queued_transfer_count": 0,
                "running_transfer_count": 0,
                "active_transfer_count": 0,
                "active_bytes_total": 0,
                "active_bytes_remaining": 0,
            }
            for job_id, job in self._jobs.items()
        }
        for record in transfer_records:
            job_id = record.get("job_id")
            if job_id is None:
                continue
            normalized = str(job_id)
            job_record = jobs.setdefault(
                normalized,
                {
                    "job_id": normalized,
                    "weight": 1.0,
                    "queued_transfer_count": 0,
                    "running_transfer_count": 0,
                    "active_transfer_count": 0,
                    "active_bytes_total": 0,
                    "active_bytes_remaining": 0,
                },
            )
            state = str(record.get("state", ""))
            if state == TransferStatusState.SUBMITTED.value:
                job_record["queued_transfer_count"] = int(
                    job_record["queued_transfer_count"]
                ) + 1
            elif state == TransferStatusState.RUNNING.value:
                job_record["running_transfer_count"] = int(
                    job_record["running_transfer_count"]
                ) + 1
            if _record_has_active_execution(record):
                bytes_total = int(record.get("bytes_total", 0) or 0)
                bytes_completed = int(record.get("bytes_completed", 0) or 0)
                job_record["active_transfer_count"] = int(
                    job_record["active_transfer_count"]
                ) + 1
                job_record["active_bytes_total"] = int(
                    job_record["active_bytes_total"]
                ) + bytes_total
                job_record["active_bytes_remaining"] = int(
                    job_record["active_bytes_remaining"]
                ) + max(0, bytes_total - bytes_completed)
        return dict(sorted(jobs.items()))

    def _active_path_records_locked(
        self,
        active_transfers: list[dict[str, object]],
    ) -> tuple[list[dict[str, object]], dict[str, dict[str, int]]]:
        transfer_ids = {str(record["transfer_id"]) for record in active_transfers}
        records: list[dict[str, object]] = []
        summary: dict[str, dict[str, int]] = {}
        for transfer_id in sorted(transfer_ids):
            admission = self._transfer_admissions.get(transfer_id, {})
            if admission.get("state") != _ADMISSION_ADMITTED:
                continue
            decision = self._scheduling_decisions.get(transfer_id)
            if decision is None:
                continue
            for assignment in decision.plan.get("assignments", ()) or ():
                if not isinstance(assignment, Mapping):
                    continue
                path = assignment.get("path")
                if not isinstance(path, Mapping):
                    continue
                chunks = assignment.get("chunks", ()) or ()
                chunk_count = len(chunks) if isinstance(chunks, list | tuple) else 0
                bytes_total = int(assignment.get("bytes", 0) or 0)
                if not bytes_total and isinstance(chunks, list | tuple):
                    bytes_total = sum(
                        int(chunk.get("bytes", 0) or 0)
                        for chunk in chunks
                        if isinstance(chunk, Mapping)
                    )
                kind = str(path.get("kind", "unknown"))
                direction = str(path.get("direction", "unknown"))
                key = f"{direction}:{kind}"
                bucket = summary.setdefault(
                    key,
                    {"path_count": 0, "chunk_count": 0, "bytes_total": 0},
                )
                bucket["path_count"] += 1
                bucket["chunk_count"] += chunk_count
                bucket["bytes_total"] += bytes_total
                records.append(
                    {
                        "transfer_id": transfer_id,
                        "kind": kind,
                        "direction": direction,
                        "target_device": path.get("target_device"),
                        "relay_device": path.get("relay_device"),
                        "bytes_total": bytes_total,
                        "chunk_count": chunk_count,
                    }
                )
        return records, summary

    def _execution_ticket_for_worker_locked(
        self,
        authorization: WorkerTransferAuthorization,
        *,
        leases: tuple[LeaseToken, ...],
        transfer_id: str,
        now: float,
    ) -> ExecutionTicket:
        decision = self._scheduling_decisions.get(str(transfer_id))
        if decision is None:
            raise ValueError("scheduling decision is unavailable")
        if not leases:
            raise ValueError("worker ticket requires at least one lease")
        lease_ids = tuple(lease.lease_id for lease in leases)
        expires_at = min(
            float(lease.expires_at or (float(now) + 30.0))
            for lease in leases
        )
        if expires_at <= float(now):
            raise ValueError("lease expired")
        return self._execution_ticket_for_plan_locked(
            transfer_id=transfer_id,
            decision=decision,
            source_buffer_id=authorization.src_buffer.buffer_id,
            destination_buffer_id=authorization.dst_buffer.buffer_id,
            now=now,
            expires_at=expires_at,
            lease_ids=lease_ids,
        )

    def _leases_for_worker_plan_locked(
        self,
        request: WorkerTransferAuthorizationRequest,
        *,
        primary_lease: LeaseToken,
    ) -> tuple[LeaseToken, ...]:
        if request.ranges:
            return (primary_lease,)
        plan = self._transfer_plans.get(request.transfer_id)
        if plan is None:
            return (primary_lease,)
        relay_devices = _relay_devices_from_plan(plan, direction=request.direction)
        if not relay_devices:
            return (primary_lease,)
        if primary_lease.relay_gpu not in relay_devices:
            return (primary_lease,)
        related: list[LeaseToken] = []
        for lease_id, mapped_transfer_id in sorted(self._reservation_transfers.items()):
            if mapped_transfer_id != request.transfer_id:
                continue
            lease = self._lease_tokens.get(lease_id)
            if lease is None:
                continue
            if lease.relay_gpu in relay_devices:
                related.append(lease)
        if primary_lease.lease_id not in {lease.lease_id for lease in related}:
            related.append(primary_lease)
        found_relays = {lease.relay_gpu for lease in related}
        if found_relays != relay_devices:
            raise ValueError("worker relay lease set mismatch")
        return tuple(
            sorted(
                related,
                key=lambda item: (int(item.relay_gpu), str(item.lease_id)),
            )
        )

    def _register_worker_staging_records_locked(
        self,
        *,
        leases: tuple[LeaseToken, ...],
        transfer_id: str,
        direction: str,
        plan: dict[str, object],
        now: float,
    ) -> dict[str, dict[str, object]]:
        records: dict[str, dict[str, object]] = {}
        for lease in leases:
            ranges = _relay_ranges_from_plan(
                plan,
                relay_gpu=lease.relay_gpu,
                direction=direction,
            )
            requested_bytes = sum(item["bytes"] for item in ranges)
            records[lease.lease_id] = self._register_staging_record_locked(
                lease=lease,
                transfer_id=transfer_id,
                direction=direction,
                ranges=ranges,
                requested_bytes=requested_bytes,
                now=now,
            )
        return records

    def _execution_ticket_for_intent_locked(
        self,
        *,
        intent: TransferIntent,
        transfer_id: str,
        decision: SchedulingDecision,
        now: float,
    ) -> ExecutionTicket:
        lease_ids = tuple(
            reservation_id
            for reservation_id, mapped_transfer_id in sorted(
                self._reservation_transfers.items()
            )
            if mapped_transfer_id == transfer_id
        )
        lease_expirations = [
            float(self._lease_tokens[lease_id].expires_at)
            for lease_id in lease_ids
            if lease_id in self._lease_tokens
            and float(self._lease_tokens[lease_id].expires_at) > float(now)
        ]
        expires_at = (
            min(lease_expirations)
            if lease_expirations
            else self._transfer_plan_expirations.get(
                str(transfer_id),
                float(now) + _DEFAULT_PLAN_TTL_SECONDS,
            )
        )
        return self._execution_ticket_for_plan_locked(
            transfer_id=transfer_id,
            decision=decision,
            source_buffer_id=intent.source_buffer_id,
            destination_buffer_id=intent.destination_buffer_id,
            now=now,
            expires_at=expires_at,
            lease_ids=lease_ids,
        )

    def _execution_ticket_for_plan_locked(
        self,
        *,
        transfer_id: str,
        decision: SchedulingDecision,
        source_buffer_id: str,
        destination_buffer_id: str,
        now: float,
        expires_at: float | None = None,
        lease_ids: tuple[str, ...] = (),
    ) -> ExecutionTicket:
        return daemon_receipts.execution_ticket_for_plan(
            transfer_id=transfer_id,
            decision=decision,
            source_buffer_id=source_buffer_id,
            destination_buffer_id=destination_buffer_id,
            now=now,
            plan_generation=self._transfer_plan_generations.get(str(transfer_id), 0),
            default_expires_at=self._transfer_plan_expirations.get(
                str(transfer_id),
                float(now) + _DEFAULT_PLAN_TTL_SECONDS,
            ),
            expires_at=expires_at,
            lease_ids=lease_ids,
        )

    def _receipt_for_intent_locked(self, intent_id: str) -> TransferReceipt:
        normalized_intent_id = str(intent_id)
        transfer_id = self._intent_transfers.get(normalized_intent_id)
        if transfer_id is None:
            transfer_id = self._archived_intent_transfers.get(normalized_intent_id)
        if transfer_id is None:
            raise ValueError("unknown transfer intent")
        archived = self._transfer_receipt_archive.get(str(transfer_id), {})
        intent = self._transfer_intents.get(normalized_intent_id)
        if intent is None and isinstance(archived.get("intent"), TransferIntent):
            intent = archived["intent"]
        status = self._transfer_statuses.get(transfer_id)
        if status is None and isinstance(archived.get("status"), TransferStatus):
            status = archived["status"]
        decision = self._scheduling_decisions.get(transfer_id)
        if decision is None and isinstance(archived.get("decision"), SchedulingDecision):
            decision = archived["decision"]
        if intent is None:
            raise ValueError("transfer intent is unavailable")
        if status is None:
            raise ValueError("transfer status is unavailable")
        if decision is None:
            raise ValueError("scheduling decision is unavailable")
        ticket = self._receipt_execution_ticket_for_transfer_locked(transfer_id)
        admission = dict(self._transfer_admissions.get(transfer_id, {}))
        if not admission and isinstance(archived.get("admission"), Mapping):
            admission = dict(archived["admission"])
        completion_source = self._transfer_completion_sources.get(transfer_id)
        if completion_source is None and archived.get("completion_source") is not None:
            completion_source = str(archived["completion_source"])
        completion_evidence = self._transfer_completion_evidence.get(transfer_id)
        if (
            completion_evidence is None
            and isinstance(archived.get("completion_evidence"), Mapping)
        ):
            completion_evidence = dict(archived["completion_evidence"])
        return daemon_receipts.receipt_for_transfer(
            transfer_id=transfer_id,
            intent=intent,
            status=status,
            decision=decision,
            ticket=ticket,
            admission=admission,
            plan_generation=self._transfer_plan_generations.get(transfer_id, 0),
            plan_expires_at=self._transfer_plan_expirations.get(transfer_id),
            admitted_state=_ADMISSION_ADMITTED,
            completion_source=completion_source,
            completion_evidence=completion_evidence,
        )

    def _intent_requires_execution_evidence_locked(self, transfer_id: str) -> bool:
        request = self._transfer_plan_requests.get(str(transfer_id), {})
        intent_id = request.get("intent_id")
        if intent_id is None:
            archived = self._transfer_receipt_archive.get(str(transfer_id), {})
            intent_id = archived.get("intent_id")
        return intent_id is not None and (
            str(intent_id) in self._transfer_intents
            or str(intent_id) in self._archived_intent_transfers
        )

    def _topology_snapshot_id_locked(self) -> str:
        if self._topology_provider is None:
            return "topology-unavailable"
        inventory = self._topology_provider.snapshot()
        return inventory.topology_snapshot_id()

    def _issue_lease_token_locked(
        self,
        lease_id: str,
        session_id: str,
        relay_gpu: int,
        now: float,
        job_id: str | None = None,
        expires_at: float = 0.0,
    ) -> LeaseToken:
        lease_token = LeaseToken(
            lease_id=lease_id,
            session_id=session_id,
            relay_gpu=relay_gpu,
            token=str(uuid.uuid4()),
            job_id=job_id,
            issued_at=float(now),
            expires_at=float(expires_at),
        )
        self._lease_tokens[lease_token.lease_id] = lease_token
        return lease_token

    def _active_buffer_lease_ids_locked(self, buffer_id: str) -> tuple[str, ...]:
        return daemon_leases.active_buffer_lease_ids(
            lease_tokens=self._lease_tokens,
            reservations=self._reservations,
            buffer_id=buffer_id,
        )

    def _register_staging_record_locked(
        self,
        *,
        lease: LeaseToken,
        transfer_id: str,
        direction: str,
        ranges: tuple[dict[str, int], ...],
        requested_bytes: int,
        now: float,
    ) -> dict[str, object]:
        record = {
            "staging_record_id": f"staging-{lease.lease_id}",
            "lease_id": lease.lease_id,
            "transfer_id": str(transfer_id),
            "session_id": lease.session_id,
            "job_id": lease.job_id,
            "relay_gpu": lease.relay_gpu,
            "buffer_ids": lease.buffer_ids,
            "direction": str(direction).lower(),
            "ranges": tuple(dict(item) for item in ranges),
            "requested_bytes": int(requested_bytes),
            "state": "authorized",
            "created_at": float(now),
        }
        self._staging_records[lease.lease_id] = record
        return record

    def _append_transfer_audit_records_locked(
        self,
        *,
        event_type: str,
        transfer_id: str,
        state: TransferStatusState | str,
        reason: str | None = None,
        failure_reason: str | None = None,
        bytes_completed: int | None = None,
    ) -> None:
        reservations = [
            self._reservations[reservation_id]
            for reservation_id, mapped_transfer_id in sorted(
                self._reservation_transfers.items()
            )
            if mapped_transfer_id == str(transfer_id)
            and reservation_id in self._reservations
        ]
        if reservations:
            for reservation in reservations:
                lease = self._lease_tokens.get(reservation.reservation_id)
                self._append_audit_record_locked(
                    event_type=event_type,
                    transfer_id=str(transfer_id),
                    reservation=reservation,
                    lease=lease,
                    staging_record=self._staging_records.get(reservation.reservation_id),
                    state=state,
                    reason=reason,
                    failure_reason=failure_reason,
                    bytes_completed=bytes_completed,
                )
            return
        self._append_audit_record_locked(
            event_type=event_type,
            transfer_id=str(transfer_id),
            state=state,
            reason=reason,
            failure_reason=failure_reason,
            bytes_completed=bytes_completed,
        )

    def _append_audit_record_locked(
        self,
        *,
        event_type: str,
        transfer_id: str | None = None,
        reservation: TransferReservation | None = None,
        lease: LeaseToken | None = None,
        staging_record: dict[str, object] | None = None,
        ticket: ExecutionTicket | None = None,
        state: TransferStatusState | str | None = None,
        reason: str | None = None,
        failure_reason: str | None = None,
        cleanup_kind: str | None = None,
        cleanup_target_id: str | None = None,
        session_id: str | None = None,
        bytes_completed: int | None = None,
        now: float | None = None,
    ) -> dict[str, object]:
        created_at = float(time.time() if now is None else now)
        normalized_transfer_id = None if transfer_id is None else str(transfer_id)
        if normalized_transfer_id is None and staging_record is not None:
            value = staging_record.get("transfer_id")
            normalized_transfer_id = None if value is None else str(value)
        status = (
            None
            if normalized_transfer_id is None
            else self._transfer_statuses.get(normalized_transfer_id)
        )
        decision = (
            None
            if normalized_transfer_id is None
            else self._scheduling_decisions.get(normalized_transfer_id)
        )
        ticket_id = None
        if ticket is not None:
            ticket_id = ticket.ticket_id
        elif normalized_transfer_id is not None:
            ticket_id = self._transfer_tickets.get(normalized_transfer_id)
        active_ticket = None if ticket_id is None else self._execution_tickets.get(ticket_id)
        if ticket is None:
            ticket = active_ticket
        lease_id = None
        if lease is not None:
            lease_id = lease.lease_id
        elif reservation is not None:
            lease_id = reservation.reservation_id
        elif staging_record is not None:
            value = staging_record.get("lease_id")
            lease_id = None if value is None else str(value)
        if lease is None and lease_id is not None:
            lease = self._lease_tokens.get(lease_id)
        resolved_session_id = session_id
        if resolved_session_id is None and status is not None:
            resolved_session_id = status.session_id
        if resolved_session_id is None and lease is not None:
            resolved_session_id = lease.session_id
        if resolved_session_id is None and reservation is not None:
            resolved_session_id = reservation.session_id
        if resolved_session_id is None and staging_record is not None:
            value = staging_record.get("session_id")
            resolved_session_id = None if value is None else str(value)
        job_id = None
        if status is not None:
            job_id = status.job_id
        elif lease is not None:
            job_id = lease.job_id
        elif staging_record is not None:
            value = staging_record.get("job_id")
            job_id = None if value is None else str(value)
        elif decision is not None:
            job_id = decision.job_id
        job = None if job_id is None else self._jobs.get(job_id)
        buffer_ids: tuple[str, ...] = ()
        if lease is not None:
            buffer_ids = tuple(lease.buffer_ids)
        elif staging_record is not None:
            buffer_ids = tuple(str(item) for item in staging_record.get("buffer_ids", ()))
        elif ticket is not None:
            buffer_ids = (ticket.source_buffer_id, ticket.destination_buffer_id)
        relay_gpu = None
        if reservation is not None:
            relay_gpu = reservation.relay_gpu
        elif lease is not None:
            relay_gpu = lease.relay_gpu
        elif staging_record is not None and staging_record.get("relay_gpu") is not None:
            relay_gpu = int(staging_record["relay_gpu"])
        direction = None
        if reservation is not None:
            direction = reservation.direction
        elif staging_record is not None:
            value = staging_record.get("direction")
            direction = None if value is None else str(value)
        elif ticket is not None:
            direction = ticket.direction
        bytes_total = 0
        if reservation is not None:
            bytes_total = int(reservation.bytes)
        elif staging_record is not None:
            bytes_total = int(staging_record.get("requested_bytes", 0) or 0)
        elif status is not None:
            bytes_total = int(status.bytes_total)
        completed = (
            int(bytes_completed)
            if bytes_completed is not None
            else (int(status.bytes_completed) if status is not None else 0)
        )
        if reservation is not None and bytes_total:
            completed = min(completed, bytes_total)
        started_at = None
        if staging_record is not None:
            started_at = float(staging_record.get("created_at", 0.0) or 0.0)
        elif decision is not None:
            started_at = float(decision.issued_at)
        duration_seconds = None
        if started_at:
            duration_seconds = max(0.0, created_at - started_at)
        record = {
            "audit_id": f"audit-{len(self._audit_records) + 1}",
            "event_type": str(event_type),
            "created_at": created_at,
            "transfer_id": normalized_transfer_id,
            "decision_id": None if decision is None else decision.decision_id,
            "ticket_id": ticket_id,
            "topology_snapshot_id": (
                None if decision is None else decision.topology_snapshot_id
            ),
            "lease_id": lease_id,
            "session_id": None if resolved_session_id is None else str(resolved_session_id),
            "job_id": job_id,
            "user_id": None if job is None else job.user_id,
            "process_id": None if job is None else job.process_id,
            "container_id": None if job is None else job.container_id,
            "relay_gpu": relay_gpu,
            "direction": direction,
            "bytes_total": bytes_total,
            "bytes_completed": completed,
            "duration_seconds": duration_seconds,
            "state": (
                state.value
                if isinstance(state, TransferStatusState)
                else (None if state is None else str(state))
            ),
            "reason": reason,
            "failure_reason": failure_reason,
            "cleanup_kind": cleanup_kind,
            "cleanup_target_id": cleanup_target_id,
            "source_buffer_id": None if ticket is None else ticket.source_buffer_id,
            "destination_buffer_id": (
                None if ticket is None else ticket.destination_buffer_id
            ),
            "buffer_ids": buffer_ids,
            "staging_record_id": (
                None
                if staging_record is None
                else staging_record.get("staging_record_id")
            ),
        }
        self._audit_records.append(record)
        return record

    def _cleanup_job_locked(self, job_id: str, reason: str) -> dict[str, int]:
        removed = _empty_removed_summary()
        normalized_job_id = str(job_id)
        job_peer = self._job_peer_identities.get(normalized_job_id)
        transfer_ids = self._transfer_ids_for_job_locked(normalized_job_id)
        self._archive_cleanup_target_locked(
            target_kind="job",
            target_id=normalized_job_id,
            peer_identity=job_peer,
            reason=reason,
            transfer_ids=transfer_ids,
        )
        job = self._jobs.pop(normalized_job_id, None)
        if job is not None:
            removed["jobs"] += 1
        self._job_peer_identities.pop(normalized_job_id, None)
        for reservation_id, lease in list(self._lease_tokens.items()):
            if lease.job_id == normalized_job_id:
                _merge_removed(
                    removed,
                    self._release_reservation_and_count_locked(
                        reservation_id,
                        final_state=TransferStatusState.CANCELED,
                        cleanup_reason=reason,
                    ),
                )
        for transfer_id in transfer_ids:
            status = self._transfer_statuses.get(transfer_id)
            if status is not None and status.state not in _TERMINAL_TRANSFER_STATES:
                self._mark_transfer_terminal_locked(
                    transfer_id,
                    TransferStatusState.CANCELED,
                    error=reason,
                )
                removed["transfers"] += 1
            self._retire_transfer_runtime_state_locked(transfer_id)
        for buffer_id, buffer in list(self._buffers.items()):
            if buffer.job_id == normalized_job_id:
                self._buffers.pop(buffer_id, None)
                removed["buffers"] += 1
        return removed

    def _transfer_ids_for_job_locked(self, job_id: str) -> tuple[str, ...]:
        transfer_ids = {
            transfer_id
            for transfer_id, status in self._transfer_statuses.items()
            if status.job_id == str(job_id)
        }
        for intent_id, intent in self._transfer_intents.items():
            if intent.job_id == str(job_id):
                transfer_id = self._intent_transfers.get(intent_id)
                if transfer_id is not None:
                    transfer_ids.add(transfer_id)
        for reservation_id, lease in self._lease_tokens.items():
            if lease.job_id == str(job_id):
                transfer_id = self._reservation_transfers.get(reservation_id)
                if transfer_id is not None:
                    transfer_ids.add(transfer_id)
        return tuple(sorted(transfer_ids))

    def _transfer_ids_for_session_locked(self, session_id: str) -> tuple[str, ...]:
        transfer_ids = {
            transfer_id
            for transfer_id, status in self._transfer_statuses.items()
            if status.session_id == str(session_id)
        }
        for intent_id, intent in self._transfer_intents.items():
            if intent.session_id == str(session_id):
                transfer_id = self._intent_transfers.get(intent_id)
                if transfer_id is not None:
                    transfer_ids.add(transfer_id)
        return tuple(sorted(transfer_ids))

    def _transfer_ids_for_buffer_locked(self, buffer_id: str) -> tuple[str, ...]:
        normalized = str(buffer_id)
        transfer_ids = set()
        for intent_id, intent in self._transfer_intents.items():
            if normalized in {intent.source_buffer_id, intent.destination_buffer_id}:
                transfer_id = self._intent_transfers.get(intent_id)
                if transfer_id is not None:
                    transfer_ids.add(transfer_id)
        for reservation_id, lease in self._lease_tokens.items():
            if normalized in lease.buffer_ids:
                transfer_id = self._reservation_transfers.get(reservation_id)
                if transfer_id is not None:
                    transfer_ids.add(transfer_id)
        return tuple(sorted(transfer_ids))

    def _transfer_peer_identity_for_owner_locked(
        self,
        *,
        job_id: str,
        session_id: str,
        peer_identity: PeerIdentity | None,
    ) -> PeerIdentity | None:
        if peer_identity is not None and peer_identity.authenticated:
            return peer_identity
        job_peer = self._job_peer_identities.get(str(job_id))
        if job_peer is not None and job_peer.authenticated:
            return job_peer
        session_peer = self._session_peer_identities.get(str(session_id))
        if session_peer is not None and session_peer.authenticated:
            return session_peer
        return None

    def _validate_peer_owns_receipt_transfer_locked(
        self,
        *,
        transfer_id: str | None,
        job_id: str,
        peer_identity: PeerIdentity | None,
    ) -> None:
        if peer_identity is None or not peer_identity.authenticated:
            return
        if transfer_id is None:
            raise ValueError("unknown transfer")
        job_key = str(job_id)
        if job_key in self._jobs:
            self._validate_peer_owns_job_locked(
                job_id=job_key,
                peer_identity=peer_identity,
            )
            return
        transfer_peer = self._transfer_peer_identities.get(str(transfer_id))
        if transfer_peer is None:
            archived = self._transfer_receipt_archive.get(str(transfer_id), {})
            archived_peer = archived.get("peer_identity")
            if isinstance(archived_peer, PeerIdentity):
                transfer_peer = archived_peer
        if transfer_peer is None or not transfer_peer.authenticated:
            raise ValueError("transfer owner identity is unavailable")
        peer_auth.validate_peer_owner_match(
            expected=transfer_peer,
            actual=peer_identity,
            owner_name="transfer",
        )

    def _validate_peer_owns_job_locked(
        self,
        *,
        job_id: str,
        peer_identity: PeerIdentity | None,
    ) -> None:
        if peer_identity is None or not peer_identity.authenticated:
            return
        job_key = str(job_id)
        job = self._jobs.get(job_key)
        if job is None:
            raise ValueError("unknown job")
        job_peer = self._job_peer_identities.get(job_key)
        if job_peer is None:
            raise ValueError("job owner identity is unavailable")
        peer_auth.validate_peer_owner_match(
            expected=job_peer,
            actual=peer_identity,
            owner_name="job",
        )

    def _validate_peer_owns_session_locked(
        self,
        *,
        session_id: str,
        peer_identity: PeerIdentity | None,
    ) -> None:
        if peer_identity is None or not peer_identity.authenticated:
            return
        session_key = str(session_id)
        if session_key not in self._sessions:
            raise ValueError("unknown session")
        session_peer = self._session_peer_identities.get(session_key)
        if session_peer is None:
            raise ValueError("session owner identity is unavailable")
        peer_auth.validate_peer_owner_match(
            expected=session_peer,
            actual=peer_identity,
            owner_name="session",
        )

    def _validate_peer_owns_buffer_locked(
        self,
        *,
        buffer_id: str,
        peer_identity: PeerIdentity | None,
    ) -> None:
        if peer_identity is None or not peer_identity.authenticated:
            return
        buffer = self._buffers.get(str(buffer_id))
        if buffer is None:
            raise ValueError("unknown buffer")
        try:
            self._validate_peer_owns_job_locked(
                job_id=buffer.job_id,
                peer_identity=peer_identity,
            )
        except ValueError as exc:
            if str(exc) == "job owner does not match authenticated peer":
                raise ValueError("buffer owner does not match authenticated peer") from exc
            raise

    def _validate_peer_owns_lease_locked(
        self,
        *,
        lease_id: str,
        peer_identity: PeerIdentity | None,
    ) -> None:
        if peer_identity is None or not peer_identity.authenticated:
            return
        lease = self._lease_tokens.get(str(lease_id))
        if lease is None:
            raise ValueError("unknown lease")
        if lease.job_id is not None:
            self._validate_peer_owns_job_locked(
                job_id=lease.job_id,
                peer_identity=peer_identity,
            )
        for buffer_id in lease.buffer_ids:
            self._validate_peer_owns_buffer_locked(
                buffer_id=buffer_id,
                peer_identity=peer_identity,
            )

    def _validate_peer_owns_staging_record_locked(
        self,
        *,
        staging_record: Mapping[str, object],
        peer_identity: PeerIdentity | None,
    ) -> None:
        if peer_identity is None or not peer_identity.authenticated:
            return
        job_id = staging_record.get("job_id")
        if job_id is not None:
            self._validate_peer_owns_job_locked(
                job_id=str(job_id),
                peer_identity=peer_identity,
            )
        for buffer_id in staging_record.get("buffer_ids", ()) or ():
            self._validate_peer_owns_buffer_locked(
                buffer_id=str(buffer_id),
                peer_identity=peer_identity,
            )

    def _validate_peer_owns_missing_cleanup_target_locked(
        self,
        *,
        target_kind: str,
        target_id: str,
        peer_identity: PeerIdentity | None,
    ) -> None:
        if peer_identity is None or not peer_identity.authenticated:
            return
        transfer_ids = self._residual_transfer_ids_for_cleanup_target_locked(
            target_kind=target_kind,
            target_id=target_id,
        )
        if not transfer_ids:
            archived_target = self._retired_cleanup_target_record_locked(
                target_kind=target_kind,
                target_id=target_id,
            )
            if archived_target is None:
                raise ValueError(f"unknown {target_kind}")
            archived_peer = archived_target.get("peer_identity")
            if isinstance(archived_peer, PeerIdentity) and archived_peer.authenticated:
                peer_auth.validate_peer_owner_match(
                    expected=archived_peer,
                    actual=peer_identity,
                    owner_name=target_kind,
                )
                return
            raise ValueError(f"{target_kind} owner identity is unavailable")
        for transfer_id in transfer_ids:
            status = self._transfer_statuses.get(transfer_id)
            if status is not None:
                self._validate_peer_owns_receipt_transfer_locked(
                    transfer_id=transfer_id,
                    job_id=status.job_id,
                    peer_identity=peer_identity,
                )
                continue
            transfer_peer = self._transfer_peer_identities.get(transfer_id)
            if transfer_peer is None or not transfer_peer.authenticated:
                raise ValueError("transfer owner identity is unavailable")
            peer_auth.validate_peer_owner_match(
                expected=transfer_peer,
                actual=peer_identity,
                owner_name="transfer",
            )

    def _residual_transfer_ids_for_cleanup_target_locked(
        self,
        *,
        target_kind: str,
        target_id: str,
    ) -> tuple[str, ...]:
        normalized = str(target_id)
        if target_kind == "job":
            return self._transfer_ids_for_job_locked(normalized)
        if target_kind == "buffer":
            transfer_ids = set(self._transfer_ids_for_buffer_locked(normalized))
            for lease_id in self._active_buffer_lease_ids_locked(normalized):
                transfer_id = self._reservation_transfers.get(lease_id)
                if transfer_id is not None:
                    transfer_ids.add(transfer_id)
            return tuple(sorted(transfer_ids))
        if target_kind == "session":
            return self._transfer_ids_for_session_locked(normalized)
        return ()

    def _validate_transfer_buffers_locked(
        self,
        buffer_ids: Iterable[str] | None,
        job_id: str | None,
        session_id: str,
        peer_identity: PeerIdentity | None = None,
    ) -> tuple[tuple[str, ...], str | None]:
        if buffer_ids is None:
            return (), None
        normalized = tuple(str(buffer_id) for buffer_id in buffer_ids)
        if not normalized:
            return (), None
        if any(not buffer_id.strip() for buffer_id in normalized):
            raise ValueError("buffer_ids must be non-empty")
        if len(set(normalized)) != len(normalized):
            raise ValueError("buffer_ids must be unique")
        owner_job_id = None if job_id is None else str(job_id)
        for buffer_id in normalized:
            buffer = self._buffers.get(buffer_id)
            if buffer is None:
                raise ValueError(f"unknown buffer: {buffer_id}")
            if owner_job_id is None:
                owner_job_id = buffer.job_id
            if buffer.job_id != str(owner_job_id):
                raise ValueError("buffer owner does not match job")
            self._validate_peer_owns_buffer_locked(
                buffer_id=buffer_id,
                peer_identity=peer_identity,
            )
        if owner_job_id is None:
            raise ValueError("job_id is required when buffer_ids are provided")
        job = self._jobs.get(str(owner_job_id))
        if job is None:
            raise ValueError("unknown job")
        if job.session_id != session_id:
            raise ValueError("job session does not match transfer session")
        self._validate_peer_owns_job_locked(
            job_id=str(owner_job_id),
            peer_identity=peer_identity,
        )
        return normalized, str(owner_job_id)

    def _mark_transfer_terminal_if_unblocked_locked(
        self,
        transfer_id: str,
        final_state: TransferStatusState,
        error: str | None = None,
    ) -> None:
        if any(value == transfer_id for value in self._reservation_transfers.values()):
            return
        status = self._transfer_statuses.get(transfer_id)
        if status is None or status.state in {
            TransferStatusState.COMPLETE,
            TransferStatusState.FAILED,
            TransferStatusState.CANCELED,
        }:
            return
        completed = status.bytes_total if final_state is TransferStatusState.COMPLETE else status.bytes_completed
        self._transfer_statuses[transfer_id] = TransferStatus(
            transfer_id=status.transfer_id,
            job_id=status.job_id,
            state=final_state,
            bytes_total=status.bytes_total,
            bytes_completed=completed,
            session_id=status.session_id,
            error=status.error if error is None else error,
        )
        self._mark_transfer_admission_terminal_locked(
            transfer_id,
            final_state,
            reason=error,
        )
        if final_state is not TransferStatusState.COMPLETE:
            self._drop_execution_ticket_for_transfer_locked(transfer_id)
        self._refresh_transfer_queue_record_locked(transfer_id)

    def _mark_transfer_terminal_locked(
        self,
        transfer_id: str,
        final_state: TransferStatusState,
        error: str | None = None,
    ) -> TransferStatus:
        status = self._transfer_statuses.get(str(transfer_id))
        if status is None:
            raise ValueError("unknown transfer")
        if status.state in _TERMINAL_TRANSFER_STATES:
            return status
        completed = (
            status.bytes_total
            if final_state is TransferStatusState.COMPLETE
            else status.bytes_completed
        )
        terminal = TransferStatus(
            transfer_id=status.transfer_id,
            job_id=status.job_id,
            state=final_state,
            bytes_total=status.bytes_total,
            bytes_completed=completed,
            session_id=status.session_id,
            error=status.error if error is None else error,
        )
        self._transfer_statuses[terminal.transfer_id] = terminal
        self._mark_transfer_admission_terminal_locked(
            terminal.transfer_id,
            final_state,
            reason=error,
        )
        if final_state is not TransferStatusState.COMPLETE:
            self._drop_execution_ticket_for_transfer_locked(terminal.transfer_id)
        self._refresh_transfer_queue_record_locked(terminal.transfer_id)
        return terminal

    def _drop_execution_ticket_for_transfer_locked(self, transfer_id: str) -> bool:
        ticket_id = self._transfer_tickets.pop(str(transfer_id), None)
        if ticket_id is not None:
            self._execution_tickets.pop(ticket_id, None)
            return True
        return False

    def _transfer_has_reservations_locked(self, transfer_id: str) -> bool:
        normalized = str(transfer_id)
        return any(
            mapped_transfer_id == normalized
            for mapped_transfer_id in self._reservation_transfers.values()
        )

    def _archive_cleanup_target_locked(
        self,
        *,
        target_kind: str,
        target_id: str,
        peer_identity: PeerIdentity | None,
        reason: str | None = None,
        transfer_ids: tuple[str, ...] = (),
    ) -> None:
        normalized_kind = str(target_kind)
        normalized_id = str(target_id)
        self._retired_cleanup_targets[(normalized_kind, normalized_id)] = {
            "target_kind": normalized_kind,
            "target_id": normalized_id,
            "peer_identity": peer_identity,
            "reason": None if reason is None else str(reason),
            "retired_at": time.time(),
            "transfer_ids": tuple(str(item) for item in transfer_ids),
        }

    def _retired_cleanup_target_record_locked(
        self,
        *,
        target_kind: str,
        target_id: str,
    ) -> dict[str, object] | None:
        return self._retired_cleanup_targets.get((str(target_kind), str(target_id)))

    def _retire_transfer_runtime_state_locked(
        self,
        transfer_id: str,
    ) -> None:
        normalized = str(transfer_id)
        self._archive_transfer_receipt_state_locked(normalized)
        removed = False
        removed = self._drop_execution_ticket_for_transfer_locked(normalized) or removed
        removed = self._pop_transfer_runtime_maps_locked(normalized) or removed
        removed = self._remove_transfer_intent_state_locked(normalized) or removed
        if self._transfer_admissions.pop(normalized, None) is not None:
            removed = True
        if normalized in self._transfer_queue_records:
            self._transfer_queue_records.pop(normalized, None)
            removed = True
        if normalized in self._transfer_queue:
            self._transfer_queue = [
                queued_id
                for queued_id in self._transfer_queue
                if queued_id != normalized
            ]
            removed = True
        if self._transfer_statuses.pop(normalized, None) is not None:
            removed = True
        if removed:
            self._runtime_state_version += 1

    def _archive_transfer_receipt_state_locked(self, transfer_id: str) -> None:
        normalized = str(transfer_id)
        existing = dict(self._transfer_receipt_archive.get(normalized, {}))
        request = self._transfer_plan_requests.get(normalized, {})
        intent_id = request.get("intent_id")
        if intent_id is None:
            intent_id = existing.get("intent_id")
        current_intent = (
            self._transfer_intents.get(str(intent_id))
            if intent_id is not None
            else None
        )
        if current_intent is None:
            archived_intent = existing.get("intent")
            if isinstance(archived_intent, TransferIntent):
                current_intent = archived_intent
        status = self._transfer_statuses.get(normalized)
        if status is None:
            archived_status = existing.get("status")
            if isinstance(archived_status, TransferStatus):
                status = archived_status
        decision = self._scheduling_decisions.get(normalized)
        if decision is None:
            archived_decision = existing.get("decision")
            if isinstance(archived_decision, SchedulingDecision):
                decision = archived_decision
        if status is None or decision is None or current_intent is None:
            if existing:
                self._transfer_receipt_archive[normalized] = existing
            return
        archived_record = {
            "transfer_id": normalized,
            "intent_id": str(intent_id) if intent_id is not None else None,
            "intent": current_intent,
            "status": status,
            "decision": decision,
            "ticket": self._receipt_execution_ticket_for_transfer_locked(normalized),
            "admission": dict(self._transfer_admissions.get(normalized, {})),
            "plan_generation": self._transfer_plan_generations.get(normalized, 0),
            "plan_expires_at": self._transfer_plan_expirations.get(normalized),
            "completion_source": self._transfer_completion_sources.get(normalized),
            "completion_evidence": dict(
                self._transfer_completion_evidence.get(normalized, {})
            ),
            "peer_identity": self._transfer_peer_identities.get(normalized),
        }
        if archived_record["intent_id"] is not None:
            self._archived_intent_transfers[str(archived_record["intent_id"])] = normalized
        updated = dict(existing)
        updated.update(archived_record)
        if updated != existing:
            self._runtime_state_version += 1
        self._transfer_receipt_archive[normalized] = updated

    def _retire_completed_transfer_lease_state_locked(
        self,
        transfer_id: str,
        *,
        reason: str | None = None,
    ) -> None:
        normalized = str(transfer_id)
        removed = self._drop_execution_ticket_for_transfer_locked(normalized)
        admission = self._transfer_admissions.get(normalized)
        if admission is not None:
            updated = dict(admission)
            updated["lease_ids"] = ()
            updated["retired_at"] = time.time()
            if reason is not None:
                updated["retired_reason"] = str(reason)
            if updated != admission:
                self._transfer_admissions[normalized] = updated
                removed = True
        if normalized in self._transfer_queue_records:
            self._transfer_queue_records.pop(normalized, None)
            removed = True
        if normalized in self._transfer_queue:
            self._transfer_queue = [
                queued_id
                for queued_id in self._transfer_queue
                if queued_id != normalized
            ]
            removed = True
        if removed:
            self._runtime_state_version += 1

    def _pop_transfer_runtime_maps_locked(self, transfer_id: str) -> bool:
        normalized = str(transfer_id)
        removed = False
        for mapping in (
            self._transfer_completion_tickets,
            self._transfer_completion_sources,
            self._transfer_completion_evidence,
            self._transfer_plan_requests,
            self._transfer_plan_generations,
            self._transfer_plan_expirations,
            self._transfer_plans,
            self._scheduling_decisions,
            self._transfer_peer_identities,
        ):
            if mapping.pop(normalized, None) is not None:
                removed = True
        return removed

    def _remove_transfer_intent_state_locked(self, transfer_id: str) -> bool:
        normalized = str(transfer_id)
        intent_ids = [
            intent_id
            for intent_id, mapped_transfer_id in self._intent_transfers.items()
            if mapped_transfer_id == normalized
        ]
        for intent_id in intent_ids:
            self._intent_transfers.pop(intent_id, None)
            self._transfer_intents.pop(intent_id, None)
        return bool(intent_ids)

    def _mark_transfer_admission_terminal_locked(
        self,
        transfer_id: str,
        final_state: TransferStatusState,
        *,
        reason: str | None = None,
    ) -> None:
        if final_state is TransferStatusState.CANCELED:
            admission_state = _ADMISSION_CANCELED
        elif final_state is TransferStatusState.FAILED:
            admission_state = _ADMISSION_FAILED
        else:
            return
        normalized_transfer_id = str(transfer_id)
        admission = self._transfer_admissions.get(normalized_transfer_id)
        if admission is None:
            return
        updated = dict(admission)
        updated["state"] = admission_state
        updated["terminal_at"] = time.time()
        if reason is not None:
            updated["terminal_reason"] = str(reason)
        self._transfer_admissions[normalized_transfer_id] = updated

    def _close_session_locked(
        self,
        session_id: str,
        reason: str = "session_closed",
        removed: dict[str, object] | None = None,
    ) -> Session | None:
        session = self._sessions.pop(session_id, None)
        if session is None:
            return None
        session_peer = self._session_peer_identities.get(session_id)
        transfer_ids = self._transfer_ids_for_session_locked(session_id)
        self._archive_cleanup_target_locked(
            target_kind="session",
            target_id=session_id,
            peer_identity=session_peer,
            reason=reason,
            transfer_ids=transfer_ids,
        )
        session.active = False
        session.closed_at = time.time()
        self._append_audit_record_locked(
            event_type="cleanup",
            session_id=session_id,
            state=TransferStatusState.CANCELED,
            reason=reason,
            failure_reason=reason,
            cleanup_kind="session",
            cleanup_target_id=session_id,
        )
        self._system_cleanup_events.append(
            CleanupRequest(
                target_kind="session",
                target_id=session_id,
                reason=reason,
                force=True,
            )
        )
        transfer_ids = self._transfer_ids_for_session_locked(session_id)
        for reservation_id, reservation in list(self._reservations.items()):
            if reservation.session_id == session_id:
                _merge_removed(
                    removed,
                    self._release_reservation_and_count_locked(
                        reservation_id,
                        final_state=TransferStatusState.CANCELED,
                        cleanup_reason=reason,
                    ),
                )
        for transfer_id in transfer_ids:
            status = self._transfer_statuses.get(transfer_id)
            if status is not None and status.state not in _TERMINAL_TRANSFER_STATES:
                self._mark_transfer_terminal_locked(
                    transfer_id,
                    TransferStatusState.CANCELED,
                    error=reason,
                )
                if removed is not None:
                    removed["transfers"] = int(removed["transfers"]) + 1
            self._retire_transfer_runtime_state_locked(transfer_id)
        for gpu in session.relay_gpus:
            quota = self._relay_quotas.get(gpu)
            if quota is not None:
                quota.sessions.discard(session_id)
        self._connection_scoped_sessions.discard(session_id)
        self._connection_scoped_session_connections.pop(session_id, None)
        removed_jobs = self._remove_session_jobs_and_buffers_locked(session_id)
        if removed is not None:
            removed["sessions"] = int(removed["sessions"]) + 1
            removed["jobs"] = int(removed["jobs"]) + removed_jobs["jobs"]
            removed["buffers"] = int(removed["buffers"]) + removed_jobs["buffers"]
        self._runtime_state_version += 1
        return session

    def _remove_session_jobs_and_buffers_locked(self, session_id: str) -> dict[str, int]:
        job_ids = {
            job_id
            for job_id, job in self._jobs.items()
            if job.session_id == session_id
        }
        removed = {"jobs": 0, "buffers": 0}
        for job_id in job_ids:
            if self._jobs.pop(job_id, None) is not None:
                removed["jobs"] += 1
            self._job_peer_identities.pop(job_id, None)
        for buffer_id, buffer in list(self._buffers.items()):
            if buffer.job_id in job_ids:
                self._buffers.pop(buffer_id, None)
                removed["buffers"] += 1
        self._session_peer_identities.pop(session_id, None)
        return removed

    def _cleanup_connection_scoped_sessions_locked(
        self,
        peer_identity: PeerIdentity | None,
        connection_id: str | None = None,
        reason: str = "socket_disconnect",
    ) -> dict[str, int]:
        removed = _empty_removed_summary()
        if peer_identity is None:
            return removed
        for session_id in sorted(tuple(self._connection_scoped_sessions)):
            if connection_id is not None:
                session_connection_id = self._connection_scoped_session_connections.get(
                    session_id
                )
                if session_connection_id != str(connection_id):
                    continue
            session_peer = self._session_peer_identities.get(session_id)
            if not peer_auth.peer_identity_same_connection(session_peer, peer_identity):
                continue
            self._close_session_locked(session_id, reason=reason, removed=removed)
        return removed

    def _touch_session_locked(self, session_id: str, now: float | None = None) -> None:
        session = self._sessions.get(session_id)
        if session is None:
            return
        session.last_seen = time.time() if now is None else float(now)

    def _reap_stale_sessions_locked(self, now: float) -> list[str]:
        if self._session_timeout_seconds <= 0.0:
            return []
        expired = [
            session_id
            for session_id, session in self._sessions.items()
            if session.active and session.last_seen > 0.0 and now - session.last_seen > self._session_timeout_seconds
        ]
        for session_id in expired:
            self._close_session_locked(session_id, reason="stale_session_timeout")
        return expired

    def _reap_expired_leases_locked(self, now: float) -> list[str]:
        expired = [
            lease_id
            for lease_id, lease in self._lease_tokens.items()
            if lease.expires_at and float(now) > lease.expires_at
        ]
        for lease_id in expired:
            self._release_expired_lease_locked(lease_id)
        return expired

    def _release_expired_lease_locked(self, lease_id: str) -> TransferReservation | None:
        reservation = self._release_reservation_locked(
            lease_id,
            final_state=TransferStatusState.CANCELED,
            cleanup_reason="lease_expired",
        )
        if reservation is None:
            self._lease_tokens.pop(lease_id, None)
        return reservation

    def _purge_stale_profiles_locked(self, now: float) -> list[str]:
        return daemon_profiles.purge_stale_profiles(
            self._profile_cache,
            max_age_seconds=self._profile_max_age_seconds,
            now=now,
        )

    def describe(
        self,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        with self._lock:
            now = time.time()
            self._reap_stale_sessions_locked(now)
            self._reap_expired_leases_locked(now)
            self._purge_stale_profiles_locked(now)
            return DaemonResponse(
                ok=True,
                payload={
                    "jobs": {key: asdict(value) for key, value in self._jobs.items()},
                    "job_peer_identities": {
                        key: asdict(value)
                        for key, value in self._job_peer_identities.items()
                    },
                    "buffers": {key: asdict(value) for key, value in self._buffers.items()},
                    "sessions": {key: asdict(value) for key, value in self._sessions.items()},
                    "session_peer_identities": {
                        key: asdict(value)
                        for key, value in self._session_peer_identities.items()
                    },
                    "reservations": {
                        key: asdict(value) for key, value in self._reservations.items()
                    },
                    "staging_records": {
                        key: dict(value) for key, value in self._staging_records.items()
                    },
                    "audit_records": [dict(record) for record in self._audit_records],
                    "connection_scoped_sessions": sorted(
                        self._connection_scoped_sessions
                    ),
                    "transfer_statuses": {
                        key: asdict(value) for key, value in self._transfer_statuses.items()
                    },
                    "transfer_queue": [
                        dict(self._transfer_queue_records[transfer_id])
                        for transfer_id in self._transfer_queue
                        if transfer_id in self._transfer_queue_records
                    ],
                    "runtime_resource_state": self._runtime_resource_state_locked(
                        now=now,
                    ),
                    "cleanup_events": [asdict(item) for item in self._cleanup_events],
                    "system_cleanup_events": [
                        asdict(item) for item in self._system_cleanup_events
                    ],
                    "relay_quotas": {
                        key: {
                            "relay_gpu": quota.relay_gpu,
                            "max_sessions": quota.max_sessions,
                            "max_inflight_chunks": quota.max_inflight_chunks,
                            "active_chunks": quota.active_chunks,
                            "sessions": sorted(quota.sessions),
                        }
                        for key, quota in self._relay_quotas.items()
                    },
                    "profile_cache": {
                        key: dict(value) for key, value in self._profile_cache.items()
                    },
                    "require_authenticated_peers": self._require_authenticated_peers,
                    "requester_peer_identity": (
                        None if peer_identity is None else asdict(peer_identity)
                    ),
                },
            )

    def handle_request(
        self,
        request: DaemonRequest,
        connection_id: str | None = None,
    ) -> DaemonResponse:
        if self._requires_authenticated_peer_for_request(request):
            return peer_auth.authenticated_peer_required_response(request.peer_identity)
        try:
            return self._handle_request(request, connection_id=connection_id)
        except (KeyError, TypeError, ValueError) as exc:
            return DaemonResponse(ok=False, error=f"invalid request: {exc}")

    def _requires_authenticated_peer_for_request(self, request: DaemonRequest) -> bool:
        if not self._require_authenticated_peers:
            return False
        peer_identity = request.peer_identity
        return peer_identity is None or not peer_identity.authenticated

    def _handle_request(
        self,
        request: DaemonRequest,
        connection_id: str | None = None,
    ) -> DaemonResponse:
        return daemon_dispatch.handle_request(
            self,
            request,
            connection_id=connection_id,
        )

    def _eligible_relays_for_session_locked(self, session: Session) -> tuple[int, ...]:
        relay_eligibility = self._relay_eligibility_for_session_locked(session)
        return tuple(item["relay_gpu"] for item in relay_eligibility["eligible_relays"])

    def _relays_for_new_session_locked(self, target_gpu: int) -> list[int]:
        if self._topology_provider is None:
            return sorted(self._relay_quotas)
        if not self._relay_quotas:
            return []
        inventory = self._topology_provider.snapshot()
        relay_eligibility = self._relay_eligibility_for_target_locked(
            target_gpu=int(target_gpu),
            requested_relays=sorted(self._relay_quotas),
            inventory=inventory,
        )
        return [int(item["relay_gpu"]) for item in relay_eligibility["eligible_relays"]]

    def _relay_eligibility_for_session_locked(
        self,
        session: Session,
        inventory=None,
    ) -> dict[str, object]:
        if not bool(session.worker_relay_capable):
            requested_relays = tuple(int(gpu) for gpu in session.relay_gpus)
            inventory_record = (
                None
                if inventory is None
                else {
                    "topology_snapshot_id": inventory.topology_snapshot_id(),
                    "topology_version": inventory.version,
                    "inventory_source": inventory.source,
                    "inventory_discovered_at": inventory.discovered_at,
                }
            )
            return {
                "requested_relays": requested_relays,
                "eligible_relays": [],
                "filtered_relays": [
                    {
                        "relay_gpu": int(relay_gpu),
                        "reason": "session is not worker relay capable",
                    }
                    for relay_gpu in requested_relays
                ],
                **({} if inventory_record is None else inventory_record),
            }
        return self._relay_eligibility_for_target_locked(
            target_gpu=session.target_gpu,
            requested_relays=session.relay_gpus,
            inventory=inventory,
        )

    def _relay_eligibility_for_target_locked(
        self,
        target_gpu: int,
        requested_relays: Iterable[int],
        inventory=None,
    ) -> dict[str, object]:
        if inventory is None:
            inventory = self._topology_provider.snapshot()
        relay_eligibility = inventory.relay_eligibility(
            target_device=int(target_gpu),
            requested_relays=requested_relays,
        )
        eligible_relays = []
        filtered_relays = list(relay_eligibility["filtered_relays"])
        for item in relay_eligibility["eligible_relays"]:
            relay_gpu = int(item["relay_gpu"])
            if relay_gpu in self._relay_quotas:
                eligible_relays.append({"relay_gpu": relay_gpu, "reason": "eligible"})
            else:
                filtered_relays.append(
                    {"relay_gpu": relay_gpu, "reason": "relay not configured"}
                )
        return {
            **relay_eligibility,
            "topology_snapshot_id": inventory.topology_snapshot_id(),
            "topology_version": inventory.version,
            "eligible_relays": eligible_relays,
            "filtered_relays": filtered_relays,
        }

    def _relay_discovery_snapshot_locked(
        self,
        *,
        inventory,
        target_gpu: int | None,
        requested_relays: Iterable[int],
    ) -> dict[str, object]:
        candidates = tuple(self._normalize_relays(requested_relays))
        if target_gpu is None:
            relay_eligibility = {
                "requested_relays": list(candidates),
                "eligible_relays": [],
                "filtered_relays": [],
                "inventory_source": inventory.source,
                "inventory_discovered_at": inventory.discovered_at,
            }
            eligibility_by_relay = {
                relay_gpu: {
                    "eligible": None,
                    "reason": "target_gpu not provided",
                }
                for relay_gpu in candidates
            }
        else:
            relay_eligibility = self._relay_eligibility_for_target_locked(
                target_gpu=target_gpu,
                requested_relays=candidates,
                inventory=inventory,
            )
            eligibility_by_relay = {}
            for item in relay_eligibility["eligible_relays"]:
                eligibility_by_relay[int(item["relay_gpu"])] = {
                    "eligible": True,
                    "reason": str(item.get("reason", "eligible")),
                }
            for item in relay_eligibility["filtered_relays"]:
                eligibility_by_relay[int(item["relay_gpu"])] = {
                    "eligible": False,
                    "reason": str(item.get("reason", "filtered")),
                }

        relay_records = [
            self._relay_discovery_record_locked(
                relay_gpu=relay_gpu,
                inventory=inventory,
                target_gpu=target_gpu,
                eligibility=eligibility_by_relay.get(
                    relay_gpu,
                    {"eligible": False, "reason": "not requested"},
                ),
            )
            for relay_gpu in candidates
        ]
        return {
            "topology_snapshot_id": inventory.topology_snapshot_id(),
            "topology_version": inventory.version,
            "target_gpu": target_gpu,
            "requested_relays": list(candidates),
            "inventory_source": inventory.source,
            "inventory_discovered_at": inventory.discovered_at,
            "relay_eligibility": relay_eligibility,
            "relays": relay_records,
            "summary": {
                "relay_count": len(relay_records),
                "configured_relay_count": sum(
                    1 for item in relay_records if item["configured"]
                ),
                "eligible_relay_count": sum(
                    1
                    for item in relay_records
                    if item["eligibility"]["eligible"] is True
                ),
                "active_session_count": sum(
                    int(item["quota"]["active_sessions"])
                    for item in relay_records
                    if item["quota"] is not None
                ),
                "active_reservation_count": sum(
                    len(item["reservations"]) for item in relay_records
                ),
                "active_lease_count": sum(len(item["leases"]) for item in relay_records),
            },
        }

    def _relay_discovery_record_locked(
        self,
        *,
        relay_gpu: int,
        inventory,
        target_gpu: int | None,
        eligibility: dict[str, object],
    ) -> dict[str, object]:
        quota = self._relay_quotas.get(relay_gpu)
        return {
            "relay_gpu": relay_gpu,
            "configured": quota is not None,
            "eligibility": {
                "target_gpu": target_gpu,
                "eligible": eligibility["eligible"],
                "reason": eligibility["reason"],
            },
            "inventory": self._relay_inventory_record(inventory, relay_gpu, target_gpu),
            "quota": daemon_leases.relay_quota_record(quota),
            "sessions": self._relay_session_records_locked(relay_gpu),
            "reservations": self._relay_reservation_records_locked(relay_gpu),
            "leases": self._relay_lease_records_locked(relay_gpu),
        }

    def _relay_inventory_record(
        self,
        inventory,
        relay_gpu: int,
        target_gpu: int | None,
    ) -> dict[str, object]:
        relay = int(relay_gpu)
        target = None if target_gpu is None else int(target_gpu)
        fabric_links = []
        for link in inventory.fabric_links:
            touches_relay = link.src_device_id == relay or link.dst_device_id == relay
            touches_target = (
                target is None
                or (link.src_device_id == relay and link.dst_device_id == target)
                or (
                    link.bidirectional
                    and link.src_device_id == target
                    and link.dst_device_id == relay
                )
            )
            if touches_relay and touches_target:
                fabric_links.append(asdict(link))
        return {
            "gpus": [
                asdict(gpu) for gpu in inventory.gpus if gpu.device_id == relay
            ],
            "pcie_paths": [
                asdict(path)
                for path in inventory.pcie_paths
                if path.device_id == relay
            ],
            "fabric_links": fabric_links,
            "path_capabilities": _relay_path_capabilities(
                inventory,
                relay_gpu=relay,
                target_gpu=target,
                fabric_links=fabric_links,
            ),
        }

    def _relay_session_records_locked(self, relay_gpu: int) -> list[dict[str, object]]:
        return daemon_leases.relay_session_records(
            relay_gpu=relay_gpu,
            quota=self._relay_quotas.get(relay_gpu),
            sessions=self._sessions,
            jobs=self._jobs,
        )

    def _relay_reservation_records_locked(
        self,
        relay_gpu: int,
    ) -> list[dict[str, object]]:
        return daemon_leases.relay_reservation_records(
            relay_gpu=relay_gpu,
            reservations=self._reservations,
            lease_tokens=self._lease_tokens,
            reservation_transfers=self._reservation_transfers,
        )

    def _relay_lease_records_locked(self, relay_gpu: int) -> list[dict[str, object]]:
        return daemon_leases.relay_lease_records(
            relay_gpu=relay_gpu,
            lease_tokens=self._lease_tokens,
            reservations=self._reservations,
            reservation_transfers=self._reservation_transfers,
        )

    def handle_wire_message(
        self,
        data: bytes | str,
        peer_identity: PeerIdentity | None = None,
        connection_id: str | None = None,
    ) -> DaemonResponse:
        try:
            text = data.decode("utf-8") if isinstance(data, bytes) else str(data)
            request_data = json.loads(text)
            if not isinstance(request_data, dict):
                raise ValueError("request must be a JSON object")
            payload = request_data.get("payload", {})
            if not isinstance(payload, dict):
                raise ValueError("payload must be a JSON object")
            request = DaemonRequest(
                request_type=RequestType(request_data["request_type"]),
                session_id=request_data.get("session_id"),
                payload=payload,
                peer_identity=peer_identity,
            )
        except Exception as exc:
            return DaemonResponse(ok=False, error=f"invalid request: {exc}")
        return self.handle_request(request, connection_id=connection_id)

    @staticmethod
    def _profile_key(target_gpu: int, relay_gpus: Iterable[int]) -> str:
        return daemon_profiles.profile_key(target_gpu, relay_gpus)

    @staticmethod
    def _normalize_relays(relay_gpus: Iterable[int]) -> list[int]:
        return sorted({int(gpu) for gpu in relay_gpus})

    def serve_forever(
        self,
        socket_path: str,
        *,
        stop_event: threading.Event | None = None,
        max_requests: int | None = None,
    ) -> None:
        peer_auth.validate_unix_socket_support(
            require_authenticated_peers=self._require_authenticated_peers
        )
        if max_requests is not None:
            max_requests = int(max_requests)
            if max_requests <= 0:
                raise ValueError("max_requests must be positive")
        unlink_stale_socket(socket_path)

        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(socket_path)
        secure_unix_socket(socket_path)
        server.listen()
        server.settimeout(0.1)

        try:
            request_count = 0
            while True:
                if stop_event is not None and stop_event.is_set():
                    break
                if max_requests is not None and request_count >= max_requests:
                    break
                try:
                    conn, _ = server.accept()
                except socket.timeout:
                    continue
                with conn:
                    peer_identity = peer_auth.peer_identity_from_socket(conn)
                    connection_id = str(uuid.uuid4())
                    data = b""
                    try:
                        while True:
                            chunk = conn.recv(65536)
                            if not chunk:
                                break
                            data += chunk
                            while b"\n" in data:
                                line, _, data = data.partition(b"\n")
                                if not line:
                                    continue
                                response = self.handle_wire_message(
                                    line,
                                    peer_identity=peer_identity,
                                    connection_id=connection_id,
                                )
                                conn.sendall(
                                    (json.dumps(asdict(response)) + "\n").encode("utf-8")
                                )
                                request_count += 1
                    finally:
                        with self._lock:
                            self._cleanup_connection_scoped_sessions_locked(
                                peer_identity,
                                connection_id=connection_id,
                                reason="socket_disconnect",
                            )
        finally:
            server.close()
            if os.path.exists(socket_path):
                os.unlink(socket_path)


def socket_path_for_user(base_dir: str = "/tmp") -> str:
    return f"{base_dir.rstrip('/')}/turbobusd.sock"


def reserve_socket(path: str) -> socket.socket:
    peer_auth.validate_unix_socket_support(require_authenticated_peers=False)
    unlink_stale_socket(path)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind(path)
    secure_unix_socket(path)
    sock.listen()
    return sock


def _topology_unavailable_response() -> DaemonResponse:
    return DaemonResponse(
        ok=False,
        error=_TOPOLOGY_UNAVAILABLE_ERROR,
    )


def _relay_path_capabilities(
    inventory,
    *,
    relay_gpu: int,
    target_gpu: int | None,
    fabric_links: list[dict[str, object]],
) -> dict[str, object]:
    pcie_paths = [
        path for path in inventory.pcie_paths if path.device_id == int(relay_gpu)
    ]
    pcie_path = pcie_paths[0] if pcie_paths else None
    enabled_fabric_links = [
        link for link in fabric_links if bool(link.get("enabled", False))
    ]
    fabric_bandwidths = [
        float(link.get("bandwidth_gbps", 0.0) or 0.0)
        for link in enabled_fabric_links
    ]
    return {
        "relay_gpu": int(relay_gpu),
        "target_gpu": target_gpu,
        "has_pcie_path": pcie_path is not None,
        "pcie_root_complex": None if pcie_path is None else pcie_path.root_complex,
        "pcie_numa_node": None if pcie_path is None else pcie_path.numa_node,
        "pcie_link_generation": (
            None if pcie_path is None else pcie_path.link_generation
        ),
        "pcie_link_width": None if pcie_path is None else pcie_path.link_width,
        "pcie_negotiated_speed_gtps": (
            None if pcie_path is None else pcie_path.negotiated_speed_gtps
        ),
        "pcie_bandwidth_gbps": (
            0.0 if pcie_path is None else pcie_path.bandwidth_gbps
        ),
        "pcie_bandwidth_source": (
            None if pcie_path is None else pcie_path.bandwidth_source
        ),
        "pcie_switch_hierarchy": (
            [] if pcie_path is None else list(pcie_path.switch_hierarchy)
        ),
        "fabric_link_count": len(fabric_links),
        "enabled_fabric_link_count": len(enabled_fabric_links),
        "fabric_kinds": sorted(
            {str(link.get("fabric")) for link in enabled_fabric_links}
        ),
        "fabric_capabilities": sorted(
            {
                str(link.get("capability"))
                for link in enabled_fabric_links
                if link.get("capability") is not None
            }
        ),
        "fabric_bandwidth_gbps": sum(fabric_bandwidths),
        "p2p_enabled": bool(enabled_fabric_links),
    }


def _relay_ranges_from_plan(
    plan: dict[str, object],
    *,
    relay_gpu: int | Iterable[int],
    direction: str,
) -> tuple[dict[str, int], ...]:
    if not isinstance(plan, dict):
        raise ValueError("transfer plan is unavailable")
    ranges: list[dict[str, int]] = []
    if isinstance(relay_gpu, int):
        relays = {int(relay_gpu)}
    else:
        relays = {int(gpu) for gpu in relay_gpu}
    if not relays:
        raise ValueError("daemon plan has no authorized relay chunks")
    requested_direction = str(direction).lower()
    for assignment in plan.get("assignments", ()) or ():
        if not isinstance(assignment, dict):
            raise ValueError("transfer plan assignment must be an object")
        path = assignment.get("path")
        if not isinstance(path, dict):
            raise ValueError("transfer plan assignment path must be an object")
        if str(path.get("kind", "")).lower() != "relay":
            continue
        if str(path.get("direction", "")).lower() != requested_direction:
            continue
        if int(path.get("relay_device", -1)) not in relays:
            continue
        for chunk in assignment.get("chunks", ()) or ():
            if not isinstance(chunk, dict):
                raise ValueError("transfer plan chunk must be an object")
            ranges.append(
                {
                    "src_offset": int(chunk["src_offset"]),
                    "dst_offset": int(chunk["dst_offset"]),
                    "bytes": int(chunk["bytes"]),
                }
            )
    if not ranges:
        raise ValueError("daemon plan has no authorized relay chunks")
    return tuple(ranges)


def _relay_devices_from_plan(
    plan: dict[str, object],
    *,
    direction: str,
) -> set[int]:
    if not isinstance(plan, dict):
        raise ValueError("transfer plan is unavailable")
    relays: set[int] = set()
    requested_direction = str(direction).lower()
    for assignment in plan.get("assignments", ()) or ():
        if not isinstance(assignment, dict):
            raise ValueError("transfer plan assignment must be an object")
        path = assignment.get("path")
        if not isinstance(path, dict):
            raise ValueError("transfer plan assignment path must be an object")
        if str(path.get("kind", "")).lower() != "relay":
            continue
        if str(path.get("direction", "")).lower() != requested_direction:
            continue
        if assignment.get("chunks"):
            relays.add(int(path.get("relay_device", -1)))
    return relays


def _decision_is_direct_only(decision: SchedulingDecision) -> bool:
    assignments = decision.plan.get("assignments", ()) or ()
    if not assignments:
        return False
    for assignment in assignments:
        if not isinstance(assignment, dict):
            return False
        path = assignment.get("path")
        if not isinstance(path, dict):
            return False
        if str(path.get("kind", "")).lower() != "direct":
            return False
    return True


def _runtime_state_without_transfer(
    runtime_state: dict[str, object],
    *,
    transfer_id: str,
) -> dict[str, object]:
    normalized = str(transfer_id)
    filtered = dict(runtime_state)
    for key in (
        "transfers",
        "queued_transfers",
        "delayed_transfers",
        "running_transfers",
        "active_transfers",
        "active_paths",
        "active_reservations",
        "active_leases",
        "relay_staging",
    ):
        value = filtered.get(key)
        if isinstance(value, list | tuple):
            filtered[key] = [
                dict(item)
                for item in value
                if isinstance(item, Mapping)
                and str(item.get("transfer_id")) != normalized
            ]
    order = filtered.get("transfer_order")
    if isinstance(order, list | tuple):
        filtered["transfer_order"] = tuple(
            str(item) for item in order if str(item) != normalized
        )
    _refresh_runtime_feedback_summary(filtered)
    job_runtime_state = filtered.get("job_runtime_state")
    transfers = filtered.get("transfers", ())
    if isinstance(job_runtime_state, Mapping) and isinstance(transfers, list | tuple):
        filtered["job_runtime_state"] = _job_runtime_state_from_records(
            job_runtime_state,
            transfers,
        )
        if isinstance(filtered.get("summary"), Mapping):
            filtered["summary"] = {
                **dict(filtered["summary"]),
                "job_runtime_state": filtered["job_runtime_state"],
            }
    return filtered


def _refresh_runtime_feedback_summary(runtime_state: dict[str, object]) -> None:
    summary = runtime_state.get("summary")
    if not isinstance(summary, Mapping):
        return
    summary_copy = dict(summary)
    path_summary: dict[str, dict[str, int]] = {}
    relay_path_summary = {"path_count": 0, "chunk_count": 0, "bytes_total": 0}
    completion_source_counts: dict[str, int] = {}
    terminal_completion_source_counts: dict[str, int] = {}
    terminal_execution_evidence = _terminal_execution_evidence_from_records(
        runtime_state.get("transfers", ())
    )
    active_by_direction = _transfer_bytes_by_direction(
        runtime_state.get("active_transfers", ()),
        include_remaining=True,
    )
    queued_by_direction = _transfer_bytes_by_direction(
        runtime_state.get("queued_transfers", ()),
        include_remaining=False,
    )
    for record in _runtime_mapping_records(runtime_state.get("active_paths", ())):
        kind = str(record.get("kind", "unknown"))
        direction = str(record.get("direction", "unknown"))
        key = f"{direction}:{kind}"
        bucket = path_summary.setdefault(
            key,
            {"path_count": 0, "chunk_count": 0, "bytes_total": 0},
        )
        bucket["path_count"] += 1
        bucket["chunk_count"] += int(record.get("chunk_count", 0) or 0)
        bucket["bytes_total"] += int(record.get("bytes_total", 0) or 0)
        if kind == "relay":
            relay_path_summary["path_count"] += 1
            relay_path_summary["chunk_count"] += int(record.get("chunk_count", 0) or 0)
            relay_path_summary["bytes_total"] += int(record.get("bytes_total", 0) or 0)
    for record in _runtime_mapping_records(runtime_state.get("transfers", ())):
        completion_source = str(record.get("completion_source", "")).lower()
        if not completion_source:
            continue
        completion_source_counts[completion_source] = (
            completion_source_counts.get(completion_source, 0) + 1
        )
        if str(record.get("state")) in {
            TransferStatusState.COMPLETE.value,
            TransferStatusState.FAILED.value,
            TransferStatusState.CANCELED.value,
        }:
            terminal_completion_source_counts[completion_source] = (
                terminal_completion_source_counts.get(completion_source, 0) + 1
            )

    active_resource_usage = dict(summary_copy.get("active_resource_usage", {}) or {})
    active_resource_usage["h2d"] = dict(active_by_direction.get("h2d", {}))
    active_resource_usage["d2h"] = dict(active_by_direction.get("d2h", {}))
    active_resource_usage["p2p"] = dict(relay_path_summary)
    relay_staging = dict(active_resource_usage.get("relay_staging", {}) or {})
    relay_staging.update(
        {
            "count": len(runtime_state.get("relay_staging", ()) or ()),
            "active_reservation_count": len(
                runtime_state.get("active_reservations", ()) or ()
            ),
            "active_lease_count": len(runtime_state.get("active_leases", ()) or ()),
        }
    )
    active_resource_usage["relay_staging"] = relay_staging

    summary_copy.update(
        {
            "queued_transfer_count": len(runtime_state.get("queued_transfers", ()) or ()),
            "delayed_transfer_count": len(runtime_state.get("delayed_transfers", ()) or ()),
            "running_transfer_count": len(runtime_state.get("running_transfers", ()) or ()),
            "active_transfer_count": len(runtime_state.get("active_transfers", ()) or ()),
            "active_reservation_count": len(runtime_state.get("active_reservations", ()) or ()),
            "active_lease_count": len(runtime_state.get("active_leases", ()) or ()),
            "relay_staging_count": len(runtime_state.get("relay_staging", ()) or ()),
            "relay_path_count": relay_path_summary["path_count"],
            "relay_path_bytes_total": relay_path_summary["bytes_total"],
            "busy_relays": tuple(sorted(busy_relays_from_runtime_state(runtime_state))),
            "relay_load": relay_load_from_runtime_state(runtime_state),
            "queued_bytes_by_direction": queued_by_direction,
            "active_bytes_by_direction": active_by_direction,
            "active_paths": path_summary,
            "active_resource_usage": active_resource_usage,
            "completion_source_counts": completion_source_counts,
            "terminal_completion_source_counts": terminal_completion_source_counts,
            "terminal_execution_evidence": terminal_execution_evidence,
        }
    )
    runtime_state["active_resource_usage"] = active_resource_usage
    runtime_state["summary"] = summary_copy


def _terminal_execution_evidence_from_records(
    records: object,
) -> dict[str, int]:
    result = {
        "direct_bytes": 0,
        "direct_chunks": 0,
        "relay_bytes": 0,
        "relay_chunks": 0,
    }
    for record in _runtime_mapping_records(records):
        if str(record.get("state")) not in {
            TransferStatusState.COMPLETE.value,
            TransferStatusState.FAILED.value,
            TransferStatusState.CANCELED.value,
        }:
            continue
        evidence = record.get("completion_evidence")
        if not isinstance(evidence, Mapping):
            continue
        path_evidence = evidence.get("execution_path_evidence")
        if not isinstance(path_evidence, Mapping):
            continue
        result["direct_bytes"] += int(path_evidence.get("direct_bytes", 0) or 0)
        result["direct_chunks"] += int(path_evidence.get("direct_chunks", 0) or 0)
        result["relay_bytes"] += int(path_evidence.get("relay_bytes", 0) or 0)
        result["relay_chunks"] += int(path_evidence.get("relay_chunks", 0) or 0)
    return result


def _transfer_bytes_by_direction(
    transfers: object,
    *,
    include_remaining: bool,
) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for record in _runtime_mapping_records(transfers):
        direction = str(record.get("direction", "unknown"))
        bucket = result.setdefault(
            direction,
            {"transfer_count": 0, "bytes_total": 0},
        )
        bucket["transfer_count"] += 1
        bucket["bytes_total"] += int(record.get("bytes_total", 0) or 0)
        if include_remaining:
            bucket["bytes_remaining"] = int(bucket.get("bytes_remaining", 0)) + max(
                0,
                int(record.get("bytes_total", 0) or 0)
                - int(record.get("bytes_completed", 0) or 0),
            )
    return result


def _job_runtime_state_from_records(
    job_runtime_state: Mapping[str, object],
    transfers: object,
) -> dict[str, dict[str, object]]:
    filtered_jobs = {
        str(job_id): {
            "job_id": str(job_id),
            "weight": float(
                record.get("weight", 1.0)
                if isinstance(record, Mapping)
                else 1.0
            ),
            "queued_transfer_count": 0,
            "running_transfer_count": 0,
            "active_transfer_count": 0,
            "active_bytes_total": 0,
            "active_bytes_remaining": 0,
        }
        for job_id, record in job_runtime_state.items()
    }
    for item in _runtime_mapping_records(transfers):
        if item.get("job_id") is None:
            continue
        job_id = str(item["job_id"])
        job_record = filtered_jobs.setdefault(
            job_id,
            {
                "job_id": job_id,
                "weight": 1.0,
                "queued_transfer_count": 0,
                "running_transfer_count": 0,
                "active_transfer_count": 0,
                "active_bytes_total": 0,
                "active_bytes_remaining": 0,
            },
        )
        state = str(item.get("state", ""))
        if state == TransferStatusState.SUBMITTED.value:
            job_record["queued_transfer_count"] += 1
        elif state == TransferStatusState.RUNNING.value:
            job_record["running_transfer_count"] += 1
        if _record_has_active_execution(item):
            bytes_total = int(item.get("bytes_total", 0) or 0)
            bytes_completed = int(item.get("bytes_completed", 0) or 0)
            job_record["active_transfer_count"] += 1
            job_record["active_bytes_total"] += bytes_total
            job_record["active_bytes_remaining"] += max(
                0,
                bytes_total - bytes_completed,
            )
    return dict(sorted(filtered_jobs.items()))


def _runtime_mapping_records(value: object) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list | tuple):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _record_has_admitted_execution(record: Mapping[str, object]) -> bool:
    return str(record.get("admission_state", _ADMISSION_ADMITTED)) == _ADMISSION_ADMITTED


def _record_has_active_execution(record: Mapping[str, object]) -> bool:
    state = str(record.get("state", ""))
    if state == TransferStatusState.RUNNING.value:
        return True
    if state != TransferStatusState.SUBMITTED.value:
        return False
    return _record_has_admitted_execution(record)

def _normalize_transfer_ranges(
    ranges: Iterable[dict[str, int]] | None,
) -> tuple[dict[str, int], ...] | None:
    if ranges is None:
        return None
    normalized: list[dict[str, int]] = []
    for item in ranges:
        if not isinstance(item, dict):
            raise ValueError("transfer ranges must be objects")
        src_offset = int(item["src_offset"])
        dst_offset = int(item["dst_offset"])
        bytes_count = int(item["bytes"])
        if src_offset < 0 or dst_offset < 0:
            raise ValueError("range offsets must be non-negative")
        if bytes_count <= 0:
            raise ValueError("range bytes must be positive")
        normalized.append(
            {
                "src_offset": src_offset,
                "dst_offset": dst_offset,
                "bytes": bytes_count,
            }
        )
    return tuple(normalized)


def _intent_chunk_bytes(intent: TransferIntent) -> int:
    for source in (intent.policy_hints, intent.metadata):
        if not isinstance(source, dict):
            continue
        value = source.get("chunk_bytes")
        if value is None:
            continue
        chunk_bytes = int(value)
        if chunk_bytes <= 0:
            raise ValueError("chunk_bytes must be positive")
        return chunk_bytes
    return max(1, int(intent.total_bytes))


def _status_bytes_match(
    status: TransferStatus,
    bytes_completed: int | None,
) -> bool:
    if bytes_completed is None:
        return True
    try:
        return int(bytes_completed) == status.bytes_completed
    except (TypeError, ValueError):
        return False


def _is_execution_completion_source(completion_source: str | None) -> bool:
    if completion_source is None:
        return False
    return str(completion_source).lower() in {"worker", "backend"}


def _normalize_completion_evidence(
    evidence: Mapping[str, object] | None,
    *,
    expected_bytes: int,
    completion_source: str,
    expected_ticket: ExecutionTicket | None = None,
) -> dict[str, object]:
    if not isinstance(evidence, Mapping):
        raise ValueError("complete intent transfer requires verified byte evidence")
    expected = int(expected_bytes)
    try:
        verified_bytes = int(evidence["verified_bytes"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("complete intent transfer requires verified byte evidence") from exc
    if verified_bytes != expected:
        raise ValueError(
            f"verified byte evidence mismatch: {verified_bytes} != {expected}"
        )
    content_match = bool(evidence.get("content_match", False))
    if not content_match:
        raise ValueError("complete intent transfer requires matching buffer evidence")
    source_digest = evidence.get("source_digest")
    destination_digest = evidence.get("destination_digest")
    if (
        source_digest is not None
        and destination_digest is not None
        and str(source_digest) != str(destination_digest)
    ):
        raise ValueError("verified byte evidence digest mismatch")
    ticket_binding = _normalize_completion_ticket_binding(
        evidence,
        expected_ticket=expected_ticket,
    )
    resource_evidence = evidence.get("resource_evidence")
    path_evidence = _normalize_execution_path_evidence(
        evidence,
        expected_bytes=expected,
    )
    direct_completion_evidence = evidence.get("direct_completion_evidence")
    relay_completion_evidence = evidence.get("relay_completion_evidence")
    expected_evidence_bytes = evidence.get("expected_bytes")
    return {
        "verified": True,
        "verified_bytes": verified_bytes,
        **(
            {}
            if expected_evidence_bytes is None
            else {"expected_bytes": int(expected_evidence_bytes)}
        ),
        "content_match": True,
        "verification_source": str(
            evidence.get("verification_source") or completion_source
        ),
        "verification_method": str(evidence.get("verification_method") or "unknown"),
        **(
            {}
            if source_digest is None
            else {"source_digest": str(source_digest)}
        ),
        **(
            {}
            if destination_digest is None
            else {"destination_digest": str(destination_digest)}
        ),
        **(
            {}
            if not isinstance(resource_evidence, Mapping)
            else {"resource_evidence": dict(resource_evidence)}
        ),
        **(
            {}
            if not path_evidence
            else {"execution_path_evidence": path_evidence}
        ),
        **(
            {}
            if not isinstance(direct_completion_evidence, Mapping)
            else {"direct_completion_evidence": dict(direct_completion_evidence)}
        ),
        **(
            {}
            if not isinstance(relay_completion_evidence, Mapping)
            else {"relay_completion_evidence": dict(relay_completion_evidence)}
        ),
        **ticket_binding,
    }


def _merge_completion_evidence(
    existing: Mapping[str, object] | None,
    incoming: Mapping[str, object],
) -> dict[str, object]:
    if not isinstance(existing, Mapping):
        return dict(incoming)
    merged = dict(existing)
    merged.update(dict(incoming))
    for field_name in (
        "resource_evidence",
        "execution_path_evidence",
        "direct_completion_evidence",
        "relay_completion_evidence",
        "cleanup",
    ):
        previous = existing.get(field_name)
        current = incoming.get(field_name)
        if isinstance(previous, Mapping) and isinstance(current, Mapping):
            nested = dict(previous)
            nested.update(dict(current))
            merged[field_name] = nested
        elif isinstance(previous, Mapping) and field_name not in incoming:
            merged[field_name] = dict(previous)
    return merged


def _normalize_execution_path_evidence(
    evidence: Mapping[str, object],
    *,
    expected_bytes: int,
    require_total_match: bool = True,
) -> dict[str, object]:
    result: dict[str, object] = {}
    int_fields = (
        "direct_bytes",
        "direct_chunks",
        "relay_bytes",
        "relay_chunks",
        "target_device",
        "relay_gpu",
    )
    for field_name in int_fields:
        if field_name in evidence and evidence[field_name] is not None:
            result[field_name] = int(evidence[field_name])
    relay_gpus = evidence.get("relay_gpus")
    if relay_gpus is not None:
        if isinstance(relay_gpus, (str, bytes)) or not isinstance(relay_gpus, Iterable):
            raise ValueError("execution path evidence relay_gpus must be iterable")
        result["relay_gpus"] = tuple(int(item) for item in relay_gpus)
    for field_name in (
        "executor",
        "path",
        "plan_source",
        "src_buffer_id",
        "dst_buffer_id",
        "staging_slot_id",
    ):
        value = evidence.get(field_name)
        if value is not None:
            result[field_name] = str(value)
    direct_bytes = result.get("direct_bytes")
    relay_bytes = result.get("relay_bytes")
    if require_total_match and direct_bytes is not None and relay_bytes is not None:
        path_bytes = int(direct_bytes) + int(relay_bytes)
        if path_bytes != int(expected_bytes):
            raise ValueError(
                f"execution path byte evidence mismatch: {path_bytes} != {int(expected_bytes)}"
            )
    return result


def _normalize_status_ticket_evidence(
    evidence: Mapping[str, object] | None,
    *,
    expected_ticket: ExecutionTicket,
) -> dict[str, object]:
    if not isinstance(evidence, Mapping):
        raise ValueError(
            "intent transfer status update requires daemon ticket evidence"
        )
    ticket_binding = _normalize_ticket_binding(
        evidence,
        expected_ticket=expected_ticket,
        evidence_name="status evidence",
    )
    resource_evidence = evidence.get("resource_evidence")
    if isinstance(resource_evidence, Mapping):
        ticket_binding["resource_evidence"] = dict(resource_evidence)
    path_evidence = _normalize_execution_path_evidence(
        evidence,
        expected_bytes=int(evidence.get("expected_bytes", 0) or 0),
        require_total_match=False,
    )
    if path_evidence:
        ticket_binding["execution_path_evidence"] = path_evidence
    return ticket_binding


def _normalize_completion_ticket_binding(
    evidence: Mapping[str, object],
    *,
    expected_ticket: ExecutionTicket | None,
) -> dict[str, object]:
    if expected_ticket is None:
        return {}
    return _normalize_ticket_binding(
        evidence,
        expected_ticket=expected_ticket,
        evidence_name="completion evidence",
    )


def _normalize_ticket_binding(
    evidence: Mapping[str, object],
    *,
    expected_ticket: ExecutionTicket,
    evidence_name: str,
) -> dict[str, object]:
    evidence_ticket_id = evidence.get("ticket_id")
    if evidence_ticket_id is None or str(evidence_ticket_id) != expected_ticket.ticket_id:
        raise ValueError(f"{evidence_name} ticket_id does not match daemon ticket")
    evidence_transfer_id = evidence.get("transfer_id")
    expected_transfer_id = expected_ticket.metadata.get("transfer_id")
    if (
        expected_transfer_id is not None
        and (
            evidence_transfer_id is None
            or str(evidence_transfer_id) != str(expected_transfer_id)
        )
    ):
        raise ValueError(f"{evidence_name} transfer_id does not match daemon ticket")
    evidence_generation = evidence.get("plan_generation")
    expected_generation = expected_ticket.metadata.get("plan_generation")
    if (
        expected_generation is not None
        and (
            evidence_generation is None
            or int(evidence_generation) != int(expected_generation)
        )
    ):
        raise ValueError(f"{evidence_name} plan_generation does not match daemon ticket")
    return {
        "ticket_id": expected_ticket.ticket_id,
        **(
            {}
            if expected_transfer_id is None
            else {"transfer_id": str(expected_transfer_id)}
        ),
        **(
            {}
            if expected_generation is None
            else {"plan_generation": int(expected_generation)}
        ),
    }


def _empty_removed_summary() -> dict[str, int]:
    return {
        "jobs": 0,
        "buffers": 0,
        "sessions": 0,
        "reservations": 0,
        "staging_records": 0,
        "transfers": 0,
    }


def _merge_removed(
    target: dict[str, int] | None,
    source: dict[str, int] | None,
) -> dict[str, int] | None:
    if target is None:
        return target
    if source is None:
        return target
    for key, value in source.items():
        target[key] = int(target.get(key, 0)) + int(value)
    return target
