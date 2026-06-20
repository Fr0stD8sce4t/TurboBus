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
from .admission_priority import (
    admission_with_priority_evidence as _admission_with_priority_evidence,
    ordered_delayed_admission_records as _ordered_delayed_admission_records,
)
from . import leases as daemon_leases
from .cleanup_helpers import (
    buffer_snapshot_with_retention_evidence as _buffer_snapshot_with_retention_evidence,
    empty_removed_summary as _empty_removed_summary,
    jsonable_cleanup_target_record as _jsonable_cleanup_target_record,
    merge_removed as _merge_removed,
    merge_retention_evidence as _merge_retention_evidence,
    session_cleanup_target_payload as _session_cleanup_target_payload,
)
from . import peer_auth
from . import planning_helpers
from . import profiles as daemon_profiles
from . import receipts as daemon_receipts
from . import block_runtime as daemon_block_runtime
from . import transfer_lifecycle as daemon_transfer_lifecycle
from .pcie_load_sampler import (
    HardwarePcieSample,
    HardwarePcieSamplerConfig,
    NvidiaSmiPcieLoadSampler,
    pcie_load_from_active_paths as _pcie_load_from_active_paths,
)
from .runtime_paths import (
    runtime_active_path_records_for_transfer as _runtime_active_path_records_for_transfer,
)
from .runtime_telemetry import (
    daemon_runtime_telemetry_snapshot as _daemon_runtime_telemetry_snapshot,
    empty_execution_path_evidence as _empty_execution_path_evidence,
    refresh_runtime_feedback_summary as _refresh_runtime_feedback_summary,
    runtime_mapping_records as _runtime_mapping_records,
    terminal_feedback_record_from_record as _terminal_feedback_record_from_record,
)
from .runtime_state_summary import (
    job_runtime_state_from_records as _job_runtime_state_from_records,
    runtime_transfer_summary_from_records as _runtime_transfer_summary_from_records,
)
from .runtime_state import DaemonRuntimeState
from .services import DaemonRequestRouter, DaemonTransferLifecycleService
from .evidence import (
    merge_completion_evidence,
    normalize_completion_evidence,
    normalize_execution_path_evidence,
    normalize_status_ticket_evidence,
)
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
from ..planner_engine import PlannerEngineOptions
from ..socket_security import (
    UnixSocketSecurityPolicy,
    secure_unix_socket,
    unlink_stale_socket,
)
from ..topology import TopologyProvider
from ..topology.pcie_fabric import pcie_fabric_snapshot_from_inventory
from ..scheduler.bandwidth_pool import (
    build_bandwidth_pool_snapshot as _build_bandwidth_pool_snapshot,
    build_runtime_edge_load_snapshot as _build_runtime_edge_load_snapshot,
)
from ..scheduler.block_plan import block_plan_from_mapping as _block_plan_from_mapping
from ..scheduler.block_queue import (
    queue_records_for_block_plan as _queue_records_for_block_plan,
    queue_summary as _block_queue_summary,
)
from ..scheduler import (
    DaemonScheduler,
    SchedulingDecision,
    scheduling_decision_leases,
)
from ..scheduler.load_feedback import (
    relay_admission_blocked_reason,
    relay_activity_from_runtime_state,
    relay_fairness_admission_blocked_reason,
    runtime_view,
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
_PUBLIC_REDACTED_VALUE = "<redacted>"
_PUBLIC_SENSITIVE_FIELD_NAMES = {
    "address",
    "allocation_base_ptr",
    "buffer_address",
    "cuda_ipc_handle",
    "device_ipc_base_ptr",
    "host_ptr",
    "device_ptr",
    "ipc_handle",
    "lease_token",
    "token",
}
_PUBLIC_SENSITIVE_FIELD_FRAGMENTS = (
    "host_ptr",
    "cuda_ipc_handle",
    "device_ptr",
    "lease_token",
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
        min_pool_bytes: int = 12 * 1024 * 1024,
        min_chunks_for_relay: int = 2,
        relay_min_effective_bw_gbps: float = 0.0,
        relay_min_direct_ratio: float = 0.0,
        topology_provider: TopologyProvider | None = None,
        require_authenticated_peers: bool = False,
        pcie_sample_max_age_seconds: float = 1.0,
        pcie_sample_timeout_seconds: float = 1.0,
        socket_security_policy: UnixSocketSecurityPolicy | None = None,
        max_sessions_per_uid: int = 16,
        max_jobs_per_uid: int = 64,
        max_buffers_per_uid: int = 4096,
        max_buffer_bytes_per_uid: int = 0,
    ) -> None:
        relays = tuple(self._normalize_relays(relay_gpus))
        self._lock = threading.Lock()
        DaemonRuntimeState().bind_to(self)
        self._request_router = DaemonRequestRouter(self)
        self._transfer_lifecycle_service = DaemonTransferLifecycleService(self)
        self._scheduler = DaemonScheduler(
            planner_options=PlannerEngineOptions(
                min_pool_bytes=int(min_pool_bytes),
                min_chunks_for_relay=int(min_chunks_for_relay),
                relay_min_effective_bw_gbps=float(relay_min_effective_bw_gbps),
                relay_min_direct_ratio=float(relay_min_direct_ratio),
            )
        )
        self._topology_provider = topology_provider
        self._session_timeout_seconds = max(0.0, float(session_timeout_seconds))
        self._profile_max_age_seconds = max(0.0, float(profile_max_age_seconds))
        self._require_authenticated_peers = bool(require_authenticated_peers)
        self._pcie_sampler = NvidiaSmiPcieLoadSampler(
            HardwarePcieSamplerConfig(
                timeout_seconds=max(0.001, float(pcie_sample_timeout_seconds))
            )
        )
        self._last_pcie_sample = None
        self._pcie_sample_max_age_seconds = max(0.0, float(pcie_sample_max_age_seconds))
        self._socket_security_policy = socket_security_policy or UnixSocketSecurityPolicy()
        self._last_socket_security_record = None
        self._tenant_quota_policy = {
            "max_sessions_per_uid": max(0, int(max_sessions_per_uid)),
            "max_jobs_per_uid": max(0, int(max_jobs_per_uid)),
            "max_buffers_per_uid": max(0, int(max_buffers_per_uid)),
            "max_buffer_bytes_per_uid": max(0, int(max_buffer_bytes_per_uid)),
        }
        self._tenant_usage_by_uid: dict[str, dict[str, int]] = {}
        self._quota_rejections: list[dict[str, object]] = []
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
            return planning_helpers.topology_unavailable_response(
                _TOPOLOGY_UNAVAILABLE_ERROR,
            )
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
            return planning_helpers.topology_unavailable_response(
                _TOPOLOGY_UNAVAILABLE_ERROR,
            )
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
        requested_relays: Iterable[int] | None = None,
    ) -> DaemonResponse:
        now = time.time()
        target = None if target_gpu is None else int(target_gpu)
        with self._lock:
            self._reap_stale_sessions_locked(now)
            self._refresh_admission_state_locked(now=now)
            if self._topology_provider is None:
                return planning_helpers.topology_unavailable_response(
                    _TOPOLOGY_UNAVAILABLE_ERROR,
                )
            inventory = self._topology_provider.snapshot()
            candidates = (
                tuple(self._normalize_relays(requested_relays))
                if requested_relays is not None
                else tuple(sorted(self._relay_quotas))
            )
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
            self._refresh_admission_state_locked(now=now)
            quota_response = self._tenant_quota_precheck_locked(
                peer_identity,
                field="active_sessions",
                delta_count=1,
            )
            if quota_response is not None:
                return quota_response
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
            quota_response = self._tenant_quota_precheck_locked(
                peer_identity,
                field="registered_jobs",
                delta_count=0 if job.job_id in self._jobs else 1,
            )
            if quota_response is not None:
                return quota_response
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
            existing_job = self._jobs.get(job.job_id)
            if existing_job is not None:
                try:
                    self._validate_peer_owns_job_locked(
                        job_id=job.job_id,
                        peer_identity=peer_identity,
                    )
                except ValueError as exc:
                    return DaemonResponse(ok=False, error=str(exc))
                if planning_helpers.job_identity_conflicts(existing_job, job):
                    return DaemonResponse(
                        ok=False,
                        error=(
                            "job_id is already bound to a different production identity"
                        ),
                    )
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
            self._refresh_admission_state_locked(now=now)
            if buffer.job_id not in self._jobs:
                return DaemonResponse(ok=False, error="unknown job")
            quota_response = self._tenant_quota_precheck_locked(
                peer_identity,
                field="registered_buffers",
                delta_count=0 if buffer.buffer_id in self._buffers else 1,
                delta_bytes=0 if buffer.buffer_id in self._buffers else buffer.size_bytes,
            )
            if quota_response is not None:
                return quota_response
            try:
                self._validate_peer_owns_job_locked(
                    job_id=buffer.job_id,
                    peer_identity=peer_identity,
                )
            except ValueError as exc:
                return DaemonResponse(ok=False, error=str(exc))
            existing_buffer = self._buffers.get(buffer.buffer_id)
            if existing_buffer is not None:
                try:
                    self._validate_peer_owns_buffer_locked(
                        buffer_id=buffer.buffer_id,
                        peer_identity=peer_identity,
                    )
                except ValueError as exc:
                    return DaemonResponse(ok=False, error=str(exc))
                if planning_helpers.buffer_registration_conflicts(existing_buffer, buffer):
                    return DaemonResponse(
                        ok=False,
                        error=(
                            "buffer_id is already bound to a different production registration"
                        ),
                    )
                return DaemonResponse(
                    ok=True,
                    payload={
                        "buffer": asdict(existing_buffer),
                        "buffer_ownership": self._buffer_ownership_record_locked(
                            existing_buffer.buffer_id
                        ),
                    },
                )
            protection = self._active_buffer_protection_record_locked(buffer.buffer_id)
            if bool(protection.get("protected", False)):
                return DaemonResponse(
                    ok=False,
                    error="buffer has active daemon-issued execution",
                    payload={"buffer_protection": protection},
                )
            self._buffers[buffer.buffer_id] = buffer
            return DaemonResponse(
                ok=True,
                payload={
                    "buffer": asdict(buffer),
                    "buffer_ownership": self._buffer_ownership_record_locked(
                        buffer.buffer_id
                    ),
                },
            )

    def cleanup(
        self,
        target_kind: str,
        target_id: str,
        reason: str,
        force: bool = False,
        owner_binding: Mapping[str, object] | None = None,
        retention_evidence: Mapping[str, object] | None = None,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        cleanup = CleanupRequest(
            target_kind=target_kind,
            target_id=target_id,
            reason=reason,
            force=force,
            owner_binding=owner_binding,
            retention_evidence=retention_evidence,
        )
        with self._lock:
            validated_owner_binding = (
                self._validate_cleanup_owner_binding_locked(
                    cleanup,
                    peer_identity=peer_identity,
                )
            )
            removed = _empty_removed_summary()
            if cleanup.target_kind == "job":
                return self._cleanup_job_target_locked(
                    cleanup,
                    peer_identity=peer_identity,
                    removed=removed,
                    validated_owner_binding=validated_owner_binding,
                )
            elif cleanup.target_kind == "buffer":
                return self._cleanup_buffer_target_locked(
                    cleanup,
                    peer_identity=peer_identity,
                    removed=removed,
                )
            elif cleanup.target_kind == "session":
                return self._cleanup_session_target_locked(
                    cleanup,
                    peer_identity=peer_identity,
                    removed=removed,
                    validated_owner_binding=validated_owner_binding,
                    retention_recorded=False,
                )
            elif cleanup.target_kind == "reservation":
                return self._cleanup_reservation_target_locked(
                    cleanup,
                    peer_identity=peer_identity,
                    removed=removed,
                    validated_owner_binding=validated_owner_binding,
                )
            else:
                return DaemonResponse(ok=False, error="unsupported cleanup target")

    def _finalize_cleanup_response_locked(
        self,
        *,
        cleanup: CleanupRequest,
        removed: dict[str, int],
        validated_owner_binding: dict[str, object] | None,
        retention_recorded: bool,
        cleanup_result: dict[str, object],
    ) -> DaemonResponse:
        self._cleanup_events.append(cleanup)
        admission_refresh = self._refresh_admission_state_locked(now=time.time())
        return DaemonResponse(
            ok=True,
            payload={
                "cleanup": asdict(cleanup),
                "removed": removed,
                "promoted_transfers": admission_refresh["promoted_transfers"],
                "admission_refresh": admission_refresh,
                **(
                    {}
                    if validated_owner_binding is None
                    else {"owner_binding": validated_owner_binding}
                ),
                **(
                    {}
                    if not retention_recorded
                    else {"retention_evidence_recorded": True}
                ),
                **cleanup_result,
            },
        )

    def _cleanup_job_target_locked(
        self,
        cleanup: CleanupRequest,
        *,
        peer_identity: PeerIdentity | None,
        removed: dict[str, int],
        validated_owner_binding: dict[str, object] | None,
    ) -> DaemonResponse:
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
        return self._finalize_cleanup_response_locked(
            cleanup=cleanup,
            removed=removed,
            validated_owner_binding=validated_owner_binding,
            retention_recorded=False,
            cleanup_result={},
        )

    def _cleanup_session_target_locked(
        self,
        cleanup: CleanupRequest,
        *,
        peer_identity: PeerIdentity | None,
        removed: dict[str, int],
        validated_owner_binding: dict[str, object] | None,
        retention_recorded: bool,
    ) -> DaemonResponse:
        archived_target = self._retired_cleanup_target_record_locked(
            target_kind=cleanup.target_kind,
            target_id=cleanup.target_id,
        )
        owned_cleanup_targets = (
            None
            if cleanup.target_id not in self._sessions
            else self._session_owned_cleanup_targets_locked(cleanup.target_id)
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
            owned_cleanup_targets=owned_cleanup_targets,
        )
        if session is None and not cleanup.force:
            return DaemonResponse(ok=False, error="unknown session")
        return self._finalize_cleanup_response_locked(
            cleanup=cleanup,
            removed=removed,
            validated_owner_binding=validated_owner_binding,
            retention_recorded=retention_recorded,
            cleanup_result=_session_cleanup_target_payload(owned_cleanup_targets),
        )

    def _cleanup_reservation_target_locked(
        self,
        cleanup: CleanupRequest,
        *,
        peer_identity: PeerIdentity | None,
        removed: dict[str, int],
        validated_owner_binding: dict[str, object] | None,
    ) -> DaemonResponse:
        archived_target = self._retired_cleanup_target_record_locked(
            target_kind=cleanup.target_kind,
            target_id=cleanup.target_id,
        )
        if validated_owner_binding is None:
            owner_response = self._validate_reservation_cleanup_owner_locked(
                cleanup=cleanup,
                archived_target=archived_target,
                peer_identity=peer_identity,
            )
            if not owner_response.ok:
                return owner_response
            if owner_response.payload.get("cleanup_mode") == "noop":
                return self._noop_reservation_cleanup_response(
                    cleanup=cleanup,
                    removed=removed,
                )
        if self._reservation_cleanup_target_is_missing(cleanup.target_id):
            if archived_target is None:
                return DaemonResponse(ok=False, error="unknown reservation")
            if validated_owner_binding is None:
                try:
                    self._validate_peer_owns_missing_cleanup_target_locked(
                        target_kind=cleanup.target_kind,
                        target_id=cleanup.target_id,
                        peer_identity=peer_identity,
                    )
                except ValueError as owner_exc:
                    return DaemonResponse(ok=False, error=str(owner_exc))
            retention_recorded = self._record_reservation_cleanup_retention_locked(
                cleanup
            )
            return self._noop_reservation_cleanup_response(
                cleanup=cleanup,
                removed=removed,
                retention_recorded=retention_recorded,
            )
        released = self._cleanup_existing_reservation_target_locked(cleanup)
        if self._reservation_cleanup_release_is_empty(released) and not cleanup.force:
            return DaemonResponse(ok=False, error="unknown reservation")
        _merge_removed(removed, released)
        retention_recorded = self._record_reservation_cleanup_retention_locked(
            cleanup
        )
        cleanup_result = self._reservation_cleanup_result(
            cleanup=cleanup,
            released=released,
        )
        return self._finalize_cleanup_response_locked(
            cleanup=cleanup,
            removed=removed,
            validated_owner_binding=validated_owner_binding,
            retention_recorded=retention_recorded,
            cleanup_result=cleanup_result,
        )

    def _validate_reservation_cleanup_owner_locked(
        self,
        *,
        cleanup: CleanupRequest,
        archived_target: Mapping[str, object] | None,
        peer_identity: PeerIdentity | None,
    ) -> DaemonResponse:
        try:
            self._validate_peer_owns_lease_locked(
                lease_id=cleanup.target_id,
                peer_identity=peer_identity,
            )
            return DaemonResponse(ok=True, payload={})
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
                return DaemonResponse(ok=True, payload={})
            if archived_target is None:
                return DaemonResponse(ok=False, error=str(exc))
            if cleanup.force:
                return DaemonResponse(ok=True, payload={})
            try:
                self._validate_peer_owns_missing_cleanup_target_locked(
                    target_kind=cleanup.target_kind,
                    target_id=cleanup.target_id,
                    peer_identity=peer_identity,
                )
            except ValueError as owner_exc:
                return DaemonResponse(ok=False, error=str(owner_exc))
            return DaemonResponse(ok=True, payload={"cleanup_mode": "noop"})

    def _reservation_cleanup_target_is_missing(self, target_id: str) -> bool:
        return target_id not in self._reservations and target_id not in self._staging_records

    def _noop_reservation_cleanup_response(
        self,
        *,
        cleanup: CleanupRequest,
        removed: dict[str, int],
        retention_recorded: bool = False,
    ) -> DaemonResponse:
        return DaemonResponse(
            ok=True,
            payload={
                "cleanup": asdict(cleanup),
                "removed": removed,
                "reservation_id": cleanup.target_id,
                "cleaned_reservation_ids": (),
                "cleanup_kind": cleanup.target_kind,
                "cleanup_mode": "noop",
                **(
                    {}
                    if not retention_recorded
                    else {"retention_evidence_recorded": True}
                ),
            },
        )

    def _record_reservation_cleanup_retention_locked(
        self,
        cleanup: CleanupRequest,
    ) -> bool:
        if not isinstance(cleanup.retention_evidence, Mapping):
            return False
        return self._record_cleanup_retention_evidence_locked(
            target_kind=cleanup.target_kind,
            target_id=cleanup.target_id,
            retention_evidence=cleanup.retention_evidence,
        )

    def _cleanup_existing_reservation_target_locked(
        self,
        cleanup: CleanupRequest,
    ) -> dict[str, int]:
        cleanup_marks_transfer_terminal = cleanup.reason != "worker_complete"
        return self._release_reservation_and_count_locked(
            cleanup.target_id,
            final_state=(
                TransferStatusState.CANCELED
                if cleanup_marks_transfer_terminal
                else TransferStatusState.COMPLETE
            ),
            cleanup_reason=cleanup.reason,
            mark_terminal=cleanup_marks_transfer_terminal,
        )

    def _reservation_cleanup_release_is_empty(
        self,
        released: Mapping[str, object],
    ) -> bool:
        return (
            int(released["reservations"]) == 0
            and int(released["staging_records"]) == 0
        )

    def _reservation_cleanup_result(
        self,
        *,
        cleanup: CleanupRequest,
        released: Mapping[str, object],
    ) -> dict[str, object]:
        cleaned = not self._reservation_cleanup_release_is_empty(released)
        return {
            "reservation_id": cleanup.target_id,
            "cleaned_reservation_ids": (cleanup.target_id,) if cleaned else (),
            "cleanup_kind": cleanup.target_kind,
            "cleanup_mode": "cleanup" if cleaned else "noop",
        }

    def _cleanup_buffer_target_locked(
        self,
        cleanup: CleanupRequest,
        *,
        peer_identity: PeerIdentity | None,
        removed: dict[str, int],
    ) -> DaemonResponse:
        retention_recorded = False
        archived_target = self._retired_cleanup_target_record_locked(
            target_kind=cleanup.target_kind,
            target_id=cleanup.target_id,
        )
        if cleanup.target_id not in self._buffers and not cleanup.force:
            return self._cleanup_missing_buffer_target_locked(
                cleanup=cleanup,
                peer_identity=peer_identity,
                removed=removed,
                archived_target=archived_target,
            )
        owner_response = self._validate_buffer_cleanup_owner_locked(
            cleanup=cleanup,
            peer_identity=peer_identity,
        )
        if not owner_response.ok:
            return owner_response
        buffer = self._buffers.get(cleanup.target_id)
        if buffer is not None:
            protection_response = self._protect_active_buffer_cleanup_locked(
                cleanup=cleanup,
                buffer=buffer,
                peer_identity=peer_identity,
            )
            if not protection_response.ok:
                return protection_response
        transfer_ids = self._transfer_ids_for_buffer_locked(cleanup.target_id)
        self._release_buffer_cleanup_leases_locked(cleanup=cleanup, removed=removed)
        self._remove_buffer_cleanup_state_locked(
            cleanup=cleanup,
            transfer_ids=transfer_ids,
            removed=removed,
        )
        retention_recorded = self._record_removed_buffer_retention_locked(cleanup)
        return self._finalize_cleanup_response_locked(
            cleanup=cleanup,
            removed=removed,
            validated_owner_binding=None,
            retention_recorded=retention_recorded,
            cleanup_result={},
        )

    def _cleanup_missing_buffer_target_locked(
        self,
        *,
        cleanup: CleanupRequest,
        peer_identity: PeerIdentity | None,
        removed: dict[str, int],
        archived_target: Mapping[str, object] | None,
    ) -> DaemonResponse:
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
        retention_recorded = False
        if isinstance(cleanup.retention_evidence, Mapping):
            retention_recorded = self._record_cleanup_retention_evidence_locked(
                target_kind=cleanup.target_kind,
                target_id=cleanup.target_id,
                retention_evidence=cleanup.retention_evidence,
            )
        return DaemonResponse(
            ok=True,
            payload={
                "cleanup": asdict(cleanup),
                "removed": removed,
                **(
                    {}
                    if not retention_recorded
                    else {"retention_evidence_recorded": True}
                ),
            },
        )

    def _validate_buffer_cleanup_owner_locked(
        self,
        *,
        cleanup: CleanupRequest,
        peer_identity: PeerIdentity | None,
    ) -> DaemonResponse:
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
        return DaemonResponse(ok=True)

    def _protect_active_buffer_cleanup_locked(
        self,
        *,
        cleanup: CleanupRequest,
        buffer: BufferRegistration,
        peer_identity: PeerIdentity | None,
    ) -> DaemonResponse:
        protection = self._active_buffer_protection_record_locked(cleanup.target_id)
        if bool(protection.get("protected", False)):
            return DaemonResponse(
                ok=False,
                error="buffer has active daemon-issued execution",
                payload={
                    "cleanup": asdict(cleanup),
                    "buffer_ownership": self._buffer_ownership_record_locked(
                        cleanup.target_id
                    ),
                    "buffer_protection": protection,
                },
            )
        self._archive_cleanup_target_locked(
            target_kind=cleanup.target_kind,
            target_id=cleanup.target_id,
            peer_identity=peer_identity,
            reason=cleanup.reason,
            transfer_ids=self._transfer_ids_for_buffer_locked(cleanup.target_id),
            buffer_snapshot=planning_helpers.buffer_snapshot_record(buffer),
            retention_evidence=_merge_retention_evidence(
                self._buffer_cleanup_ownership_evidence_locked(
                    cleanup.target_id,
                    reason=cleanup.reason,
                ),
                cleanup.retention_evidence,
            ),
        )
        return DaemonResponse(ok=True)

    def _release_buffer_cleanup_leases_locked(
        self,
        *,
        cleanup: CleanupRequest,
        removed: dict[str, int],
    ) -> None:
        for lease_id in self._active_buffer_lease_ids_locked(cleanup.target_id):
            _merge_removed(
                removed,
                self._release_reservation_and_count_locked(
                    lease_id,
                    final_state=TransferStatusState.CANCELED,
                    cleanup_reason=cleanup.reason,
                ),
            )

    def _remove_buffer_cleanup_state_locked(
        self,
        *,
        cleanup: CleanupRequest,
        transfer_ids: tuple[str, ...],
        removed: dict[str, int],
    ) -> None:
        buffer = self._buffers.pop(cleanup.target_id, None)
        if buffer is not None:
            removed["buffers"] = int(removed["buffers"]) + 1
        for transfer_id in transfer_ids:
            status = self._transfer_statuses.get(transfer_id)
            if status is not None and status.state not in _TERMINAL_TRANSFER_STATES:
                self._mark_transfer_terminal_locked(
                    transfer_id,
                    TransferStatusState.CANCELED,
                    error=cleanup.reason,
                )
                removed["transfers"] = int(removed["transfers"]) + 1
            self._retire_transfer_runtime_state_locked(transfer_id)

    def _record_removed_buffer_retention_locked(
        self,
        cleanup: CleanupRequest,
    ) -> bool:
        cleanup_retention = _merge_retention_evidence(
            self._buffer_cleanup_ownership_evidence_for_removed_locked(
                cleanup.target_id,
                reason=cleanup.reason,
            ),
            cleanup.retention_evidence,
        )
        if not isinstance(cleanup_retention, Mapping):
            return False
        return self._record_cleanup_retention_evidence_locked(
            target_kind=cleanup.target_kind,
            target_id=cleanup.target_id,
            retention_evidence=cleanup_retention,
        )

    def close_session(
        self,
        session_id: str,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        with self._lock:
            self._reap_stale_sessions_locked(time.time())
            owned_cleanup_targets = (
                None
                if str(session_id) not in self._sessions
                else self._session_owned_cleanup_targets_locked(str(session_id))
            )
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
                owned_cleanup_targets=owned_cleanup_targets,
            )
            payload = {
                "session_id": session_id,
                "removed": removed,
                **_session_cleanup_target_payload(owned_cleanup_targets),
            }
            if session is None:
                if archived_target is None:
                    return DaemonResponse(ok=False, error="unknown session")
                return DaemonResponse(ok=True, payload=payload)
            return DaemonResponse(ok=True, payload=payload)

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
                return self._handle_terminal_transfer_status_update_locked(
                    status,
                    requested_state=requested_state,
                    bytes_completed=bytes_completed,
                    error=error,
                    completion_source=completion_source,
                    completion_evidence=completion_evidence,
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
                resolved_bytes_completed = status.bytes_completed
                if bytes_completed is not None:
                    resolved_bytes_completed = max(
                        int(status.bytes_completed),
                        int(bytes_completed),
                    )
                updated = TransferStatus(
                    transfer_id=status.transfer_id,
                    job_id=status.job_id,
                    state=requested_state,
                    bytes_total=status.bytes_total,
                    bytes_completed=resolved_bytes_completed,
                    session_id=status.session_id,
                    error=status.error if error is None else error,
                )
            except ValueError as exc:
                if requested_state is TransferStatusState.COMPLETE:
                    return self._handle_transfer_status_mismatch_locked(
                        status,
                        error=str(exc),
                    )
                return DaemonResponse(ok=False, error=str(exc))
            evidence_update = self._normalize_transfer_status_evidence_update_locked(
                updated,
                completion_source=completion_source,
                completion_evidence=completion_evidence,
            )
            if not evidence_update.ok:
                return evidence_update
            normalized_completion_source = str(
                evidence_update.payload["completion_source"]
            )
            normalized_completion_evidence = evidence_update.payload.get(
                "completion_evidence"
            )
            block_progress_evidence = self._advance_block_runtime_for_status_locked(
                updated,
                completion_source=normalized_completion_source,
                completion_evidence=normalized_completion_evidence,
                now=checked_at,
            )
            if block_progress_evidence is not None:
                normalized_completion_evidence = merge_completion_evidence(
                    normalized_completion_evidence,
                    {"block_runtime": block_progress_evidence},
                )
                normalized_completion_evidence = (
                    self._completion_evidence_with_block_cleanup_locked(
                        updated.transfer_id,
                        normalized_completion_evidence,
                    )
                )
            completion_ticket = evidence_update.payload.get("completion_ticket")
            self._persist_transfer_status_update_locked(
                updated,
                completion_source=normalized_completion_source,
                completion_evidence=normalized_completion_evidence,
                completion_ticket=completion_ticket,
            )
            return self._finalize_transfer_status_update_locked(
                updated,
            )

    def _handle_terminal_transfer_status_update_locked(
        self,
        status: TransferStatus,
        *,
        requested_state: TransferStatusState,
        bytes_completed: int | None,
        error: str | None,
        completion_source: str | None,
        completion_evidence: Mapping[str, object] | None,
    ) -> DaemonResponse:
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
                evidence_error = self._completion_release_blocked_reason_locked(
                    status.transfer_id
                )
                if evidence_error is not None:
                    return DaemonResponse(ok=False, error=evidence_error)
            return DaemonResponse(ok=True, payload={"status": asdict(status)})
        return DaemonResponse(
            ok=False,
            error="terminal transfer status cannot be updated",
        )

    def _persist_transfer_status_update_locked(
        self,
        updated: TransferStatus,
        *,
        completion_source: str,
        completion_evidence: Mapping[str, object] | None,
        completion_ticket: ExecutionTicket | None,
    ) -> None:
        self._transfer_statuses[updated.transfer_id] = updated
        actions = daemon_transfer_lifecycle.status_persistence_actions(
            status=updated,
            completion_evidence=completion_evidence,
            completion_ticket=completion_ticket,
        )
        if bool(actions["store_completion_ticket"]) and completion_ticket is not None:
            self._transfer_completion_tickets[updated.transfer_id] = (
                completion_ticket
            )
        if bool(actions["mark_admission_terminal"]):
            self._mark_transfer_admission_terminal_locked(
                updated.transfer_id,
                updated.state,
                reason=updated.error,
            )
        if bool(actions["drop_active_ticket"]):
            self._drop_execution_ticket_for_transfer_locked(updated.transfer_id)
        if bool(actions["store_completion_source"]):
            self._transfer_completion_sources[updated.transfer_id] = completion_source
        if bool(actions["merge_completion_evidence"]):
            existing_evidence = self._transfer_completion_evidence.get(
                updated.transfer_id
            )
            self._transfer_completion_evidence[updated.transfer_id] = (
                merge_completion_evidence(
                    existing_evidence,
                    completion_evidence,
                )
            )
        self._refresh_transfer_queue_record_locked(updated.transfer_id)
        if bool(actions["record_terminal_feedback"]):
            self._record_terminal_runtime_feedback_locked(updated.transfer_id)

    def _finalize_transfer_status_update_locked(
        self,
        updated: TransferStatus,
    ) -> DaemonResponse:
        removed = _empty_removed_summary()
        promoted = ()
        plan = daemon_transfer_lifecycle.terminal_finalization_plan(updated)
        event_type = plan.get("event_type")
        if event_type is not None:
            audit_kwargs = {
                "event_type": str(event_type),
                "transfer_id": updated.transfer_id,
                "state": updated.state,
                "bytes_completed": updated.bytes_completed,
            }
            if plan.get("reason") is not None:
                audit_kwargs["reason"] = str(plan["reason"])
            if plan.get("failure_reason") is not None:
                audit_kwargs["failure_reason"] = str(plan["failure_reason"])
            self._append_transfer_audit_records_locked(
                **audit_kwargs,
            )
        if bool(plan.get("release_reservations", False)):
            _merge_removed(
                removed,
                self._release_reservations_for_transfer_locked(
                    updated.transfer_id,
                    final_state=updated.state,
                    cleanup_reason=str(plan["reason"]),
                ),
            )
        if plan.get("retire_reason") is not None:
            _merge_removed(
                removed,
                self._retire_terminal_transfer_without_reservations_locked(
                    updated.transfer_id,
                    reason=str(plan["retire_reason"]),
                ),
            )
        refresh_admission = str(plan.get("refresh_admission", "never"))
        if refresh_admission == "always" or (
            refresh_admission == "if_transfer_removed"
            and int(removed["transfers"]) > 0
        ):
            admission_refresh = self._refresh_admission_state_locked(
                now=time.time(),
                reap_expired=False,
            )
            promoted = admission_refresh["promoted_transfers"]
        if bool(plan.get("record_failure_cleanup_contract", False)):
            self._record_failure_cleanup_contract_locked(
                transfer_id=updated.transfer_id,
                final_state=updated.state,
                error=str(plan["reason"]),
                removed=removed,
                promoted=promoted,
            )
        return DaemonResponse(
            ok=True,
            payload=daemon_transfer_lifecycle.terminal_status_payload(
                status=updated,
                removed=removed,
                promoted_transfers=promoted,
            ),
        )

    def _handle_transfer_status_mismatch_locked(
        self,
        status: TransferStatus,
        *,
        error: str,
    ) -> DaemonResponse:
        mismatch = str(error)
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
        _merge_removed(
            removed,
            self._retire_terminal_transfer_without_reservations_locked(
                status.transfer_id,
                reason="transfer_status_mismatch",
            ),
        )
        admission_refresh = self._refresh_admission_state_locked(
            now=time.time(),
            reap_expired=False,
        )
        self._refresh_transfer_queue_record_locked(status.transfer_id)
        return DaemonResponse(
            ok=False,
            error=mismatch,
            payload={
                "status": asdict(failed),
                "removed": removed,
                "promoted_transfers": admission_refresh["promoted_transfers"],
                "admission_refresh": admission_refresh,
            },
        )

    def _normalize_transfer_status_evidence_update_locked(
        self,
        updated: TransferStatus,
        *,
        completion_source: str | None,
        completion_evidence: Mapping[str, object] | None,
    ) -> DaemonResponse:
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
                    normalized_completion_evidence = normalize_completion_evidence(
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
                        normalized_completion_evidence = normalize_completion_evidence(
                            completion_evidence,
                            expected_bytes=updated.bytes_total,
                            completion_source=normalized_completion_source,
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
                normalized_completion_evidence = normalize_status_ticket_evidence(
                    completion_evidence,
                    expected_ticket=ticket,
                )
                if updated.state in {
                    TransferStatusState.FAILED,
                    TransferStatusState.CANCELED,
                }:
                    completion_ticket = ticket
            except ValueError as exc:
                return DaemonResponse(ok=False, error=str(exc))
        return DaemonResponse(
            ok=True,
            payload={
                "completion_source": normalized_completion_source,
                "completion_evidence": normalized_completion_evidence,
                "completion_ticket": completion_ticket,
            },
        )

    def _advance_block_runtime_for_status_locked(
        self,
        status: TransferStatus,
        *,
        completion_source: str | None,
        completion_evidence: Mapping[str, object] | None,
        now: float,
    ) -> dict[str, object] | None:
        records = self._block_runtime_records.get(status.transfer_id)
        if not records:
            return None
        block_progress = (
            completion_evidence.get("block_progress")
            if isinstance(completion_evidence, Mapping)
            else None
        )
        if isinstance(block_progress, Mapping):
            updated, evidence = daemon_block_runtime.advance_from_worker_progress(
                records,
                progress=block_progress,
                completion_source=completion_source,
                completion_evidence=completion_evidence,
                now=float(now),
            )
        else:
            updated, evidence = daemon_block_runtime.advance_for_status(
                records,
                state=status.state,
                bytes_completed=status.bytes_completed,
                completion_source=completion_source,
                completion_evidence=completion_evidence,
                now=float(now),
            )
        self._block_runtime_records[status.transfer_id] = tuple(
            record.as_dict() for record in updated
        )
        self._runtime_state_version += 1
        return evidence

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
                supplemental = normalize_completion_evidence(
                    completion_evidence,
                    expected_bytes=status.bytes_total,
                    completion_source=normalized_completion_source,
                    expected_ticket=ticket,
                )
            else:
                supplemental = normalize_status_ticket_evidence(
                    completion_evidence,
                    expected_ticket=ticket,
                )
        except ValueError as exc:
            return DaemonResponse(ok=False, error=str(exc))
        existing = dict(self._transfer_completion_evidence.get(status.transfer_id, {}))
        block_progress_evidence = self._advance_block_runtime_for_status_locked(
            status,
            completion_source=normalized_completion_source,
            completion_evidence=supplemental,
            now=time.time(),
        )
        if block_progress_evidence is not None:
            supplemental = merge_completion_evidence(
                supplemental,
                {"block_runtime": block_progress_evidence},
            )
            supplemental = self._completion_evidence_with_block_cleanup_locked(
                status.transfer_id,
                supplemental,
            )
        self._transfer_completion_sources[status.transfer_id] = normalized_completion_source
        self._transfer_completion_evidence[status.transfer_id] = (
            merge_completion_evidence(existing, supplemental)
        )
        self._archive_transfer_receipt_state_locked(status.transfer_id)
        self._refresh_transfer_queue_record_locked(status.transfer_id)
        self._record_terminal_runtime_feedback_locked(status.transfer_id)
        return DaemonResponse(ok=True)

    def submit_transfer_intent(
        self,
        intent: TransferIntent,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        return self._transfer_lifecycle_service.submit_transfer_intent(
            intent,
            peer_identity=peer_identity,
        )

    def _submit_transfer_intent_impl(
        self,
        intent: TransferIntent,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        if not isinstance(intent, TransferIntent):
            return DaemonResponse(ok=False, error="intent must be a TransferIntent")
        try:
            chunk_bytes = _intent_chunk_bytes(intent)
            transfer_mode = _intent_transfer_mode(intent)
        except (TypeError, ValueError) as exc:
            return DaemonResponse(ok=False, error=str(exc))
        with self._lock:
            quota_response = self._tenant_quota_precheck_locked(
                peer_identity,
                field="active_transfers",
                delta_count=1,
            )
            if quota_response is not None:
                return quota_response
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
            mode=transfer_mode,
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
            self._refresh_admission_state_locked(now=now)
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
        return self._transfer_lifecycle_service.wait_transfer_receipt(
            intent_id,
            timeout_seconds=timeout_seconds,
            peer_identity=peer_identity,
        )

    def _wait_transfer_receipt_impl(
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

    def recover_transfer_state(
        self,
        *,
        intent_id: str | None = None,
        transfer_id: str | None = None,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        return self._transfer_lifecycle_service.recover_transfer_state(
            intent_id=intent_id,
            transfer_id=transfer_id,
            peer_identity=peer_identity,
        )

    def _recover_transfer_state_impl(
        self,
        *,
        intent_id: str | None = None,
        transfer_id: str | None = None,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        with self._lock:
            try:
                normalized_transfer_id = self._resolve_recovery_transfer_id_locked(
                    intent_id=intent_id,
                    transfer_id=transfer_id,
                )
                archived = self._transfer_receipt_archive.get(
                    normalized_transfer_id,
                    {},
                )
                status = self._transfer_statuses.get(normalized_transfer_id)
                if status is None and isinstance(archived.get("status"), TransferStatus):
                    status = archived["status"]
                if status is None:
                    return DaemonResponse(ok=False, error="transfer status is unavailable")
                self._validate_peer_owns_receipt_transfer_locked(
                    transfer_id=normalized_transfer_id,
                    job_id=status.job_id,
                    peer_identity=peer_identity,
                )
                recovery_state = self._transfer_recovery_state_locked(
                    normalized_transfer_id,
                    status=status,
                    archived=archived,
                    now=time.time(),
                )
            except ValueError as exc:
                return DaemonResponse(ok=False, error=str(exc))
            return DaemonResponse(
                ok=True,
                payload={"transfer_recovery": recovery_state},
            )

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
            preflight = self._authorize_worker_transfer_preflight_locked(
                request,
                peer_identity=peer_identity,
                now=now,
            )
            if not preflight.ok:
                return preflight
            status = preflight.payload["status"]
            lease = preflight.payload["lease"]
            cleanup_payload = preflight.payload["cleanup_payload"]

            def authorization_failure(error: str) -> DaemonResponse:
                return DaemonResponse(ok=False, error=error, payload=cleanup_payload)

            context_response = self._worker_authorization_context_locked(
                request,
                lease=lease,
                peer_identity=peer_identity,
                now=now,
            )
            if not context_response.ok:
                return authorization_failure(str(context_response.error))
            context = context_response.payload["context"]
            execution = self._issue_worker_authorization_execution_locked(
                request=request,
                status=status,
                context=context,
                now=now,
            )
            return DaemonResponse(
                ok=True,
                payload=self._worker_authorization_payload_locked(
                    request=request,
                    ticket=execution["ticket"],
                    src_buffer=context["src_buffer"],
                    dst_buffer=context["dst_buffer"],
                    lease=lease,
                    related_leases=context["related_leases"],
                    session=context["session"],
                    planning_relays=context["planning_relays"],
                    relay_eligibility=context["relay_eligibility"],
                    profile_entry=context["profile_entry"],
                    staging_records=execution["staging_records"],
                    now=now,
                ),
            )

    def _worker_authorization_context_locked(
        self,
        request: WorkerTransferAuthorizationRequest,
        *,
        lease: LeaseToken,
        peer_identity: PeerIdentity | None,
        now: float,
    ) -> DaemonResponse:
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
        related_leases_response = self._authorize_worker_related_leases_locked(
            request,
            primary_lease=lease,
            now=now,
        )
        if not related_leases_response.ok:
            return DaemonResponse(ok=False, error=str(related_leases_response.error))
        related_leases = related_leases_response.payload["related_leases"]
        ranges_response = self._worker_authorized_ranges_locked(
            request=request,
            plan=plan,
            related_leases=related_leases,
        )
        if not ranges_response.ok:
            return ranges_response
        buffer_response = self._worker_authorization_buffers_locked(
            request=request,
            lease=lease,
            peer_identity=peer_identity,
        )
        if not buffer_response.ok:
            return buffer_response
        session = self._sessions.get(request.session_id)
        if session is None:
            return DaemonResponse(ok=False, error="transfer session is unavailable")
        relay_eligibility = self._relay_eligibility_for_session_locked(session)
        planning_relays = tuple(
            int(item["relay_gpu"]) for item in relay_eligibility["eligible_relays"]
        )
        profile_entry = self._trusted_profile_entry_locked(
            target_gpu=session.target_gpu,
            planning_relays=planning_relays,
            fallback_relays=tuple(session.relay_gpus),
        )
        return DaemonResponse(
            ok=True,
            payload={
                "context": {
                    "reservation": reservation,
                    "plan": plan,
                    "related_leases": related_leases,
                    "authorized_ranges": ranges_response.payload["authorized_ranges"],
                    "src_buffer": buffer_response.payload["src_buffer"],
                    "dst_buffer": buffer_response.payload["dst_buffer"],
                    "session": session,
                    "relay_eligibility": relay_eligibility,
                    "planning_relays": planning_relays,
                    "profile_entry": profile_entry,
                }
            },
        )

    def _completion_evidence_with_block_cleanup_locked(
        self,
        transfer_id: str,
        completion_evidence: Mapping[str, object] | None,
    ) -> dict[str, object] | None:
        if completion_evidence is None:
            return None
        records = self._block_runtime_records.get(str(transfer_id))
        if not records:
            return dict(completion_evidence)
        updated = dict(completion_evidence)
        cleanup = dict(updated.get("cleanup") or {})
        cleanup["block_runtime_cleanup"] = daemon_block_runtime.block_cleanup_summary(
            records
        )
        updated["cleanup"] = cleanup
        return updated

    def _worker_authorized_ranges_locked(
        self,
        *,
        request: WorkerTransferAuthorizationRequest,
        plan: Mapping[str, object],
        related_leases: Iterable[LeaseToken],
    ) -> DaemonResponse:
        related_leases_tuple = tuple(related_leases)
        try:
            authorized_relay_ranges = planning_helpers.relay_ranges_from_plan(
                plan,
                relay_gpu=tuple(item.relay_gpu for item in related_leases_tuple),
                direction=request.direction,
            )
        except ValueError as exc:
            return DaemonResponse(ok=False, error=str(exc))
        if request.ranges and request.ranges != authorized_relay_ranges:
            return DaemonResponse(
                ok=False,
                error="worker ranges do not match daemon relay plan",
            )
        requested_bytes = sum(item["bytes"] for item in authorized_relay_ranges)
        reservation_bytes = sum(
            int(self._reservations[item.lease_id].bytes)
            for item in related_leases_tuple
            if item.lease_id in self._reservations
        )
        if requested_bytes > reservation_bytes:
            return DaemonResponse(
                ok=False,
                error="authorization exceeds reservation bytes",
            )
        return DaemonResponse(
            ok=True,
            payload={"authorized_ranges": authorized_relay_ranges},
        )

    def _worker_authorization_buffers_locked(
        self,
        *,
        request: WorkerTransferAuthorizationRequest,
        lease: LeaseToken,
        peer_identity: PeerIdentity | None,
    ) -> DaemonResponse:
        required_buffers = (request.src_buffer_id, request.dst_buffer_id)
        if required_buffers != lease.buffer_ids:
            return DaemonResponse(ok=False, error="lease buffer mismatch")
        src_buffer = self._buffers.get(request.src_buffer_id)
        dst_buffer = self._buffers.get(request.dst_buffer_id)
        if src_buffer is None or dst_buffer is None:
            return DaemonResponse(ok=False, error="unknown buffer")
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
        return DaemonResponse(
            ok=True,
            payload={
                "src_buffer": src_buffer,
                "dst_buffer": dst_buffer,
            },
        )

    def _issue_worker_authorization_execution_locked(
        self,
        *,
        request: WorkerTransferAuthorizationRequest,
        status: TransferStatus,
        context: Mapping[str, object],
        now: float,
    ) -> dict[str, object]:
        authorization = WorkerTransferAuthorization(
            transfer_id=request.transfer_id,
            lease_id=request.lease_id,
            session_id=request.session_id,
            job_id=request.job_id,
            src_buffer=context["src_buffer"],
            dst_buffer=context["dst_buffer"],
            direction=request.direction,
            ranges=context["authorized_ranges"],
            relay_gpu=context["reservation"].relay_gpu,
            plan=context["plan"],
        )
        related_leases = context["related_leases"]
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
            plan=context["plan"],
            now=now,
        )
        self._append_worker_authorization_audit_records_locked(
            request=request,
            status=status,
            ticket=ticket,
            related_leases=related_leases,
            staging_records=staging_records,
            now=now,
        )
        return {
            "ticket": ticket,
            "staging_records": staging_records,
        }

    def _append_worker_authorization_audit_records_locked(
        self,
        *,
        request: WorkerTransferAuthorizationRequest,
        status: TransferStatus,
        ticket: ExecutionTicket,
        related_leases: Iterable[LeaseToken],
        staging_records: Mapping[str, Mapping[str, object]],
        now: float,
    ) -> None:
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

    def _worker_authorization_payload_locked(
        self,
        *,
        request: WorkerTransferAuthorizationRequest,
        ticket: ExecutionTicket,
        src_buffer: BufferRegistration,
        dst_buffer: BufferRegistration,
        lease: LeaseToken,
        related_leases: Iterable[LeaseToken],
        session: Session,
        planning_relays: tuple[int, ...],
        relay_eligibility: Mapping[str, object],
        profile_entry: Mapping[str, object] | None,
        staging_records: Mapping[str, Mapping[str, object]],
        now: float,
    ) -> dict[str, object]:
        related_leases_tuple = tuple(related_leases)
        decision = self._scheduling_decisions.get(request.transfer_id)
        return {
            "ticket": asdict(ticket),
            "decision": None if decision is None else asdict(decision),
            "src_buffer": asdict(src_buffer),
            "dst_buffer": asdict(dst_buffer),
            "relay_gpu": lease.relay_gpu,
            "relay_gpus": tuple(item.relay_gpu for item in related_leases_tuple),
            "lease_id": request.lease_id,
            "lease_ids": tuple(item.lease_id for item in related_leases_tuple),
            "transfer_id": request.transfer_id,
            "authorized_at": now,
            "plan_generation": self._transfer_plan_generations.get(
                request.transfer_id,
                0,
            ),
            "planning": {
                "target_gpu": session.target_gpu,
                "profile_key": self._profile_key(session.target_gpu, planning_relays),
                "profile_entry": None if profile_entry is None else dict(profile_entry),
                "relay_eligibility": dict(relay_eligibility),
            },
            "staging_record": dict(staging_records[lease.lease_id]),
            "staging_records": [
                dict(staging_records[item.lease_id])
                for item in related_leases_tuple
            ],
        }

    def _authorize_worker_transfer_preflight_locked(
        self,
        request: WorkerTransferAuthorizationRequest,
        *,
        peer_identity: PeerIdentity | None,
        now: float,
    ) -> DaemonResponse:
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
        cleanup_payload = self._authorization_cleanup_payload_locked(
            transfer_id=request.transfer_id,
            job_id=request.job_id,
            session_id=request.session_id,
            lease_id=request.lease_id,
        )
        return DaemonResponse(
            ok=True,
            payload={
                "status": status,
                "lease": lease,
                "cleanup_payload": cleanup_payload,
            },
        )

    def _authorize_worker_related_leases_locked(
        self,
        request: WorkerTransferAuthorizationRequest,
        *,
        primary_lease: LeaseToken,
        now: float,
    ) -> DaemonResponse:
        related_leases = self._leases_for_worker_plan_locked(
            request,
            primary_lease=primary_lease,
        )
        if len(related_leases) <= 1:
            return DaemonResponse(
                ok=True,
                payload={"related_leases": (primary_lease,)},
            )
        related_lease_ids = {item.lease_id for item in related_leases}
        admission_error = self._validate_transfer_admission_locked(
            request.transfer_id,
            lease_id=None,
            now=now,
        )
        if admission_error is not None:
            return DaemonResponse(ok=False, error=admission_error)
        for related_lease in related_leases:
            if related_lease.lease_id == primary_lease.lease_id:
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
            if related_lease.buffer_ids != primary_lease.buffer_ids:
                return DaemonResponse(ok=False, error="lease buffer mismatch")
        admission = self._transfer_admissions.get(request.transfer_id, {})
        admission_lease_ids = set(
            str(item) for item in admission.get("lease_ids", ()) or ()
        )
        if admission_lease_ids and admission_lease_ids != related_lease_ids:
            return DaemonResponse(ok=False, error="worker lease set mismatch")
        return DaemonResponse(
            ok=True,
            payload={"related_leases": related_leases},
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
            self._purge_stale_profiles_locked(now)
            self._refresh_admission_state_locked(now=now)
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
                    return planning_helpers.topology_unavailable_response(
                        _TOPOLOGY_UNAVAILABLE_ERROR,
                    )
                return DaemonResponse(ok=False, error=str(exc))
            transfer_id, status, reservations = self._register_planned_transfer_state_locked(
                decision,
                session=session,
                intent_id=intent_id,
                buffer_ids=buffer_ids_tuple,
                job_id=plan_job_id,
                total_bytes=int(total_bytes),
                chunk_bytes=int(chunk_bytes),
                ranges=normalized_ranges,
                direction=direction,
                mode=mode,
                topology_snapshot_id=topology_snapshot_id,
                workload_kind=str(workload_kind),
                priority=int(priority),
                allow_delayed=allow_delayed_admission,
                peer_identity=peer_identity,
                now=now,
            )
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
            entry = daemon_profiles.cached_profile(self._profile_cache, key)
            if entry is not None and not self._profile_matches_current_topology_locked(
                entry,
                target_gpu=int(target_gpu),
                relay_gpus=relay_gpus,
            ):
                daemon_profiles.invalidate_cached_profile(self._profile_cache, key)
                entry = None
            return DaemonResponse(
                ok=True,
                payload={"profile": entry},
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
        key = self._profile_key(target, relays)
        with self._lock:
            self._purge_stale_profiles_locked(time.time())
            try:
                topology_binding = self._profile_topology_binding_locked(
                    target_gpu=target,
                    relay_gpus=relays,
                )
                entry = daemon_profiles.profile_entry(
                    target_gpu=target,
                    relay_gpus=relays,
                    profile=profile,
                    profile_bytes=int(profile_bytes),
                    updated_at=float(time.time() if updated_at is None else updated_at),
                    topology_binding=topology_binding,
                )
            except (TypeError, ValueError) as exc:
                return DaemonResponse(ok=False, error=str(exc))
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
            admission_refresh = self._refresh_admission_state_locked(now=checked_at)
            return [str(item) for item in admission_refresh["expired_leases"]]

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
            self._release_staging_only_reservation_locked(
                reservation_id=reservation_id,
                reservation_key=reservation_key,
                final_state=final_state,
                cleanup_reason=cleanup_reason,
            )
            return None
        transfer_id = self._reservation_transfers.get(reservation_key)
        lease = self._lease_tokens.get(reservation_key)
        staging_record = self._staging_records.get(reservation_key)
        self._archive_active_reservation_cleanup_locked(
            reservation_key=reservation_key,
            lease=lease,
            transfer_id=transfer_id,
            cleanup_reason=cleanup_reason,
        )
        if cleanup_reason is not None:
            self._append_reservation_cleanup_audit_locked(
                transfer_id=transfer_id,
                reservation=reservation,
                lease=lease,
                staging_record=staging_record,
                final_state=final_state,
                cleanup_reason=cleanup_reason,
                cleanup_target_id=reservation_key,
            )
        transfer_id = self._drop_active_reservation_state_locked(
            reservation_key=reservation_key,
            reservation=reservation,
        )
        if transfer_id is not None and mark_terminal:
            self._finalize_transfer_after_reservation_release_locked(
                transfer_id=transfer_id,
                final_state=final_state,
                cleanup_reason=cleanup_reason,
            )
        if cleanup_reason is not None:
            self._record_system_reservation_cleanup_event(
                reservation_id=reservation_id,
                cleanup_reason=cleanup_reason,
            )
        return reservation

    def _release_staging_only_reservation_locked(
        self,
        *,
        reservation_id: str,
        reservation_key: str,
        final_state: TransferStatusState,
        cleanup_reason: str | None,
    ) -> None:
        staging_record = self._staging_records.pop(reservation_key, None)
        if staging_record is None or cleanup_reason is None:
            return
        transfer_id = staging_record.get("transfer_id")
        self._archive_cleanup_target_locked(
            target_kind="reservation",
            target_id=reservation_key,
            peer_identity=self._staging_cleanup_peer_identity_locked(staging_record),
            reason=cleanup_reason,
            transfer_ids=(() if transfer_id is None else (str(transfer_id),)),
        )
        self._append_audit_record_locked(
            event_type="cleanup",
            staging_record=staging_record,
            state=final_state,
            reason=cleanup_reason,
            failure_reason=self._cleanup_failure_reason(final_state, cleanup_reason),
            cleanup_kind="reservation",
            cleanup_target_id=reservation_key,
        )
        self._record_system_reservation_cleanup_event(
            reservation_id=reservation_id,
            cleanup_reason=cleanup_reason,
        )

    def _staging_cleanup_peer_identity_locked(
        self,
        staging_record: Mapping[str, object],
    ) -> PeerIdentity | None:
        job_id = staging_record.get("job_id")
        if job_id is not None:
            archived_peer = self._job_peer_identities.get(str(job_id))
            if archived_peer is not None:
                return archived_peer
        for buffer_id in staging_record.get("buffer_ids", ()) or ():
            buffer = self._buffers.get(str(buffer_id))
            if buffer is None:
                continue
            archived_peer = self._job_peer_identities.get(buffer.job_id)
            if archived_peer is not None:
                return archived_peer
        return None

    def _archive_active_reservation_cleanup_locked(
        self,
        *,
        reservation_key: str,
        lease: LeaseToken | None,
        transfer_id: str | None,
        cleanup_reason: str | None,
    ) -> None:
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

    def _append_reservation_cleanup_audit_locked(
        self,
        *,
        transfer_id: str | None,
        reservation: TransferReservation,
        lease: LeaseToken | None,
        staging_record: Mapping[str, object] | None,
        final_state: TransferStatusState,
        cleanup_reason: str,
        cleanup_target_id: str,
    ) -> None:
        self._append_audit_record_locked(
            event_type="cleanup",
            transfer_id=transfer_id,
            reservation=reservation,
            lease=lease,
            staging_record=staging_record,
            state=final_state,
            reason=cleanup_reason,
            failure_reason=self._cleanup_failure_reason(final_state, cleanup_reason),
            cleanup_kind="reservation",
            cleanup_target_id=cleanup_target_id,
        )

    def _drop_active_reservation_state_locked(
        self,
        *,
        reservation_key: str,
        reservation: TransferReservation,
    ) -> str | None:
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
        return transfer_id

    def _finalize_transfer_after_reservation_release_locked(
        self,
        *,
        transfer_id: str,
        final_state: TransferStatusState,
        cleanup_reason: str | None,
    ) -> None:
        self._mark_transfer_terminal_if_unblocked_locked(
            transfer_id,
            final_state,
            error=self._cleanup_failure_reason(final_state, cleanup_reason),
        )
        status_after = self._transfer_statuses.get(transfer_id)
        if (
            status_after is None
            or status_after.state not in _TERMINAL_TRANSFER_STATES
            or self._transfer_has_reservations_locked(transfer_id)
        ):
            return
        if status_after.state is TransferStatusState.COMPLETE:
            self._retire_completed_transfer_lease_state_locked(
                transfer_id,
                reason=cleanup_reason,
            )
        else:
            self._retire_transfer_runtime_state_locked(transfer_id)

    def _cleanup_failure_reason(
        self,
        final_state: TransferStatusState,
        cleanup_reason: str | None,
    ) -> str | None:
        if final_state in {TransferStatusState.FAILED, TransferStatusState.CANCELED}:
            return cleanup_reason
        return None

    def _record_system_reservation_cleanup_event(
        self,
        *,
        reservation_id: str,
        cleanup_reason: str,
    ) -> None:
        self._system_cleanup_events.append(
            CleanupRequest(
                target_kind="reservation",
                target_id=reservation_id,
                reason=cleanup_reason,
                force=True,
            )
        )

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

    def _retire_terminal_transfer_without_reservations_locked(
        self,
        transfer_id: str,
        *,
        reason: str | None = None,
    ) -> dict[str, int]:
        normalized_transfer_id = str(transfer_id)
        status = self._transfer_statuses.get(normalized_transfer_id)
        if status is None or status.state not in _TERMINAL_TRANSFER_STATES:
            return _empty_removed_summary()
        if self._transfer_has_reservations_locked(normalized_transfer_id):
            return _empty_removed_summary()
        removed = _empty_removed_summary()
        transfer_was_live = (
            normalized_transfer_id in self._transfer_queue_records
            or normalized_transfer_id in self._transfer_queue
        )
        if status.state is TransferStatusState.COMPLETE:
            self._retire_completed_transfer_lease_state_locked(
                normalized_transfer_id,
                reason=reason,
            )
        else:
            self._retire_transfer_runtime_state_locked(normalized_transfer_id)
        if transfer_was_live:
            removed["transfers"] = int(removed["transfers"]) + 1
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
        profile_entry = self._trusted_profile_entry_locked(
            target_gpu=session.target_gpu,
            planning_relays=planning_relays,
            fallback_relays=tuple(session.relay_gpus),
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
        job_id: str | None,
        total_bytes: int,
        workload_kind: str,
        priority: int,
        allow_delayed: bool,
        enforce_fairness: bool = True,
        now: float,
    ) -> dict[str, object]:
        leases = scheduling_decision_leases(decision)
        if not leases:
            fallback_reason = decision.fallback_reason
            return self._admission_record_locked(
                state=_ADMISSION_ADMITTED,
                reason=fallback_reason or "direct_or_fallback_plan",
                decision=decision,
                session=session,
                job_id=job_id,
                total_bytes=total_bytes,
                workload_kind=workload_kind,
                priority=priority,
                leases=(),
                requested_chunks=0,
                fairness=None,
                timestamp_field="admitted_at",
                now=now,
            )
        requested_chunks = sum(lease.chunk_limit for lease in leases)
        reason = self._relay_admission_blocked_reason_locked(
            session=session,
            leases=leases,
            job_id=job_id,
            total_bytes=int(total_bytes),
            workload_kind=str(workload_kind),
            priority=int(priority),
            enforce_fairness=bool(enforce_fairness),
            now=now,
        )
        fairness = self._relay_admission_fairness_record_locked(
            job_id=job_id,
            total_bytes=int(total_bytes),
            workload_kind=str(workload_kind),
            priority=int(priority),
            enforce_fairness=bool(enforce_fairness),
            now=now,
        )
        if reason is None:
            return self._admission_record_locked(
                state=_ADMISSION_ADMITTED,
                reason="relay_resources_available",
                decision=decision,
                session=session,
                job_id=job_id,
                total_bytes=total_bytes,
                workload_kind=workload_kind,
                priority=priority,
                leases=leases,
                requested_chunks=requested_chunks,
                fairness=fairness,
                timestamp_field="admitted_at",
                now=now,
            )
        if allow_delayed:
            return self._admission_record_locked(
                state=_ADMISSION_DELAYED,
                reason=reason,
                decision=decision,
                session=session,
                job_id=job_id,
                total_bytes=total_bytes,
                workload_kind=workload_kind,
                priority=priority,
                leases=leases,
                requested_chunks=requested_chunks,
                fairness=fairness,
                timestamp_field="delayed_at",
                now=now,
            )
        return self._admission_record_locked(
            state=_ADMISSION_ADMITTED,
            reason="scheduler_fallback_or_rejection",
            decision=decision,
            session=session,
            job_id=job_id,
            total_bytes=total_bytes,
            workload_kind=workload_kind,
            priority=priority,
            leases=leases,
            requested_chunks=0,
            fairness=fairness,
            timestamp_field="admitted_at",
            now=now,
        )

    def _admission_record_locked(
        self,
        *,
        state: str,
        reason: str | None,
        decision: SchedulingDecision,
        session: Session,
        job_id: str | None,
        total_bytes: int,
        workload_kind: str,
        priority: int,
        leases,
        requested_chunks: int,
        fairness: Mapping[str, object] | None,
        timestamp_field: str,
        now: float,
    ) -> dict[str, object]:
        admission = {
            "state": state,
            "reason": reason,
            "decision_state": str(decision.state.value),
            "fallback_reason": decision.fallback_reason,
            "requested_lease_count": len(leases),
            "requested_chunks": int(requested_chunks),
            "lease_ids": (),
            timestamp_field: float(now),
        }
        if fairness is not None:
            admission["fairness"] = fairness
        admission["multi_tenant_admission"] = (
            self._multi_tenant_admission_evidence_locked(
                state=state,
                reason=str(reason),
                session=session,
                job_id=job_id,
                total_bytes=int(total_bytes),
                workload_kind=str(workload_kind),
                priority=int(priority),
                leases=leases,
                fairness=fairness,
                now=now,
            )
        )
        return admission

    def _relay_admission_blocked_reason_locked(
        self,
        *,
        session: Session,
        leases,
        job_id: str | None,
        total_bytes: int,
        workload_kind: str,
        priority: int,
        enforce_fairness: bool,
        now: float,
    ) -> str | None:
        total_chunks = sum(lease.chunk_limit for lease in leases)
        if session.active_chunks + total_chunks > session.max_inflight_chunks:
            return "session relay admission is delayed by chunk quota"
        runtime_state = self._runtime_resource_state_locked(now=float(now))
        load_view = runtime_view(
            runtime_state=runtime_state,
            job_id=None if job_id is None else str(job_id),
            total_bytes=max(
                int(total_bytes),
                sum(int(lease.bytes_limit) for lease in leases),
            ),
            workload_kind=str(workload_kind),
            priority=int(priority),
        )
        if enforce_fairness:
            fairness_blocked = relay_fairness_admission_blocked_reason(load_view)
            if fairness_blocked is not None:
                return fairness_blocked
        for lease in leases:
            if lease.relay_device not in session.relay_gpus:
                return "relay admission is delayed by session relay ownership"
            quota = self._relay_quotas.get(lease.relay_device)
            if quota is None:
                return "relay admission is delayed by missing relay quota"
            if not quota.can_reserve(lease.chunk_limit):
                return "relay admission is delayed by relay chunk quota"
            runtime_blocked = relay_admission_blocked_reason(
                load_view,
                int(lease.relay_device),
                direction=str(lease.direction),
            )
            if runtime_blocked is not None:
                return f"relay admission is delayed by {runtime_blocked}"
        return None

    def _relay_admission_fairness_record_locked(
        self,
        *,
        job_id: str | None,
        total_bytes: int,
        workload_kind: str,
        priority: int,
        enforce_fairness: bool,
        now: float,
    ) -> dict[str, object]:
        load_view = runtime_view(
            runtime_state=self._runtime_resource_state_locked(now=float(now)),
            job_id=None if job_id is None else str(job_id),
            total_bytes=int(total_bytes),
            workload_kind=str(workload_kind),
            priority=int(priority),
        )
        blocked_reason = (
            relay_fairness_admission_blocked_reason(load_view)
            if enforce_fairness
            else None
        )
        return {
            "source": "daemon_relay_fairness_admission",
            "job_id": None if job_id is None else str(job_id),
            "enforced": bool(enforce_fairness),
            "blocked_reason": blocked_reason,
            "job_weight": float(load_view.job_weight),
            "total_weight": float(load_view.total_weight),
            "request_charge_bytes": float(load_view.request_charge_bytes),
            "current_job_active_bytes": int(load_view.current_job_active_bytes),
            "total_active_bytes": int(load_view.total_active_bytes),
            "current_job_backlog_bytes": int(load_view.current_job_backlog_bytes),
            "total_backlog_bytes": int(load_view.total_backlog_bytes),
            "current_weighted_active_bytes": float(
                load_view.current_weighted_active_bytes
            ),
            "projected_weighted_active_bytes": float(
                load_view.projected_weighted_active_bytes
            ),
            "average_weighted_active_bytes": float(
                load_view.average_weighted_active_bytes
            ),
            "current_weighted_fairness_bytes": float(
                load_view.current_weighted_fairness_bytes
            ),
            "projected_weighted_fairness_bytes": float(
                load_view.projected_weighted_fairness_bytes
            ),
            "average_weighted_fairness_bytes": float(
                load_view.average_weighted_fairness_bytes
            ),
            "fairness_threshold_bytes": float(load_view.fairness_threshold_bytes),
            "resource_pressure": float(load_view.resource_pressure),
            "queued_transfer_count": int(load_view.queued_transfer_count),
            "admitted_transfer_count": int(load_view.admitted_transfer_count),
            "running_transfer_count": int(load_view.running_transfer_count),
            "active_transfer_count": int(load_view.active_transfer_count),
            "delayed_transfer_count": int(load_view.delayed_transfer_count),
        }

    def _multi_tenant_admission_evidence_locked(
        self,
        *,
        state: str,
        reason: str,
        session: Session,
        job_id: str | None,
        total_bytes: int,
        workload_kind: str,
        priority: int,
        leases,
        fairness: Mapping[str, object] | None,
        now: float,
    ) -> dict[str, object]:
        normalized_job_id = str(job_id or session.session_id)
        runtime_state = self._runtime_resource_state_locked(now=float(now))
        runtime_summary = (
            runtime_state.get("summary", {})
            if isinstance(runtime_state.get("summary"), Mapping)
            else {}
        )
        job_runtime_state = runtime_summary.get("job_runtime_state")
        if not isinstance(job_runtime_state, Mapping):
            job_runtime_state = runtime_state.get("job_runtime_state", {})
        current_job_state = {}
        if isinstance(job_runtime_state, Mapping):
            candidate = job_runtime_state.get(normalized_job_id, {})
            if isinstance(candidate, Mapping):
                current_job_state = dict(candidate)
        lease_records = tuple(
            self._admission_requested_lease_record_locked(lease)
            for lease in tuple(leases or ())
        )
        buffer_ownership = tuple(
            self._buffer_ownership_record_locked(buffer_id)
            for buffer_id in sorted(self._buffers)
            if self._buffers[buffer_id].job_id == normalized_job_id
        )
        return {
            "source": "daemon_multi_tenant_fairness_admission",
            "state": str(state),
            "reason": str(reason),
            "job_id": normalized_job_id,
            "session_id": session.session_id,
            "workload_kind": str(workload_kind),
            "priority": int(priority),
            "request_bytes": int(total_bytes),
            "session_active_chunks": int(session.active_chunks),
            "session_max_inflight_chunks": int(session.max_inflight_chunks),
            "session_relay_gpus": tuple(int(gpu) for gpu in session.relay_gpus),
            "queued_transfer_count": int(
                runtime_summary.get("queued_transfer_count", 0) or 0
            ),
            "admitted_transfer_count": int(
                runtime_summary.get("admitted_transfer_count", 0) or 0
            ),
            "delayed_transfer_count": int(
                runtime_summary.get("delayed_transfer_count", 0) or 0
            ),
            "running_transfer_count": int(
                runtime_summary.get("running_transfer_count", 0) or 0
            ),
            "active_transfer_count": int(
                runtime_summary.get("active_transfer_count", 0) or 0
            ),
            "active_lease_count": int(runtime_summary.get("active_lease_count", 0) or 0),
            "active_reservation_count": int(
                runtime_summary.get("active_reservation_count", 0) or 0
            ),
            "current_job_runtime_state": current_job_state,
            "requested_leases": lease_records,
            "buffer_ownership": buffer_ownership,
            "fairness": {} if fairness is None else dict(fairness),
            "captured_at": float(now),
        }

    def _admission_requested_lease_record_locked(self, lease) -> dict[str, object]:
        relay_device = int(lease.relay_device)
        quota = self._relay_quotas.get(relay_device)
        active_lease_ids = tuple(
            str(lease_id)
            for lease_id, token in sorted(self._lease_tokens.items())
            if int(token.relay_gpu) == relay_device
            and lease_id in self._reservations
        )
        return {
            "relay_device": relay_device,
            "job_id": None if lease.job_id is None else str(lease.job_id),
            "chunk_limit": int(lease.chunk_limit),
            "bytes_limit": int(lease.bytes_limit),
            "active_lease_ids": active_lease_ids,
            "quota_active_chunks": (
                None if quota is None else int(quota.active_chunks)
            ),
            "quota_max_inflight_chunks": (
                None if quota is None else int(quota.max_inflight_chunks)
            ),
            "quota_available_chunks": (
                None
                if quota is None
                else max(0, int(quota.max_inflight_chunks) - int(quota.active_chunks))
            ),
        }

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

    def _register_planned_transfer_state_locked(
        self,
        decision: SchedulingDecision,
        *,
        session: Session,
        intent_id: str | None,
        buffer_ids: tuple[str, ...],
        job_id: str | None,
        total_bytes: int,
        chunk_bytes: int,
        ranges: tuple[dict[str, int], ...] | None,
        direction: str,
        mode: str,
        topology_snapshot_id: str | None,
        workload_kind: str,
        priority: int,
        allow_delayed: bool,
        peer_identity: PeerIdentity | None,
        now: float,
    ) -> tuple[str, TransferStatus, list[TransferReservation]]:
        transfer_id = str(uuid.uuid4())
        self._transfer_plan_generations[transfer_id] = 1
        admission = self._admission_for_decision_locked(
            decision,
            session=session,
            job_id=job_id,
            total_bytes=int(total_bytes),
            workload_kind=str(workload_kind),
            priority=int(priority),
            allow_delayed=allow_delayed,
            now=now,
        )
        reservations = self._admitted_transfer_reservations_locked(
            admission=admission,
            session=session,
            decision=decision,
            transfer_id=transfer_id,
            buffer_ids=buffer_ids,
        )
        status = self._planned_transfer_status_locked(
            transfer_id=transfer_id,
            session=session,
            job_id=job_id,
            total_bytes=total_bytes,
        )
        self._register_transfer_owner_peer_locked(
            transfer_id=transfer_id,
            status=status,
            peer_identity=peer_identity,
        )
        self._register_transfer_plan_contract_locked(
            transfer_id=transfer_id,
            decision=decision,
            session=session,
            job_id=job_id,
            total_bytes=total_bytes,
            chunk_bytes=chunk_bytes,
            mode=mode,
            direction=direction,
            buffer_ids=buffer_ids,
            ranges=ranges,
            intent_id=intent_id,
            topology_snapshot_id=topology_snapshot_id,
            workload_kind=workload_kind,
            priority=priority,
            admission=admission,
            now=now,
        )
        self._register_block_runtime_records_locked(
            transfer_id=transfer_id,
            decision=decision,
        )
        self._record_planned_transfer_locked(
            transfer_id=transfer_id,
            status=status,
            intent_id=intent_id,
            buffer_ids=buffer_ids,
            total_bytes=total_bytes,
            chunk_bytes=chunk_bytes,
            ranges=ranges,
            direction=direction,
            decision=decision,
            now=now,
        )
        self._attach_admission_priority_evidence_locked(transfer_id, now=now)
        self._transfer_buffer_snapshots[transfer_id] = self._buffer_snapshots_for_ids_locked(
            buffer_ids
        )
        self._touch_session_locked(session.session_id, now)
        self._issue_direct_plan_ticket_if_needed_locked(
            transfer_id=transfer_id,
            decision=decision,
            buffer_ids=buffer_ids,
            reservations=reservations,
            now=now,
        )
        return transfer_id, status, reservations

    def _admitted_transfer_reservations_locked(
        self,
        *,
        admission: dict[str, object],
        session: Session,
        decision: SchedulingDecision,
        transfer_id: str,
        buffer_ids: tuple[str, ...],
    ) -> list[TransferReservation]:
        if admission["state"] != _ADMISSION_ADMITTED:
            return []
        reservations = self._commit_scheduler_leases_locked(
            session,
            decision,
            transfer_id=transfer_id,
            buffer_ids=buffer_ids,
        )
        admission["lease_ids"] = tuple(
            reservation.reservation_id for reservation in reservations
        )
        return reservations

    def _planned_transfer_status_locked(
        self,
        *,
        transfer_id: str,
        session: Session,
        job_id: str | None,
        total_bytes: int,
    ) -> TransferStatus:
        status = TransferStatus(
            transfer_id=str(transfer_id),
            job_id=str(job_id or session.session_id),
            state=TransferStatusState.SUBMITTED,
            bytes_total=int(total_bytes),
            bytes_completed=0,
            session_id=session.session_id,
        )
        self._transfer_statuses[str(transfer_id)] = status
        return status

    def _register_block_runtime_records_locked(
        self,
        *,
        transfer_id: str,
        decision: SchedulingDecision,
    ) -> None:
        normalized_transfer_id = str(transfer_id)
        records = daemon_block_runtime.runtime_records_for_block_plan(
            transfer_id=normalized_transfer_id,
            plan=dict(decision.plan),
            plan_generation=self._transfer_plan_generations.get(
                normalized_transfer_id,
                0,
            ),
            lease_ids_by_relay=self._block_runtime_lease_ids_by_relay_locked(
                normalized_transfer_id
            ),
        )
        if records:
            self._block_runtime_records[normalized_transfer_id] = tuple(
                record.as_dict() for record in records
            )
            self._runtime_state_version += 1

    def _block_runtime_lease_ids_by_relay_locked(
        self,
        transfer_id: str,
    ) -> dict[int, tuple[str, ...]]:
        relay_leases: dict[int, list[str]] = {}
        for lease_id, mapped_transfer_id in sorted(self._reservation_transfers.items()):
            if str(mapped_transfer_id) != str(transfer_id):
                continue
            lease = self._lease_tokens.get(lease_id)
            if lease is None:
                continue
            relay_leases.setdefault(int(lease.relay_gpu), []).append(str(lease_id))
        return {
            relay_gpu: tuple(lease_ids)
            for relay_gpu, lease_ids in sorted(relay_leases.items())
        }

    def _register_transfer_owner_peer_locked(
        self,
        *,
        transfer_id: str,
        status: TransferStatus,
        peer_identity: PeerIdentity | None,
    ) -> None:
        transfer_peer_identity = self._transfer_peer_identity_for_owner_locked(
            job_id=status.job_id,
            session_id=status.session_id,
            peer_identity=peer_identity,
        )
        if transfer_peer_identity is not None:
            self._transfer_peer_identities[str(transfer_id)] = transfer_peer_identity

    def _register_transfer_plan_contract_locked(
        self,
        *,
        transfer_id: str,
        decision: SchedulingDecision,
        session: Session,
        job_id: str | None,
        total_bytes: int,
        chunk_bytes: int,
        mode: str,
        direction: str,
        buffer_ids: tuple[str, ...],
        ranges: tuple[dict[str, int], ...] | None,
        intent_id: str | None,
        topology_snapshot_id: str | None,
        workload_kind: str,
        priority: int,
        admission: dict[str, object],
        now: float,
    ) -> None:
        normalized_transfer_id = str(transfer_id)
        self._transfer_plans[normalized_transfer_id] = dict(decision.plan)
        self._scheduling_decisions[normalized_transfer_id] = decision
        self._transfer_plan_requests[normalized_transfer_id] = {
            "session_id": session.session_id,
            "total_bytes": int(total_bytes),
            "chunk_bytes": int(chunk_bytes),
            "mode": str(mode),
            "direction": str(direction).lower(),
            "job_id": None if job_id is None else str(job_id),
            "buffer_ids": buffer_ids,
            "ranges": ranges,
            "intent_id": None if intent_id is None else str(intent_id),
            "topology_snapshot_id": topology_snapshot_id,
            "workload_kind": str(workload_kind),
            "priority": int(priority),
        }
        self._transfer_plan_expirations[normalized_transfer_id] = (
            self._plan_expires_at_for_decision(decision, now=now)
        )
        self._transfer_admissions[normalized_transfer_id] = {
            **admission,
            "plan_generation": self._transfer_plan_generations[normalized_transfer_id],
            "plan_expires_at": self._transfer_plan_expirations[normalized_transfer_id],
        }

    def _attach_admission_priority_evidence_locked(
        self,
        transfer_id: str,
        *,
        now: float,
    ) -> None:
        normalized_transfer_id = str(transfer_id)
        admission = self._transfer_admissions[normalized_transfer_id]
        admission_order = _admission_priority_record(
            transfer_id=normalized_transfer_id,
            admission=admission,
            queue_record=self._transfer_queue_records.get(normalized_transfer_id, {}),
            runtime_state=self._runtime_resource_state_locked(now=float(now)),
            now=float(now),
        )
        self._transfer_admissions[normalized_transfer_id] = _admission_with_priority_evidence(
            {
                **admission,
                "priority_order": admission_order,
            },
            admission_order,
        )
        self._refresh_transfer_queue_record_locked(normalized_transfer_id, now=now)

    def _issue_direct_plan_ticket_if_needed_locked(
        self,
        *,
        transfer_id: str,
        decision: SchedulingDecision,
        buffer_ids: tuple[str, ...],
        reservations: list[TransferReservation],
        now: float,
    ) -> None:
        if reservations:
            return
        if len(buffer_ids) < 2:
            return
        if not planning_helpers.decision_is_direct_only(decision):
            return
        ticket = self._execution_ticket_for_plan_locked(
            transfer_id=str(transfer_id),
            decision=decision,
            source_buffer_id=buffer_ids[0],
            destination_buffer_id=buffer_ids[1],
            now=now,
            lease_ids=(),
        )
        self._execution_tickets[ticket.ticket_id] = ticket
        self._transfer_tickets[str(transfer_id)] = ticket.ticket_id

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
        profile_entry = self._trusted_profile_entry_locked(
            target_gpu=session.target_gpu,
            planning_relays=planning_relays,
            fallback_relays=tuple(session.relay_gpus),
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

    def _buffer_snapshots_for_ids_locked(
        self,
        buffer_ids: Iterable[str],
    ) -> dict[str, dict[str, object]]:
        snapshots: dict[str, dict[str, object]] = {}
        normalized = tuple(str(buffer_id) for buffer_id in buffer_ids)
        if len(normalized) >= 1:
            source = self._buffers.get(normalized[0])
            if isinstance(source, BufferRegistration):
                snapshots["source"] = planning_helpers.buffer_snapshot_record(source)
                snapshots["source"]["daemon_buffer_ownership"] = (
                    self._buffer_ownership_record_locked(normalized[0])
                )
        if len(normalized) >= 2:
            destination = self._buffers.get(normalized[1])
            if isinstance(destination, BufferRegistration):
                snapshots["destination"] = planning_helpers.buffer_snapshot_record(destination)
                snapshots["destination"]["daemon_buffer_ownership"] = (
                    self._buffer_ownership_record_locked(normalized[1])
                )
        return snapshots

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

    def _refresh_admission_state_locked(
        self,
        *,
        now: float,
        reap_expired: bool = True,
    ) -> dict[str, object]:
        expired_leases = (
            tuple(self._reap_expired_leases_locked(float(now)))
            if reap_expired
            else ()
        )
        promoted = self._promote_delayed_transfers_locked(now=float(now))
        admitted_transfer_ids: list[str] = []
        delayed_transfer_ids: list[str] = []
        terminal_transfer_ids: list[str] = []
        for transfer_id, admission in sorted(self._transfer_admissions.items()):
            state = str(admission.get("state", ""))
            if state == _ADMISSION_ADMITTED:
                admitted_transfer_ids.append(str(transfer_id))
            elif state == _ADMISSION_DELAYED:
                delayed_transfer_ids.append(str(transfer_id))
            elif state in {_ADMISSION_CANCELED, _ADMISSION_FAILED, _ADMISSION_EXPIRED}:
                terminal_transfer_ids.append(str(transfer_id))
            self._refresh_transfer_queue_record_locked(str(transfer_id), now=float(now))
        delayed_priority_queue = _ordered_delayed_admission_records(
            transfer_admissions=self._transfer_admissions,
            transfer_queue_records=self._transfer_queue_records,
            runtime_state=self._runtime_resource_state_locked(now=float(now)),
            now=float(now),
        )
        return {
            "refreshed_at": float(now),
            "expired_leases": expired_leases,
            "promoted_transfers": promoted,
            "admitted_transfer_ids": tuple(admitted_transfer_ids),
            "delayed_transfer_ids": tuple(delayed_transfer_ids),
            "delayed_priority_queue": delayed_priority_queue,
            "terminal_transfer_ids": tuple(terminal_transfer_ids),
        }

    def _promote_delayed_transfers_locked(
        self,
        *,
        now: float,
    ) -> tuple[dict[str, object], ...]:
        promoted: list[dict[str, object]] = []
        runtime_state = self._runtime_resource_state_locked(now=float(now))
        delayed_transfer_ids = tuple(
            item["transfer_id"]
            for item in _ordered_delayed_admission_records(
                transfer_admissions=self._transfer_admissions,
                transfer_queue_records=self._transfer_queue_records,
                runtime_state=runtime_state,
                now=float(now),
            )
        )
        for transfer_id in delayed_transfer_ids:
            status = self._transfer_statuses.get(transfer_id)
            if status is None or status.state in _TERMINAL_TRANSFER_STATES:
                continue
            request = self._transfer_plan_requests.get(transfer_id)
            if request is None:
                continue
            promoted_record = self._promote_delayed_transfer_locked(
                transfer_id=transfer_id,
                status=status,
                request=request,
                runtime_state=runtime_state,
                now=now,
            )
            if promoted_record is not None:
                promoted.append(promoted_record)
        return tuple(promoted)

    def _promote_delayed_transfer_locked(
        self,
        *,
        transfer_id: str,
        status: TransferStatus,
        request: Mapping[str, object],
        runtime_state: Mapping[str, object],
        now: float,
    ) -> dict[str, object] | None:
        admission_order = _admission_priority_record(
            transfer_id=transfer_id,
            admission=self._transfer_admissions.get(transfer_id, {}),
            queue_record=self._transfer_queue_records.get(transfer_id, {}),
            runtime_state=runtime_state,
            now=float(now),
        )
        try:
            session, decision, buffer_ids_tuple = (
                self._promotion_scheduler_decision_locked(
                    transfer_id=transfer_id,
                    request=request,
                    now=now,
                )
            )
        except ValueError as exc:
            self._record_delayed_promotion_failure_locked(
                transfer_id=transfer_id,
                error=str(exc),
                admission_order=admission_order,
                now=now,
            )
            return None
        admission = self._admission_for_decision_locked(
            decision,
            session=session,
            job_id=request.get("job_id"),
            total_bytes=int(request["total_bytes"]),
            workload_kind=str(request.get("workload_kind", "generic")),
            priority=int(request.get("priority", 0) or 0),
            allow_delayed=True,
            enforce_fairness=False,
            now=now,
        )
        if admission["state"] != _ADMISSION_ADMITTED:
            self._record_delayed_promotion_check_locked(
                transfer_id=transfer_id,
                admission=admission,
                admission_order=admission_order,
                now=now,
            )
            return None
        return self._commit_delayed_promotion_locked(
            transfer_id=transfer_id,
            status=status,
            session=session,
            decision=decision,
            buffer_ids=buffer_ids_tuple,
            request=request,
            admission=admission,
            admission_order=admission_order,
            now=now,
        )

    def _promotion_scheduler_decision_locked(
        self,
        *,
        transfer_id: str,
        request: Mapping[str, object],
        now: float,
    ) -> tuple[Session, SchedulingDecision, tuple[str, ...]]:
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
            job_id=None if request.get("job_id") is None else str(request["job_id"]),
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
        return session, decision, buffer_ids_tuple

    def _record_delayed_promotion_failure_locked(
        self,
        *,
        transfer_id: str,
        error: str,
        admission_order: Mapping[str, object],
        now: float,
    ) -> None:
        admission = dict(self._transfer_admissions.get(transfer_id, {}))
        admission.update(
            {
                "state": _ADMISSION_DELAYED,
                "reason": str(error),
                "promotion_failed_at": float(now),
                "priority_order": admission_order,
            }
        )
        self._transfer_admissions[transfer_id] = admission
        self._refresh_transfer_queue_record_locked(transfer_id, now=now)

    def _record_delayed_promotion_check_locked(
        self,
        *,
        transfer_id: str,
        admission: Mapping[str, object],
        admission_order: Mapping[str, object],
        now: float,
    ) -> None:
        updated = {
            **admission,
            "plan_generation": self._transfer_plan_generations.get(transfer_id, 0),
            "plan_expires_at": self._transfer_plan_expirations.get(transfer_id),
            "promotion_checked_at": float(now),
            "priority_order": admission_order,
        }
        updated = _admission_with_priority_evidence(updated, admission_order)
        self._transfer_admissions[transfer_id] = updated
        self._refresh_transfer_queue_record_locked(transfer_id, now=now)

    def _commit_delayed_promotion_locked(
        self,
        *,
        transfer_id: str,
        status: TransferStatus,
        session: Session,
        decision: SchedulingDecision,
        buffer_ids: tuple[str, ...],
        request: Mapping[str, object],
        admission: Mapping[str, object],
        admission_order: Mapping[str, object],
        now: float,
    ) -> dict[str, object]:
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
            buffer_ids=buffer_ids,
        )
        self._register_block_runtime_records_locked(
            transfer_id=transfer_id,
            decision=decision,
        )
        updated_admission = {
            **admission,
            "lease_ids": tuple(
                reservation.reservation_id for reservation in reservations
            ),
            "plan_generation": generation,
            "plan_expires_at": self._transfer_plan_expirations[transfer_id],
            "promoted_at": float(now),
            "priority_order": admission_order,
        }
        updated_admission = _admission_with_priority_evidence(
            updated_admission,
            admission_order,
        )
        self._transfer_admissions[transfer_id] = updated_admission
        ticket = self._ticket_for_promoted_transfer_locked(
            transfer_id=transfer_id,
            decision=decision,
            request=request,
            now=now,
        )
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
        return {
            "transfer_id": transfer_id,
            "plan_generation": generation,
            "lease_ids": tuple(
                reservation.reservation_id for reservation in reservations
            ),
            "ticket_id": None if ticket is None else ticket.ticket_id,
            "priority_order": admission_order,
        }

    def _ticket_for_promoted_transfer_locked(
        self,
        *,
        transfer_id: str,
        decision: SchedulingDecision,
        request: Mapping[str, object],
        now: float,
    ) -> ExecutionTicket | None:
        intent_id = request.get("intent_id")
        intent = None if intent_id is None else self._transfer_intents.get(str(intent_id))
        if intent is None:
            return None
        ticket = self._execution_ticket_for_intent_locked(
            intent=intent,
            transfer_id=transfer_id,
            decision=decision,
            now=now,
        )
        self._execution_tickets[ticket.ticket_id] = ticket
        self._transfer_tickets[transfer_id] = ticket.ticket_id
        return ticket

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
            normalize_completion_evidence(
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
            "admission_fairness": (
                dict(admission["fairness"])
                if isinstance(admission.get("fairness"), Mapping)
                else None
            ),
            "multi_tenant_admission": (
                dict(admission["multi_tenant_admission"])
                if isinstance(admission.get("multi_tenant_admission"), Mapping)
                else None
            ),
            "plan_generation": self._transfer_plan_generations.get(str(transfer_id), 0),
            "plan_expires_at": self._transfer_plan_expirations.get(str(transfer_id)),
            "block_plan": _block_plan_runtime_record(decision.plan),
            "block_queue": self._block_runtime_queue_record_locked(str(transfer_id)),
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
            record.get("admission_fairness"),
            record.get("admission_priority_order"),
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
            fairness = admission.get("fairness")
            if isinstance(fairness, Mapping):
                record["admission_fairness"] = dict(fairness)
            priority_order = admission.get("priority_order")
            if isinstance(priority_order, Mapping):
                record["admission_priority_order"] = dict(priority_order)
            multi_tenant_admission = admission.get("multi_tenant_admission")
            if isinstance(multi_tenant_admission, Mapping):
                record["multi_tenant_admission"] = dict(multi_tenant_admission)
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
            record["block_plan"] = _block_plan_runtime_record(decision.plan)
            record["block_queue"] = self._block_runtime_queue_record_locked(
                str(transfer_id)
            )
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
            record.get("admission_fairness"),
            record.get("admission_priority_order"),
            int(record.get("plan_generation", 0) or 0),
            record.get("plan_expires_at"),
            record.get("started_at"),
            record.get("completed_at"),
        )
        if previous_signature != updated_signature:
            self._runtime_state_version += 1
        return record

    def _block_runtime_queue_record_locked(self, transfer_id: str) -> dict[str, object]:
        records = self._block_runtime_records.get(str(transfer_id))
        if records:
            return daemon_block_runtime.queue_view(records)
        decision = self._scheduling_decisions.get(str(transfer_id))
        if decision is None:
            return {
                "source": "daemon_block_runtime",
                "available": False,
                "block_count": 0,
                "states": {},
                "bytes_by_state": {},
            }
        return _block_queue_runtime_record(decision.plan)

    def _runtime_resource_state_locked(
        self,
        *,
        now: float | None = None,
    ) -> dict[str, object]:
        captured_at = float(time.time() if now is None else now)
        transfer_records, recent_terminal_feedback = (
            self._runtime_transfer_records_locked(captured_at)
        )
        transfer_summary = _runtime_transfer_summary_from_records(
            {
                job_id: {
                    "job_id": job_id,
                    "weight": float(job.weight),
                }
                for job_id, job in self._jobs.items()
            },
            transfer_records,
            recent_terminal_feedback,
        )
        transfer_groups = transfer_summary["transfer_groups"]
        active_by_direction = transfer_summary["active_bytes_by_direction"]
        queued_by_direction = transfer_summary["queued_bytes_by_direction"]
        job_runtime_state = transfer_summary["job_runtime_state"]
        relay_state = self._runtime_relay_state_locked(
            transfer_groups["active_transfers"]
        )
        active_resource_usage = self._runtime_active_resource_usage_locked(
            active_by_direction=active_by_direction,
            staging_records=relay_state["staging_records"],
            active_reservations=relay_state["active_reservations"],
            active_leases=relay_state["active_leases"],
        )
        pcie_pool_state = self._refresh_pcie_bandwidth_pool_locked(
            now=captured_at,
            active_paths=relay_state["path_records"],
        )
        runtime_state = {
            "version": self._runtime_state_version,
            "captured_at": captured_at,
            "transfer_order": tuple(self._transfer_queue),
            "transfers": transfer_records,
            "queued_transfers": transfer_groups["queued_transfers"],
            "admitted_transfers": transfer_groups["admitted_transfers"],
            "delayed_transfers": transfer_groups["delayed_transfers"],
            "running_transfers": transfer_groups["running_transfers"],
            "active_transfers": transfer_groups["active_transfers"],
            "recent_terminal_transfers": recent_terminal_feedback,
            "active_paths": relay_state["path_records"],
            "active_resource_usage": active_resource_usage,
            "job_runtime_state": job_runtime_state,
            "active_reservations": relay_state["active_reservations"],
            "active_leases": relay_state["active_leases"],
            "relay_staging": relay_state["staging_records"],
            "pcie_fabric": pcie_pool_state["pcie_fabric"],
            "pcie_edge_load": pcie_pool_state["pcie_edge_load"],
            "pcie_bandwidth_pool": pcie_pool_state["pcie_bandwidth_pool"],
            "hardware_monitoring": pcie_pool_state["hardware_monitoring"],
            "tenant_usage": self._tenant_usage_snapshot_locked(),
            "quota_rejections": tuple(self._quota_rejections[-128:]),
            "summary": self._runtime_resource_summary_locked(
                transfer_groups=transfer_groups,
                terminal_transfer_count=int(
                    transfer_summary["terminal_transfer_count"]
                ),
                recent_terminal_transfer_count=len(recent_terminal_feedback),
                relay_state=relay_state,
                active_resource_usage=active_resource_usage,
                queued_by_direction=queued_by_direction,
                active_by_direction=active_by_direction,
                job_runtime_state=job_runtime_state,
            ),
        }
        _refresh_runtime_feedback_summary(runtime_state)
        return runtime_state

    def _tenant_quota_precheck_locked(
        self,
        peer_identity: PeerIdentity | None,
        *,
        field: str,
        delta_count: int = 0,
        delta_bytes: int = 0,
    ) -> DaemonResponse | None:
        uid = peer_auth.peer_uid(peer_identity)
        if uid is None:
            if self._require_authenticated_peers:
                return peer_auth.authenticated_peer_required_response(peer_identity)
            return None
        usage = self._tenant_usage_snapshot_locked()
        current = dict(usage.get(uid, {}))
        if field == "registered_buffers":
            limit = int(self._tenant_quota_policy["max_buffers_per_uid"])
            if limit > 0 and int(current.get(field, 0) or 0) + int(delta_count) > limit:
                return self._tenant_quota_rejection_locked(uid, field)
            byte_limit = int(self._tenant_quota_policy["max_buffer_bytes_per_uid"])
            if (
                byte_limit > 0
                and int(current.get("registered_buffer_bytes", 0) or 0)
                + int(delta_bytes)
                > byte_limit
            ):
                return self._tenant_quota_rejection_locked(
                    uid,
                    "registered_buffer_bytes",
                )
            return None
        policy_field = {
            "active_sessions": "max_sessions_per_uid",
            "registered_jobs": "max_jobs_per_uid",
        }.get(field)
        if policy_field is None:
            return None
        limit = int(self._tenant_quota_policy[policy_field])
        if limit > 0 and int(current.get(field, 0) or 0) + int(delta_count) > limit:
            return self._tenant_quota_rejection_locked(uid, field)
        return None

    def _tenant_quota_rejection_locked(
        self,
        uid: str,
        field: str,
    ) -> DaemonResponse:
        record = {
            "uid": str(uid),
            "field": str(field),
            "rejected_at": time.time(),
        }
        self._quota_rejections.append(record)
        self._quota_rejections[:] = self._quota_rejections[-128:]
        return DaemonResponse(
            ok=False,
            error=f"tenant quota exceeded: {field}",
            payload={"tenant_quota_rejection": dict(record)},
        )

    def _tenant_usage_snapshot_locked(self) -> dict[str, dict[str, int]]:
        usage: dict[str, dict[str, int]] = {}
        for uid in self._known_tenant_uids_locked():
            usage[uid] = {
                "active_sessions": 0,
                "registered_jobs": 0,
                "registered_buffers": 0,
                "registered_buffer_bytes": 0,
                "active_leases": 0,
                "active_transfers": 0,
            }
        for session_id, session in self._sessions.items():
            if not session.active:
                continue
            uid = peer_auth.peer_uid(self._session_peer_identities.get(session_id))
            if uid is not None:
                usage.setdefault(uid, _empty_tenant_usage())["active_sessions"] += 1
        for job_id, job in self._jobs.items():
            uid = peer_auth.peer_uid(self._job_peer_identities.get(job_id)) or (
                None if job.user_id is None else str(job.user_id)
            )
            if uid is not None:
                usage.setdefault(uid, _empty_tenant_usage())["registered_jobs"] += 1
        for buffer in self._buffers.values():
            job = self._jobs.get(buffer.job_id)
            uid = peer_auth.peer_uid(self._job_peer_identities.get(buffer.job_id)) or (
                None if job is None or job.user_id is None else str(job.user_id)
            )
            if uid is not None:
                tenant = usage.setdefault(uid, _empty_tenant_usage())
                tenant["registered_buffers"] += 1
                tenant["registered_buffer_bytes"] += int(buffer.size_bytes)
        for lease_id, lease in self._lease_tokens.items():
            if lease_id not in self._reservations:
                continue
            uid = self._uid_for_job_locked(lease.job_id)
            if uid is not None:
                usage.setdefault(uid, _empty_tenant_usage())["active_leases"] += 1
        for status in self._transfer_statuses.values():
            if TransferStatusState(status.state) in _TERMINAL_TRANSFER_STATES:
                continue
            uid = self._uid_for_transfer_locked(status.transfer_id)
            if uid is not None:
                usage.setdefault(uid, _empty_tenant_usage())["active_transfers"] += 1
        self._tenant_usage_by_uid = {key: dict(value) for key, value in usage.items()}
        return {key: dict(value) for key, value in usage.items()}

    def _known_tenant_uids_locked(self) -> set[str]:
        uids = {
            uid
            for uid in (
                peer_auth.peer_uid(peer)
                for peer in (
                    *self._session_peer_identities.values(),
                    *self._job_peer_identities.values(),
                )
            )
            if uid is not None
        }
        uids.update(str(job.user_id) for job in self._jobs.values() if job.user_id is not None)
        return set(uids)

    def _uid_for_job_locked(self, job_id: str | None) -> str | None:
        if job_id is None:
            return None
        job_key = str(job_id)
        uid = peer_auth.peer_uid(self._job_peer_identities.get(job_key))
        if uid is not None:
            return uid
        job = self._jobs.get(job_key)
        return None if job is None or job.user_id is None else str(job.user_id)

    def _uid_for_transfer_locked(self, transfer_id: str) -> str | None:
        record = self._transfer_queue_records.get(str(transfer_id), {})
        job_id = record.get("job_id")
        if job_id is not None:
            return self._uid_for_job_locked(str(job_id))
        status = self._transfer_statuses.get(str(transfer_id))
        if status is not None:
            return self._uid_for_job_locked(status.job_id)
        return None

    def _runtime_transfer_records_locked(
        self,
        captured_at: float,
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        for transfer_id in tuple(self._transfer_queue):
            self._refresh_transfer_queue_record_locked(transfer_id, now=captured_at)
        transfer_records = [
            dict(self._transfer_queue_records[transfer_id])
            for transfer_id in self._transfer_queue
            if transfer_id in self._transfer_queue_records
        ]
        live_transfer_ids = {str(record["transfer_id"]) for record in transfer_records}
        recent_terminal_feedback = [
            dict(record)
            for transfer_id, record in sorted(
                self._recent_terminal_feedback_records.items(),
                key=lambda item: float(item[1].get("recorded_at", 0.0) or 0.0),
            )
            if transfer_id not in live_transfer_ids
        ]
        return transfer_records, recent_terminal_feedback

    def _runtime_relay_state_locked(
        self,
        active_transfers: list[dict[str, object]],
    ) -> dict[str, object]:
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
        staging_records = [
            dict(value) for _, value in sorted(self._staging_records.items())
        ]
        relay_runtime_state = {
            "active_paths": path_records,
            "active_reservations": active_reservations,
            "active_leases": active_leases,
            "relay_staging": staging_records,
        }
        relay_activity = relay_activity_from_runtime_state(relay_runtime_state)
        return {
            "path_records": path_records,
            "path_summary": path_summary,
            "active_reservations": active_reservations,
            "active_leases": active_leases,
            "staging_records": staging_records,
            "busy_relays": tuple(sorted(relay_activity["busy_relays"])),
            "relay_load": relay_activity["relay_load"],
        }

    def _runtime_pcie_bandwidth_pool_locked(
        self,
        *,
        active_paths: object,
    ) -> dict[str, object]:
        return self._refresh_pcie_bandwidth_pool_locked(
            now=time.time(),
            active_paths=active_paths,
        )

    def _refresh_pcie_bandwidth_pool_locked(
        self,
        *,
        now: float,
        active_paths: object,
    ) -> dict[str, object]:
        hardware_sample = self._refresh_pcie_hardware_sample_locked()
        if self._topology_provider is None:
            unavailable = {
                "source": "daemon_pcie_bandwidth_pool",
                "available": False,
                "reason": _TOPOLOGY_UNAVAILABLE_ERROR,
                "paths": {},
                "edges": {},
            }
            return {
                "pcie_fabric": {},
                "pcie_edge_load": {},
                "pcie_bandwidth_pool": unavailable,
                "hardware_monitoring": self._hardware_monitoring_summary_locked(
                    hardware_sample
                ),
            }
        try:
            inventory = self._topology_provider.snapshot()
        except Exception as exc:
            unavailable = {
                "source": "daemon_pcie_bandwidth_pool",
                "available": False,
                "reason": str(exc),
                "paths": {},
                "edges": {},
            }
            return {
                "pcie_fabric": {},
                "pcie_edge_load": {},
                "pcie_bandwidth_pool": unavailable,
                "hardware_monitoring": self._hardware_monitoring_summary_locked(
                    hardware_sample
                ),
            }
        fabric = pcie_fabric_snapshot_from_inventory(inventory)
        fabric_record = fabric.as_dict()
        capacity_by_edge = {
            str(edge.edge_id): float(edge.capacity_gbps)
            for edge in fabric.edges
        }
        path_edge_map = {
            int(path.device_id): tuple(path.edge_ids)
            for path in fabric.paths
        }
        edge_load = _pcie_load_from_active_paths(
            active_paths=active_paths,
            path_edge_map=path_edge_map,
            capacity_by_edge=capacity_by_edge,
        )
        runtime_edge_load = _build_runtime_edge_load_snapshot(
            fabric=fabric,
            hardware_sample=hardware_sample,
            active_path_load=edge_load,
        )
        return {
            "pcie_fabric": fabric_record,
            "pcie_edge_load": runtime_edge_load,
            "pcie_bandwidth_pool": _build_bandwidth_pool_snapshot(
                pcie_fabric=fabric_record,
                edge_load=runtime_edge_load,
            ),
            "hardware_monitoring": self._hardware_monitoring_summary_locked(
                hardware_sample
            ),
        }

    def _refresh_pcie_hardware_sample_locked(self):
        sample = self._last_pcie_sample
        if sample is not None and self._pcie_sample_max_age_seconds > 0.0:
            sampled_at = float(getattr(sample, "sampled_at", 0.0) or 0.0)
            if sampled_at > 0.0 and time.time() - sampled_at <= self._pcie_sample_max_age_seconds:
                return sample
        sampler = self._pcie_sampler
        try:
            sample = sampler.sample()
        except Exception as exc:
            sample = HardwarePcieSample(
                sampled_at=time.time(),
                known=False,
                error=str(exc) or exc.__class__.__name__,
            )
        self._last_pcie_sample = sample
        return sample

    def _hardware_monitoring_summary_locked(self, sample=None) -> dict[str, object]:
        resolved = self._last_pcie_sample if sample is None else sample
        if resolved is None:
            return {
                "source": "nvidia_smi_dmon",
                "known": False,
                "sampled_at": 0.0,
                "sample_age_ms": 0.0,
                "error": "hardware monitoring has not sampled yet",
                "counters": [],
            }
        as_dict = getattr(resolved, "as_dict", None)
        if callable(as_dict):
            return dict(as_dict())
        return {
            "source": str(getattr(resolved, "source", "unknown")),
            "known": bool(getattr(resolved, "known", False)),
            "sampled_at": float(getattr(resolved, "sampled_at", 0.0) or 0.0),
            "sample_age_ms": float(getattr(resolved, "sample_age_ms", 0.0) or 0.0),
            "error": getattr(resolved, "error", None),
            "counters": [],
        }

    def _runtime_active_resource_usage_locked(
        self,
        *,
        active_by_direction: dict[str, dict[str, int]],
        staging_records: list[dict[str, object]],
        active_reservations: list[dict[str, object]],
        active_leases: list[dict[str, object]],
    ) -> dict[str, object]:
        return {
            "h2d": dict(active_by_direction.get("h2d", {})),
            "d2h": dict(active_by_direction.get("d2h", {})),
            "p2p": {},
            "relay_staging": {
                "count": len(staging_records),
                "active_reservation_count": len(active_reservations),
                "active_lease_count": len(active_leases),
            },
        }

    def _runtime_resource_summary_locked(
        self,
        *,
        transfer_groups: dict[str, list[dict[str, object]]],
        terminal_transfer_count: int,
        recent_terminal_transfer_count: int,
        relay_state: dict[str, object],
        active_resource_usage: dict[str, object],
        queued_by_direction: dict[str, dict[str, int]],
        active_by_direction: dict[str, dict[str, int]],
        job_runtime_state: dict[str, dict[str, object]],
    ) -> dict[str, object]:
        active_reservations = relay_state["active_reservations"]
        active_leases = relay_state["active_leases"]
        staging_records = relay_state["staging_records"]
        return {
            "queued_transfer_count": len(transfer_groups["queued_transfers"]),
            "admitted_transfer_count": len(transfer_groups["admitted_transfers"]),
            "delayed_transfer_count": len(transfer_groups["delayed_transfers"]),
            "running_transfer_count": len(transfer_groups["running_transfers"]),
            "active_transfer_count": len(transfer_groups["active_transfers"]),
            "recent_terminal_transfer_count": int(recent_terminal_transfer_count),
            "terminal_transfer_count": int(terminal_transfer_count),
            "active_reservation_count": len(active_reservations),
            "active_lease_count": len(active_leases),
            "relay_staging_count": len(staging_records),
            "relay_path_count": 0,
            "relay_path_bytes_total": 0,
            "busy_relays": relay_state["busy_relays"],
            "relay_load": relay_state["relay_load"],
            "completion_source_counts": {},
            "terminal_completion_source_counts": {},
            "active_execution_evidence": _empty_execution_path_evidence(),
            "active_execution_evidence_by_source": {},
            "terminal_execution_evidence": {},
            "terminal_execution_evidence_by_source": {},
            "runtime_feedback_metrics": {},
            "queued_bytes_by_direction": queued_by_direction,
            "active_bytes_by_direction": active_by_direction,
            "active_paths": relay_state["path_summary"],
            "active_resource_usage": active_resource_usage,
            "job_runtime_state": job_runtime_state,
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
        job_runtime_state = {
            job_id: {
                "job_id": job_id,
                "weight": float(job.weight),
            }
            for job_id, job in self._jobs.items()
        }
        return _job_runtime_state_from_records(job_runtime_state, transfer_records)

    def _active_path_records_locked(
        self,
        active_transfers: list[dict[str, object]],
    ) -> tuple[list[dict[str, object]], dict[str, dict[str, int]]]:
        records: list[dict[str, object]] = []
        summary: dict[str, dict[str, int]] = {}
        for record in active_transfers:
            if not isinstance(record, Mapping):
                continue
            transfer_id = str(record.get("transfer_id", ""))
            if not transfer_id:
                continue
            admission = self._transfer_admissions.get(transfer_id, {})
            if admission.get("state") != _ADMISSION_ADMITTED:
                continue
            decision = self._scheduling_decisions.get(transfer_id)
            if decision is None:
                continue
            for path_record in _runtime_active_path_records_for_transfer(
                record=record,
                decision=decision,
            ):
                kind = str(path_record.get("kind", "unknown"))
                direction = str(path_record.get("direction", "unknown"))
                key = f"{direction}:{kind}"
                bucket = summary.setdefault(
                    key,
                    {"path_count": 0, "chunk_count": 0, "bytes_total": 0},
                )
                bucket["path_count"] += 1
                bucket["chunk_count"] += int(path_record.get("chunk_count", 0) or 0)
                bucket["bytes_total"] += int(path_record.get("bytes_total", 0) or 0)
                records.append(dict(path_record))
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
        relay_devices = planning_helpers.relay_devices_from_plan(
            plan,
            direction=request.direction,
        )
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
            ranges = planning_helpers.relay_ranges_from_plan(
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
        ticket = daemon_receipts.execution_ticket_for_plan(
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
            metadata=self._execution_ticket_owner_metadata_locked(
                transfer_id=str(transfer_id),
                decision=decision,
                lease_ids=lease_ids,
            ),
        )
        self._mark_block_runtime_ticket_issued_locked(
            transfer_id=str(transfer_id),
            ticket=ticket,
            now=now,
        )
        return ticket

    def _mark_block_runtime_ticket_issued_locked(
        self,
        *,
        transfer_id: str,
        ticket: ExecutionTicket,
        now: float,
    ) -> None:
        records = self._block_runtime_records.get(str(transfer_id))
        if not records:
            return
        updated = daemon_block_runtime.mark_ticket_issued(
            records,
            ticket_id=ticket.ticket_id,
            issued_at=float(now),
        )
        self._block_runtime_records[str(transfer_id)] = tuple(
            record.as_dict() for record in updated
        )
        self._runtime_state_version += 1

    def _authorization_cleanup_payload_locked(
        self,
        *,
        transfer_id: str,
        job_id: str,
        session_id: str,
        lease_id: str,
    ) -> dict[str, object]:
        normalized_transfer_id = str(transfer_id)
        normalized_lease_id = str(lease_id)
        admission = self._transfer_admissions.get(normalized_transfer_id, {})
        lease_ids = tuple(str(item) for item in admission.get("lease_ids", ()) or ())
        if not lease_ids:
            lease_ids = (normalized_lease_id,)
        elif normalized_lease_id not in lease_ids:
            lease_ids = (normalized_lease_id, *lease_ids)
        relay_gpus: list[int] = []
        primary_relay_gpu: int | None = None
        for scoped_lease_id in lease_ids:
            lease = self._lease_tokens.get(scoped_lease_id)
            if lease is None:
                continue
            relay_gpus.append(int(lease.relay_gpu))
            if scoped_lease_id == normalized_lease_id:
                primary_relay_gpu = int(lease.relay_gpu)
        owner_binding = self._worker_owner_binding_locked(
            transfer_id=normalized_transfer_id,
            job_id=str(job_id),
            session_id=str(session_id),
            lease_ids=lease_ids,
        )
        return {
            "transfer_id": normalized_transfer_id,
            "job_id": str(job_id),
            "session_id": str(session_id),
            "lease_id": normalized_lease_id,
            "lease_ids": lease_ids,
            "relay_gpu": primary_relay_gpu,
            "relay_gpus": tuple(sorted(set(relay_gpus))),
            "plan_generation": self._transfer_plan_generations.get(
                normalized_transfer_id,
                0,
            ),
            "owner_binding": owner_binding,
        }

    def _execution_ticket_owner_metadata_locked(
        self,
        *,
        transfer_id: str,
        decision: SchedulingDecision,
        lease_ids: tuple[str, ...],
    ) -> dict[str, object]:
        metadata = {
            "owner_binding": self._worker_owner_binding_locked(
                transfer_id=str(transfer_id),
                job_id=str(decision.job_id),
                session_id=str(decision.session_id),
                lease_ids=lease_ids,
            )
        }
        block_runtime = daemon_block_runtime.ticket_metadata_view(
            self._block_runtime_records.get(str(transfer_id), ())
        )
        if block_runtime is not None:
            metadata["block_runtime"] = block_runtime
        intent = self._transfer_intents.get(str(decision.intent_id))
        if intent is not None:
            policy_hints = (
                intent.policy_hints
                if isinstance(intent.policy_hints, Mapping)
                else {}
            )
            if "transfer_mode" in policy_hints:
                metadata["transfer_mode"] = str(policy_hints["transfer_mode"])
            if "skip_verification" in policy_hints:
                metadata["skip_verification"] = bool(policy_hints["skip_verification"])
        return metadata

    def _worker_owner_binding_locked(
        self,
        *,
        transfer_id: str,
        job_id: str,
        session_id: str,
        lease_ids: Iterable[str],
    ) -> dict[str, object]:
        normalized_transfer_id = str(transfer_id)
        normalized_job_id = str(job_id)
        normalized_session_id = str(session_id)
        normalized_lease_ids = tuple(str(item) for item in lease_ids)
        relay_gpus: list[int] = []
        for lease_id in normalized_lease_ids:
            lease = self._lease_tokens.get(lease_id)
            if lease is None:
                continue
            relay_gpus.append(int(lease.relay_gpu))
        owner_peer = self._transfer_peer_identities.get(normalized_transfer_id)
        if owner_peer is None and normalized_job_id in self._job_peer_identities:
            owner_peer = self._job_peer_identities.get(normalized_job_id)
        if owner_peer is None:
            owner_peer = self._session_peer_identities.get(normalized_session_id)
        owner_binding = {
            "job_id": normalized_job_id,
            "session_id": normalized_session_id,
            "transfer_id": normalized_transfer_id,
            "lease_ids": normalized_lease_ids,
            "relay_gpus": tuple(sorted(set(relay_gpus))),
            "cleanup_scope": {
                "target_kind": "reservation",
                "target_ids": normalized_lease_ids,
            },
        }
        if owner_peer is not None and owner_peer.authenticated:
            owner_binding["peer_identity"] = asdict(owner_peer)
        return owner_binding

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
        block_runtime_records = self._block_runtime_records.get(transfer_id)
        if (
            not block_runtime_records
            and isinstance(archived.get("block_runtime"), Iterable)
            and not isinstance(archived.get("block_runtime"), (str, bytes, Mapping))
        ):
            block_runtime_records = tuple(
                dict(record)
                for record in archived["block_runtime"]
                if isinstance(record, Mapping)
            )
        buffer_snapshots = self._transfer_buffer_snapshots.get(transfer_id)
        if (
            buffer_snapshots is None
            and isinstance(archived.get("buffer_snapshots"), Mapping)
        ):
            buffer_snapshots = {
                str(key): dict(value)
                for key, value in archived["buffer_snapshots"].items()
                if isinstance(value, Mapping)
            }
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
            block_runtime=block_runtime_records,
            buffer_snapshots=buffer_snapshots,
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

    def _buffer_ownership_record_locked(self, buffer_id: str) -> dict[str, object]:
        normalized = str(buffer_id)
        buffer = self._buffers.get(normalized)
        archived_target = self._retired_cleanup_target_record_locked(
            target_kind="buffer",
            target_id=normalized,
        )
        if buffer is None and archived_target is not None:
            snapshot = archived_target.get("buffer_snapshot")
            owner_job_id = (
                snapshot.get("job_id")
                if isinstance(snapshot, Mapping)
                else None
            )
            transfer_ids = tuple(
                str(item) for item in archived_target.get("transfer_ids", ()) or ()
            )
            return {
                "buffer_id": normalized,
                "state": "retired",
                "job_id": None if owner_job_id is None else str(owner_job_id),
                "transfer_ids": transfer_ids,
                "active_lease_ids": (),
                "active_ticket_ids": (),
                "protected": False,
                "retired_target": _jsonable_cleanup_target_record(archived_target),
            }
        active_lease_ids = self._active_buffer_lease_ids_locked(normalized)
        active_ticket_ids = self._active_buffer_ticket_ids_locked(normalized)
        transfer_ids = self._transfer_ids_for_buffer_locked(normalized)
        job_id = None if buffer is None else buffer.job_id
        job = None if job_id is None else self._jobs.get(str(job_id))
        session_id = None if job is None else job.session_id
        peer_identity = (
            None
            if job_id is None
            else self._job_peer_identities.get(str(job_id))
        )
        return {
            "buffer_id": normalized,
            "state": "registered" if buffer is not None else "unknown",
            "job_id": None if job_id is None else str(job_id),
            "session_id": None if session_id is None else str(session_id),
            "active_lease_ids": active_lease_ids,
            "active_ticket_ids": active_ticket_ids,
            "transfer_ids": transfer_ids,
            "protected": bool(active_lease_ids or active_ticket_ids),
            "peer_identity": (
                None if peer_identity is None else asdict(peer_identity)
            ),
        }

    def _public_buffer_records_locked(self) -> dict[str, dict[str, object]]:
        return {
            key: self._public_buffer_record_locked(value)
            for key, value in sorted(self._buffers.items())
        }

    def _public_buffer_record_locked(
        self,
        buffer: BufferRegistration,
    ) -> dict[str, object]:
        metadata = _redact_public_payload(dict(buffer.metadata))
        return {
            "buffer_id": buffer.buffer_id,
            "job_id": buffer.job_id,
            "kind": str(buffer.kind),
            "size_bytes": int(buffer.size_bytes),
            "device_index": buffer.device_index,
            "pinned": bool(buffer.pinned),
            "handle_type": str(buffer.handle_type),
            "metadata": metadata,
            "redacted_fields": ("address",),
            "metadata_redacted": _public_payload_has_redaction(metadata),
        }

    def _active_buffer_ticket_ids_locked(self, buffer_id: str) -> tuple[str, ...]:
        normalized = str(buffer_id)
        return tuple(
            ticket_id
            for ticket_id, ticket in sorted(self._execution_tickets.items())
            if normalized in {ticket.source_buffer_id, ticket.destination_buffer_id}
            and self._ticket_transfer_is_active_locked(ticket)
        )

    def _ticket_transfer_is_active_locked(self, ticket: ExecutionTicket) -> bool:
        transfer_id = ticket.metadata.get("transfer_id")
        if transfer_id is None:
            return True
        status = self._transfer_statuses.get(str(transfer_id))
        return status is None or status.state not in _TERMINAL_TRANSFER_STATES

    def _active_buffer_protection_record_locked(
        self,
        buffer_id: str,
    ) -> dict[str, object]:
        ownership = self._buffer_ownership_record_locked(buffer_id)
        return {
            "buffer_id": str(buffer_id),
            "protected": bool(ownership.get("protected", False)),
            "active_lease_ids": tuple(
                str(item) for item in ownership.get("active_lease_ids", ()) or ()
            ),
            "active_ticket_ids": tuple(
                str(item) for item in ownership.get("active_ticket_ids", ()) or ()
            ),
            "transfer_ids": tuple(
                str(item) for item in ownership.get("transfer_ids", ()) or ()
            ),
        }

    def _buffer_cleanup_ownership_evidence_locked(
        self,
        buffer_id: str,
        *,
        reason: str,
    ) -> dict[str, object]:
        return {
            "daemon_buffer_ownership": self._buffer_ownership_record_locked(buffer_id),
            "cleanup_reason": str(reason),
            "cleanup_recorded_at": time.time(),
        }

    def _buffer_cleanup_ownership_evidence_for_removed_locked(
        self,
        buffer_id: str,
        *,
        reason: str,
    ) -> dict[str, object]:
        archived_target = self._retired_cleanup_target_record_locked(
            target_kind="buffer",
            target_id=str(buffer_id),
        )
        ownership = self._buffer_ownership_record_locked(buffer_id)
        return {
            "daemon_buffer_ownership": ownership,
            "cleanup_reason": str(reason),
            "cleanup_recorded_at": time.time(),
            "retired_target": (
                None
                if archived_target is None
                else _jsonable_cleanup_target_record(archived_target)
            ),
        }

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
        context = self._audit_record_context_locked(
            transfer_id=transfer_id,
            reservation=reservation,
            lease=lease,
            staging_record=staging_record,
            ticket=ticket,
            session_id=session_id,
        )
        record = self._audit_record_payload_locked(
            created_at=created_at,
            event_type=str(event_type),
            context=context,
            state=state,
            reason=reason,
            failure_reason=failure_reason,
            cleanup_kind=cleanup_kind,
            cleanup_target_id=cleanup_target_id,
            bytes_completed=bytes_completed,
        )
        self._audit_records.append(record)
        return record

    def _audit_record_context_locked(
        self,
        *,
        transfer_id: str | None,
        reservation: TransferReservation | None,
        lease: LeaseToken | None,
        staging_record: Mapping[str, object] | None,
        ticket: ExecutionTicket | None,
        session_id: str | None,
    ) -> dict[str, object]:
        normalized_transfer_id = self._audit_transfer_id(
            transfer_id=transfer_id,
            staging_record=staging_record,
        )
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
        ticket_id, ticket = self._audit_ticket(
            transfer_id=normalized_transfer_id,
            ticket=ticket,
        )
        lease_id, lease = self._audit_lease(
            reservation=reservation,
            lease=lease,
            staging_record=staging_record,
        )
        resolved_session_id = self._audit_session_id(
            session_id=session_id,
            status=status,
            reservation=reservation,
            lease=lease,
            staging_record=staging_record,
        )
        job_id = self._audit_job_id(
            status=status,
            decision=decision,
            lease=lease,
            staging_record=staging_record,
        )
        job = None if job_id is None else self._jobs.get(job_id)
        return {
            "transfer_id": normalized_transfer_id,
            "status": status,
            "decision": decision,
            "ticket": ticket,
            "ticket_id": ticket_id,
            "lease": lease,
            "lease_id": lease_id,
            "session_id": resolved_session_id,
            "job_id": job_id,
            "job": job,
            "buffer_ids": self._audit_buffer_ids(
                lease=lease,
                staging_record=staging_record,
                ticket=ticket,
            ),
            "relay_gpu": self._audit_relay_gpu(
                reservation=reservation,
                lease=lease,
                staging_record=staging_record,
            ),
            "direction": self._audit_direction(
                reservation=reservation,
                staging_record=staging_record,
                ticket=ticket,
            ),
            "bytes_total": self._audit_bytes_total(
                reservation=reservation,
                staging_record=staging_record,
                status=status,
            ),
            "staging_record": staging_record,
        }

    def _audit_record_payload_locked(
        self,
        *,
        created_at: float,
        event_type: str,
        context: Mapping[str, object],
        state: TransferStatusState | str | None,
        reason: str | None,
        failure_reason: str | None,
        cleanup_kind: str | None,
        cleanup_target_id: str | None,
        bytes_completed: int | None,
    ) -> dict[str, object]:
        status = context.get("status")
        decision = context.get("decision")
        ticket = context.get("ticket")
        job = context.get("job")
        bytes_total = int(context.get("bytes_total", 0) or 0)
        completed = (
            int(bytes_completed)
            if bytes_completed is not None
            else (
                int(status.bytes_completed)
                if isinstance(status, TransferStatus)
                else 0
            )
        )
        if context.get("lease_id") is not None and bytes_total:
            completed = min(completed, bytes_total)
        duration_seconds = self._audit_duration_seconds(
            created_at=created_at,
            staging_record=context.get("staging_record"),
            decision=decision,
        )
        return {
            "audit_id": f"audit-{len(self._audit_records) + 1}",
            "event_type": str(event_type),
            "created_at": created_at,
            "transfer_id": context.get("transfer_id"),
            "decision_id": None if decision is None else decision.decision_id,
            "ticket_id": context.get("ticket_id"),
            "topology_snapshot_id": (
                None if decision is None else decision.topology_snapshot_id
            ),
            "lease_id": context.get("lease_id"),
            "session_id": (
                None
                if context.get("session_id") is None
                else str(context.get("session_id"))
            ),
            "job_id": context.get("job_id"),
            "user_id": None if job is None else job.user_id,
            "process_id": None if job is None else job.process_id,
            "container_id": None if job is None else job.container_id,
            "relay_gpu": context.get("relay_gpu"),
            "direction": context.get("direction"),
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
            "buffer_ids": context.get("buffer_ids", ()),
            "staging_record_id": (
                None
                if context.get("staging_record") is None
                else context["staging_record"].get("staging_record_id")
            ),
        }

    def _audit_transfer_id(
        self,
        *,
        transfer_id: str | None,
        staging_record: Mapping[str, object] | None,
    ) -> str | None:
        if transfer_id is not None:
            return str(transfer_id)
        if staging_record is None:
            return None
        value = staging_record.get("transfer_id")
        return None if value is None else str(value)

    def _audit_ticket(
        self,
        *,
        transfer_id: str | None,
        ticket: ExecutionTicket | None,
    ) -> tuple[str | None, ExecutionTicket | None]:
        if ticket is not None:
            return ticket.ticket_id, ticket
        if transfer_id is None:
            return None, None
        ticket_id = self._transfer_tickets.get(transfer_id)
        active_ticket = None if ticket_id is None else self._execution_tickets.get(ticket_id)
        return ticket_id, active_ticket

    def _audit_lease(
        self,
        *,
        reservation: TransferReservation | None,
        lease: LeaseToken | None,
        staging_record: Mapping[str, object] | None,
    ) -> tuple[str | None, LeaseToken | None]:
        if lease is not None:
            return lease.lease_id, lease
        if reservation is not None:
            lease_id = reservation.reservation_id
        elif staging_record is not None:
            value = staging_record.get("lease_id")
            lease_id = None if value is None else str(value)
        else:
            lease_id = None
        resolved_lease = None if lease_id is None else self._lease_tokens.get(lease_id)
        return lease_id, resolved_lease

    def _audit_session_id(
        self,
        *,
        session_id: str | None,
        status: TransferStatus | None,
        reservation: TransferReservation | None,
        lease: LeaseToken | None,
        staging_record: Mapping[str, object] | None,
    ) -> str | None:
        if session_id is not None:
            return session_id
        if status is not None:
            return status.session_id
        if lease is not None:
            return lease.session_id
        if reservation is not None:
            return reservation.session_id
        if staging_record is None:
            return None
        value = staging_record.get("session_id")
        return None if value is None else str(value)

    def _audit_job_id(
        self,
        *,
        status: TransferStatus | None,
        decision: SchedulingDecision | None,
        lease: LeaseToken | None,
        staging_record: Mapping[str, object] | None,
    ) -> str | None:
        if status is not None:
            return status.job_id
        if lease is not None:
            return lease.job_id
        if staging_record is not None:
            value = staging_record.get("job_id")
            return None if value is None else str(value)
        if decision is not None:
            return decision.job_id
        return None

    def _audit_buffer_ids(
        self,
        *,
        lease: LeaseToken | None,
        staging_record: Mapping[str, object] | None,
        ticket: ExecutionTicket | None,
    ) -> tuple[str, ...]:
        if lease is not None:
            return tuple(lease.buffer_ids)
        if staging_record is not None:
            return tuple(str(item) for item in staging_record.get("buffer_ids", ()))
        if ticket is not None:
            return (ticket.source_buffer_id, ticket.destination_buffer_id)
        return ()

    def _audit_relay_gpu(
        self,
        *,
        reservation: TransferReservation | None,
        lease: LeaseToken | None,
        staging_record: Mapping[str, object] | None,
    ) -> int | None:
        if reservation is not None:
            return reservation.relay_gpu
        if lease is not None:
            return lease.relay_gpu
        if staging_record is not None and staging_record.get("relay_gpu") is not None:
            return int(staging_record["relay_gpu"])
        return None

    def _audit_direction(
        self,
        *,
        reservation: TransferReservation | None,
        staging_record: Mapping[str, object] | None,
        ticket: ExecutionTicket | None,
    ) -> str | None:
        if reservation is not None:
            return reservation.direction
        if staging_record is not None:
            value = staging_record.get("direction")
            return None if value is None else str(value)
        if ticket is not None:
            return ticket.direction
        return None

    def _audit_bytes_total(
        self,
        *,
        reservation: TransferReservation | None,
        staging_record: Mapping[str, object] | None,
        status: TransferStatus | None,
    ) -> int:
        if reservation is not None:
            return int(reservation.bytes)
        if staging_record is not None:
            return int(staging_record.get("requested_bytes", 0) or 0)
        if status is not None:
            return int(status.bytes_total)
        return 0

    def _audit_duration_seconds(
        self,
        *,
        created_at: float,
        staging_record: object,
        decision: object,
    ) -> float | None:
        started_at = None
        if isinstance(staging_record, Mapping):
            started_at = float(staging_record.get("created_at", 0.0) or 0.0)
        elif isinstance(decision, SchedulingDecision):
            started_at = float(decision.issued_at)
        if not started_at:
            return None
        return max(0.0, created_at - started_at)

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
        for transfer_id, archived in self._transfer_receipt_archive.items():
            if not isinstance(archived, Mapping):
                continue
            archived_ticket = archived.get("ticket")
            if isinstance(archived_ticket, ExecutionTicket) and normalized in {
                str(archived_ticket.source_buffer_id),
                str(archived_ticket.destination_buffer_id),
            }:
                transfer_ids.add(str(transfer_id))
                continue
            archived_snapshots = archived.get("buffer_snapshots")
            if not isinstance(archived_snapshots, Mapping):
                continue
            for snapshot in archived_snapshots.values():
                if not isinstance(snapshot, Mapping):
                    continue
                if str(snapshot.get("buffer_id", "")) == normalized:
                    transfer_ids.add(str(transfer_id))
                    break
        return tuple(sorted(transfer_ids))

    def _session_owned_cleanup_targets_locked(
        self,
        session_id: str,
    ) -> dict[str, tuple[dict[str, object], ...]]:
        normalized_session_id = str(session_id)
        session_peer = self._session_peer_identities.get(normalized_session_id)
        job_ids = tuple(
            sorted(
                job_id
                for job_id, job in self._jobs.items()
                if job.session_id == normalized_session_id
            )
        )
        job_id_set = set(job_ids)
        job_targets: list[dict[str, object]] = []
        for job_id in job_ids:
            job_targets.append(
                {
                    "target_kind": "job",
                    "target_id": job_id,
                    "peer_identity": self._job_peer_identities.get(job_id) or session_peer,
                    "transfer_ids": self._transfer_ids_for_job_locked(job_id),
                }
            )
        buffer_targets: list[dict[str, object]] = []
        for buffer_id, buffer in sorted(self._buffers.items()):
            if buffer.job_id not in job_id_set:
                continue
            transfer_ids = set(self._transfer_ids_for_buffer_locked(buffer_id))
            for lease_id in self._active_buffer_lease_ids_locked(buffer_id):
                transfer_id = self._reservation_transfers.get(lease_id)
                if transfer_id is not None:
                    transfer_ids.add(str(transfer_id))
            buffer_targets.append(
                {
                    "target_kind": "buffer",
                    "target_id": buffer_id,
                    "peer_identity": self._job_peer_identities.get(buffer.job_id)
                    or session_peer,
                    "transfer_ids": tuple(sorted(transfer_ids)),
                    "buffer_snapshot": planning_helpers.buffer_snapshot_record(buffer),
                }
            )
        return {
            "jobs": tuple(job_targets),
            "buffers": tuple(buffer_targets),
        }

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

    def _coerce_peer_identity(
        self,
        value: object,
    ) -> PeerIdentity | None:
        if isinstance(value, PeerIdentity):
            return value
        if isinstance(value, Mapping):
            try:
                return PeerIdentity(**dict(value))
            except (TypeError, ValueError):
                return None
        return None

    def _archived_transfer_owner_peer_locked(
        self,
        transfer_id: str,
    ) -> PeerIdentity | None:
        archived = self._transfer_receipt_archive.get(str(transfer_id), {})
        if not isinstance(archived, Mapping):
            return None
        archived_peer = self._coerce_peer_identity(archived.get("peer_identity"))
        if archived_peer is not None and archived_peer.authenticated:
            return archived_peer
        archived_status = archived.get("status")
        job_id = getattr(archived_status, "job_id", None)
        if job_id is not None:
            job_peer = self._job_peer_identities.get(str(job_id))
            if job_peer is not None and job_peer.authenticated:
                return job_peer
        archived_intent = archived.get("intent")
        session_id = getattr(archived_intent, "session_id", None)
        if session_id is not None:
            session_peer = self._session_peer_identities.get(str(session_id))
            if session_peer is not None and session_peer.authenticated:
                return session_peer
        return None

    def _transfer_owner_peer_for_archive_locked(
        self,
        *,
        transfer_id: str,
        status: TransferStatus,
        existing: Mapping[str, object],
    ) -> PeerIdentity | None:
        transfer_peer = self._transfer_peer_identities.get(str(transfer_id))
        if transfer_peer is not None and transfer_peer.authenticated:
            return transfer_peer
        existing_peer = self._coerce_peer_identity(existing.get("peer_identity"))
        if existing_peer is not None and existing_peer.authenticated:
            return existing_peer
        return self._transfer_peer_identity_for_owner_locked(
            job_id=status.job_id,
            session_id=status.session_id or "",
            peer_identity=None,
        )

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
            transfer_peer = self._archived_transfer_owner_peer_locked(
                str(transfer_id)
            )
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
            archived_peer = self._coerce_peer_identity(
                archived_target.get("peer_identity")
            )
            if archived_peer is not None and archived_peer.authenticated:
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
            if transfer_peer is None:
                transfer_peer = self._archived_transfer_owner_peer_locked(transfer_id)
            if transfer_peer is None or not transfer_peer.authenticated:
                raise ValueError("transfer owner identity is unavailable")
            peer_auth.validate_peer_owner_match(
                expected=transfer_peer,
                actual=peer_identity,
                owner_name="transfer",
            )

    def _validate_cleanup_owner_binding_locked(
        self,
        cleanup: CleanupRequest,
        *,
        peer_identity: PeerIdentity | None,
    ) -> dict[str, object] | None:
        owner_binding = cleanup.owner_binding
        if owner_binding is None:
            return None
        if cleanup.target_kind != "reservation":
            raise ValueError("cleanup owner_binding is only supported for reservations")
        if not isinstance(owner_binding, Mapping):
            raise ValueError("cleanup owner_binding must be a mapping")
        binding = self._normalized_cleanup_owner_binding_locked(cleanup, owner_binding)
        job_id = str(binding["job_id"])
        session_id = str(binding["session_id"])
        transfer_id = str(binding["transfer_id"])
        normalized_lease_ids = binding["lease_ids"]
        normalized_cleanup_target_ids = binding["cleanup_scope"]["target_ids"]
        owner_peer_identity = binding.get("owner_peer_identity")
        normalized_owner_peer = binding.get("peer_identity")
        if (
            owner_peer_identity is not None
            and peer_identity is not None
            and peer_identity.authenticated
        ):
            peer_auth.validate_peer_owner_match(
                expected=owner_peer_identity,
                actual=peer_identity,
                owner_name="cleanup owner_binding",
            )
        for scoped_lease_id in normalized_cleanup_target_ids:
            owner_peer_identity, normalized_owner_peer = (
                self._validate_cleanup_owner_binding_target_locked(
                    scoped_lease_id=str(scoped_lease_id),
                    job_id=job_id,
                    session_id=session_id,
                    transfer_id=transfer_id,
                    owner_peer_identity=owner_peer_identity,
                    normalized_owner_peer=normalized_owner_peer,
                    peer_identity=peer_identity,
                )
            )
        result = {
            "job_id": job_id,
            "session_id": session_id,
            "transfer_id": transfer_id,
            "lease_ids": normalized_lease_ids,
            "cleanup_scope": {
                "target_kind": binding["cleanup_scope"]["target_kind"],
                "target_ids": normalized_cleanup_target_ids,
            },
        }
        relay_gpus = owner_binding.get("relay_gpus")
        if isinstance(relay_gpus, Iterable) and not isinstance(relay_gpus, (str, bytes)):
            result["relay_gpus"] = tuple(sorted({int(item) for item in relay_gpus}))
        if normalized_owner_peer is not None:
            result["peer_identity"] = normalized_owner_peer
        return result

    def _normalized_cleanup_owner_binding_locked(
        self,
        cleanup: CleanupRequest,
        owner_binding: Mapping[str, object],
    ) -> dict[str, object]:
        job_id = str(owner_binding.get("job_id", ""))
        session_id = str(owner_binding.get("session_id", ""))
        transfer_id = str(owner_binding.get("transfer_id", ""))
        if not job_id.strip():
            raise ValueError("cleanup owner_binding job_id must be non-empty")
        if not session_id.strip():
            raise ValueError("cleanup owner_binding session_id must be non-empty")
        if not transfer_id.strip():
            raise ValueError("cleanup owner_binding transfer_id must be non-empty")
        lease_ids = owner_binding.get("lease_ids")
        if not isinstance(lease_ids, Iterable) or isinstance(lease_ids, (str, bytes)):
            raise ValueError("cleanup owner_binding lease_ids must be iterable")
        normalized_lease_ids = tuple(str(item) for item in lease_ids)
        if not normalized_lease_ids or any(not item.strip() for item in normalized_lease_ids):
            raise ValueError("cleanup owner_binding lease_ids must be non-empty")
        cleanup_scope = owner_binding.get("cleanup_scope")
        if not isinstance(cleanup_scope, Mapping):
            raise ValueError("cleanup owner_binding cleanup_scope must be a mapping")
        cleanup_target_kind = str(cleanup_scope.get("target_kind", "")).lower()
        if cleanup_target_kind != "reservation":
            raise ValueError("cleanup owner_binding cleanup_scope must target reservations")
        cleanup_target_ids = cleanup_scope.get("target_ids")
        if not isinstance(cleanup_target_ids, Iterable) or isinstance(
            cleanup_target_ids,
            (str, bytes),
        ):
            raise ValueError(
                "cleanup owner_binding cleanup_scope target_ids must be iterable"
            )
        normalized_cleanup_target_ids = tuple(str(item) for item in cleanup_target_ids)
        if (
            not normalized_cleanup_target_ids
            or any(not item.strip() for item in normalized_cleanup_target_ids)
        ):
            raise ValueError(
                "cleanup owner_binding cleanup_scope target_ids must be non-empty"
            )
        if normalized_cleanup_target_ids != normalized_lease_ids:
            raise ValueError(
                "cleanup owner_binding cleanup_scope does not match lease_ids"
            )
        normalized_target_id = str(cleanup.target_id)
        if normalized_target_id not in normalized_cleanup_target_ids:
            raise ValueError("cleanup target escaped daemon-issued cleanup scope")
        owner_peer = owner_binding.get("peer_identity")
        normalized_owner_peer: dict[str, object] | None = None
        owner_peer_identity: PeerIdentity | None = None
        if isinstance(owner_peer, Mapping):
            owner_peer_identity = PeerIdentity(**dict(owner_peer))
            normalized_owner_peer = asdict(owner_peer_identity)
        result: dict[str, object] = {
            "job_id": job_id,
            "session_id": session_id,
            "transfer_id": transfer_id,
            "lease_ids": normalized_lease_ids,
            "cleanup_scope": {
                "target_kind": cleanup_target_kind,
                "target_ids": normalized_cleanup_target_ids,
            },
        }
        if normalized_owner_peer is not None:
            result["peer_identity"] = normalized_owner_peer
        if owner_peer_identity is not None:
            result["owner_peer_identity"] = owner_peer_identity
        return result

    def _validate_cleanup_owner_binding_target_locked(
        self,
        *,
        scoped_lease_id: str,
        job_id: str,
        session_id: str,
        transfer_id: str,
        owner_peer_identity: PeerIdentity | None,
        normalized_owner_peer: dict[str, object] | None,
        peer_identity: PeerIdentity | None,
    ) -> tuple[PeerIdentity | None, dict[str, object] | None]:
        reservation = self._reservations.get(scoped_lease_id)
        if reservation is not None:
            self._validate_cleanup_owner_binding_reservation_locked(
                scoped_lease_id=scoped_lease_id,
                reservation=reservation,
                job_id=job_id,
                session_id=session_id,
                transfer_id=transfer_id,
                peer_identity=peer_identity,
            )
            return owner_peer_identity, normalized_owner_peer
        staging_record = self._staging_records.get(scoped_lease_id)
        if staging_record is not None:
            self._validate_cleanup_owner_binding_staging_locked(
                staging_record=staging_record,
                job_id=job_id,
                session_id=session_id,
                transfer_id=transfer_id,
                peer_identity=peer_identity,
            )
            return owner_peer_identity, normalized_owner_peer
        return self._validate_cleanup_owner_binding_archive_locked(
            scoped_lease_id=scoped_lease_id,
            job_id=job_id,
            transfer_id=transfer_id,
            owner_peer_identity=owner_peer_identity,
            normalized_owner_peer=normalized_owner_peer,
            peer_identity=peer_identity,
        )

    def _validate_cleanup_owner_binding_reservation_locked(
        self,
        *,
        scoped_lease_id: str,
        reservation: TransferReservation,
        job_id: str,
        session_id: str,
        transfer_id: str,
        peer_identity: PeerIdentity | None,
    ) -> None:
        if str(reservation.session_id) != session_id:
            raise ValueError("cleanup owner_binding session does not match reservation")
        lease = self._lease_tokens.get(scoped_lease_id)
        if lease is None:
            raise ValueError("cleanup owner_binding lease token is unavailable")
        if lease.job_id is not None and str(lease.job_id) != job_id:
            raise ValueError("cleanup owner_binding job does not match lease")
        mapped_transfer_id = self._reservation_transfers.get(scoped_lease_id)
        if mapped_transfer_id != transfer_id:
            raise ValueError("cleanup owner_binding transfer does not match reservation")
        self._validate_peer_owns_lease_locked(
            lease_id=scoped_lease_id,
            peer_identity=peer_identity,
        )

    def _validate_cleanup_owner_binding_staging_locked(
        self,
        *,
        staging_record: Mapping[str, object],
        job_id: str,
        session_id: str,
        transfer_id: str,
        peer_identity: PeerIdentity | None,
    ) -> None:
        if str(staging_record.get("session_id", "")) != session_id:
            raise ValueError("cleanup owner_binding session does not match staging record")
        if str(staging_record.get("job_id", "")) != job_id:
            raise ValueError("cleanup owner_binding job does not match staging record")
        if str(staging_record.get("transfer_id", "")) != transfer_id:
            raise ValueError("cleanup owner_binding transfer does not match staging record")
        self._validate_peer_owns_staging_record_locked(
            staging_record=staging_record,
            peer_identity=peer_identity,
        )

    def _validate_cleanup_owner_binding_archive_locked(
        self,
        *,
        scoped_lease_id: str,
        job_id: str,
        transfer_id: str,
        owner_peer_identity: PeerIdentity | None,
        normalized_owner_peer: dict[str, object] | None,
        peer_identity: PeerIdentity | None,
    ) -> tuple[PeerIdentity | None, dict[str, object] | None]:
        archived_target = self._retired_cleanup_target_record_locked(
            target_kind="reservation",
            target_id=scoped_lease_id,
        )
        if archived_target is None:
            raise ValueError("cleanup owner_binding references unknown reservation")
        archived_transfer_ids = tuple(
            str(item) for item in archived_target.get("transfer_ids", ()) or ()
        )
        if archived_transfer_ids and transfer_id not in archived_transfer_ids:
            raise ValueError(
                "cleanup owner_binding transfer does not match archived reservation"
            )
        if archived_transfer_ids:
            self._validate_peer_owns_receipt_transfer_locked(
                transfer_id=transfer_id,
                job_id=job_id,
                peer_identity=peer_identity,
            )
        archived_peer = self._coerce_peer_identity(archived_target.get("peer_identity"))
        if (
            owner_peer_identity is not None
            and archived_peer is not None
            and archived_peer.authenticated
        ):
            peer_auth.validate_peer_owner_match(
                expected=archived_peer,
                actual=owner_peer_identity,
                owner_name="reservation",
            )
            return owner_peer_identity, normalized_owner_peer
        if owner_peer_identity is not None:
            return owner_peer_identity, normalized_owner_peer
        for archived_transfer_id in archived_transfer_ids:
            archived_transfer_peer = self._archived_transfer_owner_peer_locked(
                archived_transfer_id
            )
            if archived_transfer_peer is not None:
                owner_peer_identity = archived_transfer_peer
                normalized_owner_peer = asdict(archived_transfer_peer)
                break
        if (
            owner_peer_identity is not None
            and peer_identity is not None
            and peer_identity.authenticated
        ):
            peer_auth.validate_peer_owner_match(
                expected=owner_peer_identity,
                actual=peer_identity,
                owner_name="cleanup owner_binding",
            )
        return owner_peer_identity, normalized_owner_peer

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
        self._record_terminal_runtime_feedback_locked(transfer_id)

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
        self._record_terminal_runtime_feedback_locked(terminal.transfer_id)
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
        buffer_snapshot: Mapping[str, object] | None = None,
        retention_evidence: Mapping[str, object] | None = None,
    ) -> None:
        normalized_kind = str(target_kind)
        normalized_id = str(target_id)
        existing = dict(
            self._retired_cleanup_targets.get((normalized_kind, normalized_id), {})
        )
        existing_peer = self._coerce_peer_identity(existing.get("peer_identity"))
        archived_peer = peer_identity if peer_identity is not None else existing_peer
        existing_transfer_ids = tuple(
            str(item) for item in existing.get("transfer_ids", ()) or ()
        )
        merged_transfer_ids = tuple(
            dict.fromkeys(
                existing_transfer_ids
                + tuple(str(item) for item in transfer_ids)
            )
        )
        record = {
            "target_kind": normalized_kind,
            "target_id": normalized_id,
            "peer_identity": archived_peer,
            "reason": (
                existing.get("reason") if reason is None else str(reason)
            ),
            "retired_at": time.time(),
            "transfer_ids": merged_transfer_ids,
        }
        if isinstance(buffer_snapshot, Mapping):
            record["buffer_snapshot"] = dict(buffer_snapshot)
        elif isinstance(existing.get("buffer_snapshot"), Mapping):
            record["buffer_snapshot"] = dict(existing["buffer_snapshot"])
        merged_retention = _merge_retention_evidence(
            existing.get("retention_evidence"),
            retention_evidence,
        )
        if merged_retention is not None:
            record["retention_evidence"] = merged_retention
        self._retired_cleanup_targets[(normalized_kind, normalized_id)] = {
            **existing,
            **record,
        }

    def _retired_cleanup_target_record_locked(
        self,
        *,
        target_kind: str,
        target_id: str,
    ) -> dict[str, object] | None:
        return self._retired_cleanup_targets.get((str(target_kind), str(target_id)))

    def _record_cleanup_retention_evidence_locked(
        self,
        *,
        target_kind: str,
        target_id: str,
        retention_evidence: Mapping[str, object],
    ) -> bool:
        if not isinstance(retention_evidence, Mapping):
            return False
        archived_target = self._retired_cleanup_target_record_locked(
            target_kind=target_kind,
            target_id=target_id,
        )
        if archived_target is None:
            return False
        merged_retention = _merge_retention_evidence(
            archived_target.get("retention_evidence"),
            retention_evidence,
        )
        if merged_retention is None:
            return False
        updated_target = dict(archived_target)
        updated_target["retention_evidence"] = merged_retention
        self._retired_cleanup_targets[(str(target_kind), str(target_id))] = updated_target
        if str(target_kind) == "buffer":
            self._record_buffer_retention_for_transfers_locked(
                target_id=str(target_id),
                archived_target=updated_target,
            )
        self._runtime_state_version += 1
        return True

    def _record_buffer_retention_for_transfers_locked(
        self,
        *,
        target_id: str,
        archived_target: Mapping[str, object],
    ) -> None:
        transfer_ids = tuple(
            str(item) for item in archived_target.get("transfer_ids", ()) or ()
        )
        if not transfer_ids:
            return
        retention_evidence = archived_target.get("retention_evidence")
        if not isinstance(retention_evidence, Mapping):
            return
        buffer_snapshot = archived_target.get("buffer_snapshot")
        for transfer_id in transfer_ids:
            self._merge_buffer_retention_into_transfer_archive_locked(
                transfer_id=str(transfer_id),
                buffer_id=str(target_id),
                retention_evidence=retention_evidence,
                archived_buffer_snapshot=buffer_snapshot,
            )

    def _merge_buffer_retention_into_transfer_archive_locked(
        self,
        *,
        transfer_id: str,
        buffer_id: str,
        retention_evidence: Mapping[str, object],
        archived_buffer_snapshot: object,
    ) -> None:
        archived = self._transfer_receipt_archive.get(str(transfer_id))
        if not isinstance(archived, Mapping):
            return
        buffer_snapshots = {
            str(key): dict(value)
            for key, value in archived.get("buffer_snapshots", {}).items()
            if isinstance(value, Mapping)
        }
        updated = False
        for role, snapshot in list(buffer_snapshots.items()):
            if str(snapshot.get("buffer_id")) != str(buffer_id):
                continue
            buffer_snapshots[role] = _buffer_snapshot_with_retention_evidence(
                snapshot,
                retention_evidence=retention_evidence,
                archived_buffer_snapshot=archived_buffer_snapshot,
            )
            updated = True
        if not updated:
            return
        updated_archived = dict(archived)
        updated_archived["buffer_snapshots"] = buffer_snapshots
        self._transfer_receipt_archive[str(transfer_id)] = updated_archived

    def _retire_transfer_runtime_state_locked(
        self,
        transfer_id: str,
    ) -> None:
        normalized = str(transfer_id)
        self._archive_transfer_receipt_state_locked(normalized)
        self._record_terminal_runtime_feedback_locked(normalized)
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
        archived_record = daemon_transfer_lifecycle.archive_record(
            transfer_id=normalized,
            existing=existing,
            request=request,
            intent=current_intent,
            status=status,
            decision=decision,
            ticket=self._receipt_execution_ticket_for_transfer_locked(normalized),
            admission=self._transfer_admissions.get(normalized, {}),
            plan_generation=self._transfer_plan_generations.get(normalized, 0),
            plan_expires_at=self._transfer_plan_expirations.get(normalized),
            completion_source=self._transfer_completion_sources.get(normalized),
            completion_evidence=self._transfer_completion_evidence.get(normalized, {}),
            block_runtime=self._block_runtime_records.get(normalized, ()),
            buffer_snapshots=self._transfer_buffer_snapshots.get(normalized, {}),
            queue_record=self._transfer_queue_records.get(normalized, {}),
            reservations=(
                self._runtime_reservation_record_locked(reservation_id, reservation)
                for reservation_id, reservation in sorted(self._reservations.items())
                if self._reservation_transfers.get(reservation_id) == normalized
            ),
            leases=(
                self._runtime_lease_record_locked(lease_id, lease)
                for lease_id, lease in sorted(self._lease_tokens.items())
                if self._reservation_transfers.get(lease_id) == normalized
            ),
            peer_identity=self._transfer_owner_peer_for_archive_locked(
                transfer_id=normalized,
                status=status,
                existing=existing,
            ),
        )
        if archived_record["intent_id"] is not None:
            self._archived_intent_transfers[str(archived_record["intent_id"])] = normalized
        updated = daemon_transfer_lifecycle.merge_archive_record(
            existing=existing,
            record=archived_record,
        )
        if updated != existing:
            self._runtime_state_version += 1
        self._transfer_receipt_archive[normalized] = updated

    def _resolve_recovery_transfer_id_locked(
        self,
        *,
        intent_id: str | None,
        transfer_id: str | None,
    ) -> str:
        if transfer_id is not None:
            normalized_transfer_id = str(transfer_id)
            if (
                normalized_transfer_id not in self._transfer_statuses
                and normalized_transfer_id not in self._transfer_receipt_archive
            ):
                raise ValueError("unknown transfer")
            return normalized_transfer_id
        if intent_id is None:
            raise ValueError("intent_id or transfer_id is required")
        normalized_intent_id = str(intent_id)
        resolved = self._intent_transfers.get(normalized_intent_id)
        if resolved is None:
            resolved = self._archived_intent_transfers.get(normalized_intent_id)
        if resolved is None:
            raise ValueError("unknown transfer intent")
        return str(resolved)

    def _transfer_recovery_state_locked(
        self,
        transfer_id: str,
        *,
        status: TransferStatus,
        archived: Mapping[str, object],
        now: float,
    ) -> dict[str, object]:
        normalized = str(transfer_id)
        self._archive_transfer_receipt_state_locked(normalized)
        archived = self._transfer_receipt_archive.get(normalized, archived)
        admission = dict(self._transfer_admissions.get(normalized, {}))
        if not admission and isinstance(archived.get("admission"), Mapping):
            admission = dict(archived["admission"])
        queue_record = dict(self._transfer_queue_records.get(normalized, {}))
        if not queue_record and isinstance(archived.get("queue_record"), Mapping):
            queue_record = dict(archived["queue_record"])
        block_runtime = tuple(
            dict(record)
            for record in self._block_runtime_records.get(normalized, ())
            if isinstance(record, Mapping)
        )
        if not block_runtime and isinstance(archived.get("block_runtime"), Iterable):
            block_runtime = tuple(
                dict(record)
                for record in archived.get("block_runtime", ()) or ()
                if isinstance(record, Mapping)
            )
        ticket = self._receipt_execution_ticket_for_transfer_locked(normalized)
        reservations = tuple(
            self._runtime_reservation_record_locked(reservation_id, reservation)
            for reservation_id, reservation in sorted(self._reservations.items())
            if self._reservation_transfers.get(reservation_id) == normalized
        )
        archived_reservations = archived.get("reservations")
        if (
            not reservations
            and isinstance(archived_reservations, Iterable)
            and not isinstance(archived_reservations, (str, bytes, Mapping))
        ):
            reservations = tuple(
                dict(item)
                for item in archived_reservations
                if isinstance(item, Mapping)
            )
        leases = tuple(
            self._runtime_lease_record_locked(lease_id, lease)
            for lease_id, lease in sorted(self._lease_tokens.items())
            if self._reservation_transfers.get(lease_id) == normalized
        )
        archived_leases = archived.get("leases")
        if (
            not leases
            and isinstance(archived_leases, Iterable)
            and not isinstance(archived_leases, (str, bytes, Mapping))
        ):
            leases = tuple(
                dict(item)
                for item in archived_leases
                if isinstance(item, Mapping)
            )
        buffer_snapshots = self._transfer_buffer_snapshots.get(normalized)
        if not isinstance(buffer_snapshots, Mapping):
            buffer_snapshots = archived.get("buffer_snapshots", {})
        cleanup_targets = tuple(
            _jsonable_cleanup_target_record(target)
            for target in self._retired_cleanup_targets.values()
            if normalized
            in {str(item) for item in target.get("transfer_ids", ()) or ()}
        )
        intent = self._transfer_intents.get(str(archived.get("intent_id")))
        if intent is None and isinstance(archived.get("intent"), TransferIntent):
            intent = archived["intent"]
        decision = self._scheduling_decisions.get(normalized)
        if decision is None and isinstance(archived.get("decision"), SchedulingDecision):
            decision = archived["decision"]
        receipt = None
        if intent is not None and decision is not None:
            try:
                receipt = self._receipt_for_intent_locked(intent.intent_id)
            except ValueError:
                receipt = None
        return daemon_transfer_lifecycle.recovery_state(
            transfer_id=normalized,
            status=status,
            archived=archived,
            admission=admission,
            queue_record=queue_record,
            block_runtime=block_runtime,
            ticket=ticket,
            reservations=reservations,
            leases=leases,
            buffer_snapshots=(
                buffer_snapshots if isinstance(buffer_snapshots, Mapping) else {}
            ),
            cleanup_targets=cleanup_targets,
            receipt=receipt,
            completion_source=archived.get(
                "completion_source",
                self._transfer_completion_sources.get(normalized),
            ),
            completion_evidence=archived.get(
                "completion_evidence",
                self._transfer_completion_evidence.get(normalized, {}),
            ),
            recovered_at=now,
            archived_active=normalized in self._transfer_receipt_archive,
        )

    def _record_terminal_runtime_feedback_locked(self, transfer_id: str) -> None:
        normalized = str(transfer_id)
        record = self._transfer_queue_records.get(normalized)
        if not isinstance(record, Mapping):
            archived = self._transfer_receipt_archive.get(normalized, {})
            status = archived.get("status")
            if status is None:
                return
            completion_evidence = dict(archived.get("completion_evidence", {}) or {})
            record = {
                "transfer_id": normalized,
                "job_id": getattr(status, "job_id", None),
                "session_id": getattr(status, "session_id", None),
                "state": getattr(getattr(status, "state", None), "value", getattr(status, "state", None)),
                "direction": str(
                    getattr(archived.get("intent"), "direction", "unknown")
                ).lower(),
                "bytes_total": int(getattr(status, "bytes_total", 0) or 0),
                "bytes_completed": int(getattr(status, "bytes_completed", 0) or 0),
                "completion_source": archived.get("completion_source"),
                "completion_evidence": completion_evidence,
                "workload_kind": getattr(
                    getattr(archived.get("intent"), "workload_kind", None),
                    "value",
                    None,
                ),
                "priority": int(getattr(archived.get("intent"), "priority", 0) or 0),
            }
        feedback = _terminal_feedback_record_from_record(
            record,
            recorded_at=time.time(),
        )
        if feedback is None:
            return
        self._recent_terminal_feedback_records[normalized] = feedback
        self._recent_terminal_feedback_order = [
            transfer_id
            for transfer_id in self._recent_terminal_feedback_order
            if transfer_id != normalized
        ]
        self._recent_terminal_feedback_order.append(normalized)
        while (
            len(self._recent_terminal_feedback_order)
            > int(self._recent_terminal_feedback_capacity)
        ):
            stale_transfer_id = self._recent_terminal_feedback_order.pop(0)
            self._recent_terminal_feedback_records.pop(stale_transfer_id, None)
        self._runtime_state_version += 1

    def _retire_completed_transfer_lease_state_locked(
        self,
        transfer_id: str,
        *,
        reason: str | None = None,
    ) -> None:
        normalized = str(transfer_id)
        self._archive_transfer_receipt_state_locked(normalized)
        self._record_terminal_runtime_feedback_locked(normalized)
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

    def _record_failure_cleanup_contract_locked(
        self,
        *,
        transfer_id: str,
        final_state: TransferStatusState,
        error: str,
        removed: Mapping[str, object],
        promoted: Iterable[Mapping[str, object]],
    ) -> None:
        normalized = str(transfer_id)
        contract = daemon_transfer_lifecycle.failure_cleanup_contract(
            transfer_id=normalized,
            final_state=final_state,
            error=error,
            removed=removed,
            promoted_transfers=promoted,
            recorded_at=time.time(),
            active_ticket_retained=normalized in self._transfer_tickets,
            active_reservation_count=sum(
                1
                for mapped_transfer_id in self._reservation_transfers.values()
                if mapped_transfer_id == normalized
            ),
            active_staging_count=sum(
                1
                for record in self._staging_records.values()
                if str(record.get("transfer_id", "")) == normalized
            ),
        )
        self._merge_failure_cleanup_contract_into_evidence_locked(
            transfer_id=normalized,
            contract=contract,
        )
        self._archive_transfer_receipt_state_locked(normalized)
        self._record_terminal_runtime_feedback_locked(normalized)

    def _merge_failure_cleanup_contract_into_evidence_locked(
        self,
        *,
        transfer_id: str,
        contract: Mapping[str, object],
    ) -> None:
        normalized = str(transfer_id)
        active_evidence = self._transfer_completion_evidence.get(normalized)
        if isinstance(active_evidence, Mapping):
            updated = dict(active_evidence)
            updated["failure_cleanup_contract"] = dict(contract)
            cleanup = updated.get("cleanup")
            if isinstance(cleanup, Mapping):
                cleanup_record = dict(cleanup)
            else:
                cleanup_record = {}
            cleanup_record.setdefault("ok", True)
            cleanup_record["failure_cleanup_contract"] = dict(contract)
            updated["cleanup"] = cleanup_record
            self._transfer_completion_evidence[normalized] = updated
            return
        archived = self._transfer_receipt_archive.get(normalized)
        if not isinstance(archived, Mapping):
            return
        archived_evidence = dict(archived.get("completion_evidence", {}) or {})
        archived_evidence["failure_cleanup_contract"] = dict(contract)
        cleanup = archived_evidence.get("cleanup")
        cleanup_record = dict(cleanup) if isinstance(cleanup, Mapping) else {}
        cleanup_record.setdefault("ok", True)
        cleanup_record["failure_cleanup_contract"] = dict(contract)
        archived_evidence["cleanup"] = cleanup_record
        updated_archive = dict(archived)
        updated_archive["completion_evidence"] = archived_evidence
        self._transfer_receipt_archive[normalized] = updated_archive

    def _pop_transfer_runtime_maps_locked(self, transfer_id: str) -> bool:
        normalized = str(transfer_id)
        removed = False
        for mapping in (
            self._transfer_completion_tickets,
            self._transfer_completion_sources,
            self._transfer_completion_evidence,
            self._transfer_buffer_snapshots,
            self._transfer_plan_requests,
            self._transfer_plan_generations,
            self._transfer_plan_expirations,
            self._transfer_plans,
            self._block_runtime_records,
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
        owned_cleanup_targets: Mapping[str, object] | None = None,
    ) -> Session | None:
        session = self._sessions.pop(session_id, None)
        if session is None:
            return None
        if owned_cleanup_targets is None:
            owned_cleanup_targets = self._session_owned_cleanup_targets_locked(session_id)
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
        removed_jobs = self._remove_session_jobs_and_buffers_locked(
            session_id,
            reason=reason,
            owned_cleanup_targets=owned_cleanup_targets,
        )
        if removed is not None:
            removed["sessions"] = int(removed["sessions"]) + 1
            removed["jobs"] = int(removed["jobs"]) + removed_jobs["jobs"]
            removed["buffers"] = int(removed["buffers"]) + removed_jobs["buffers"]
        self._runtime_state_version += 1
        return session

    def _remove_session_jobs_and_buffers_locked(
        self,
        session_id: str,
        *,
        reason: str,
        owned_cleanup_targets: Mapping[str, object] | None = None,
    ) -> dict[str, int]:
        cleanup_targets = (
            {
                "jobs": (),
                "buffers": (),
            }
            if owned_cleanup_targets is None
            else {
                "jobs": tuple(owned_cleanup_targets.get("jobs", ()) or ()),
                "buffers": tuple(owned_cleanup_targets.get("buffers", ()) or ()),
            }
        )
        for target in cleanup_targets["jobs"]:
            self._archive_cleanup_target_locked(
                target_kind=str(target.get("target_kind", "job")),
                target_id=str(target["target_id"]),
                peer_identity=target.get("peer_identity"),
                reason=reason,
                transfer_ids=tuple(str(item) for item in target.get("transfer_ids", ())),
            )
        for target in cleanup_targets["buffers"]:
            self._archive_cleanup_target_locked(
                target_kind=str(target.get("target_kind", "buffer")),
                target_id=str(target["target_id"]),
                peer_identity=target.get("peer_identity"),
                reason=reason,
                transfer_ids=tuple(str(item) for item in target.get("transfer_ids", ())),
                buffer_snapshot=target.get("buffer_snapshot"),
            )
        job_ids = {
            str(target["target_id"]) for target in cleanup_targets["jobs"]
        }
        if not job_ids:
            job_ids = {
                job_id
                for job_id, job in self._jobs.items()
                if job.session_id == session_id
            }
        removed = {"jobs": 0, "buffers": 0}
        for job_id in sorted(job_ids):
            if self._jobs.pop(job_id, None) is not None:
                removed["jobs"] += 1
            self._job_peer_identities.pop(job_id, None)
        buffer_ids = {
            str(target["target_id"]) for target in cleanup_targets["buffers"]
        }
        if not buffer_ids:
            buffer_ids = {
                buffer_id
                for buffer_id, buffer in self._buffers.items()
                if buffer.job_id in job_ids
            }
        for buffer_id in sorted(buffer_ids):
            if self._buffers.pop(buffer_id, None) is not None:
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
        cleaned_by_connection: set[str] = set()
        if connection_id is not None:
            normalized_connection_id = str(connection_id)
            for session_id in sorted(tuple(self._connection_scoped_sessions)):
                session_connection_id = self._connection_scoped_session_connections.get(
                    session_id
                )
                if session_connection_id != normalized_connection_id:
                    continue
                self._close_session_locked(session_id, reason=reason, removed=removed)
                cleaned_by_connection.add(session_id)
        if peer_identity is None:
            return removed
        for session_id in sorted(tuple(self._connection_scoped_sessions)):
            if session_id in cleaned_by_connection:
                continue
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

    def _profile_topology_binding_locked(
        self,
        *,
        target_gpu: int,
        relay_gpus: Iterable[int],
    ) -> dict[str, object]:
        if self._topology_provider is None:
            raise ValueError(_TOPOLOGY_UNAVAILABLE_ERROR)
        inventory = self._topology_provider.snapshot()
        requested_relays = tuple(self._normalize_relays(relay_gpus))
        relay_eligibility = self._relay_eligibility_for_target_locked(
            target_gpu=int(target_gpu),
            requested_relays=requested_relays,
            inventory=inventory,
        )
        pcie_fabric = pcie_fabric_snapshot_from_inventory(inventory).as_dict()
        eligible_by_relay = {
            int(item["relay_gpu"]): dict(item)
            for item in relay_eligibility["eligible_relays"]
        }
        missing_relays = [
            int(relay)
            for relay in requested_relays
            if int(relay) not in eligible_by_relay
        ]
        if missing_relays:
            raise ValueError(
                "profile relay devices must be trusted topology-eligible: "
                + ",".join(str(relay) for relay in missing_relays)
            )
        relay_bindings = []
        for relay in requested_relays:
            record = dict(eligible_by_relay[int(relay)])
            topology = dict(record.get("topology", {}) or {})
            if not bool(topology.get("pcie_trusted", False)):
                raise ValueError(
                    f"profile relay {relay} is missing trusted PCIe topology"
                )
            if not bool(topology.get("fabric_trusted", False)):
                raise ValueError(
                    f"profile relay {relay} is missing trusted fabric topology"
                )
            relay_bindings.append(
                {
                    "relay_gpu": int(relay),
                    "reason": str(record.get("reason", "eligible")),
                    "topology": topology,
                }
            )
        return {
            "source": "daemon_trusted_topology",
            "topology_snapshot_id": inventory.topology_snapshot_id(),
            "topology_version": inventory.version,
            "inventory_source": inventory.source,
            "inventory_discovered_at": inventory.discovered_at,
            "pcie_fabric": pcie_fabric,
            "target_gpu": int(target_gpu),
            "relay_gpus": list(requested_relays),
            "relay_topology": relay_bindings,
        }

    def _profile_matches_current_topology_locked(
        self,
        entry: Mapping[str, object],
        *,
        target_gpu: int,
        relay_gpus: Iterable[int],
    ) -> bool:
        binding = entry.get("topology_binding")
        if not isinstance(binding, Mapping):
            return False
        if self._topology_provider is None:
            return False
        try:
            current = self._profile_topology_binding_locked(
                target_gpu=int(target_gpu),
                relay_gpus=relay_gpus,
            )
        except ValueError:
            return False
        if str(binding.get("topology_snapshot_id", "")) != str(
            current.get("topology_snapshot_id", "")
        ):
            return False
        if int(binding.get("topology_version", -1) or -1) != int(
            current.get("topology_version", -2) or -2
        ):
            return False
        return tuple(int(item) for item in binding.get("relay_gpus", ()) or ()) == tuple(
            int(item) for item in current.get("relay_gpus", ()) or ()
        )

    def _trusted_profile_entry_locked(
        self,
        *,
        target_gpu: int,
        planning_relays: Iterable[int],
        fallback_relays: Iterable[int],
    ) -> dict | None:
        planning_key = self._profile_key(target_gpu, planning_relays)
        entry = daemon_profiles.cached_profile(self._profile_cache, planning_key)
        if entry is not None and self._profile_matches_current_topology_locked(
            entry,
            target_gpu=int(target_gpu),
            relay_gpus=planning_relays,
        ):
            return entry
        if entry is not None:
            daemon_profiles.invalidate_cached_profile(self._profile_cache, planning_key)
        planning_tuple = tuple(int(item) for item in planning_relays)
        fallback_tuple = tuple(int(item) for item in fallback_relays)
        if planning_tuple == fallback_tuple:
            return None
        fallback_key = self._profile_key(target_gpu, fallback_tuple)
        fallback_entry = daemon_profiles.cached_profile(self._profile_cache, fallback_key)
        if fallback_entry is not None and self._profile_matches_current_topology_locked(
            fallback_entry,
            target_gpu=int(target_gpu),
            relay_gpus=fallback_tuple,
        ):
            return fallback_entry
        if fallback_entry is not None:
            daemon_profiles.invalidate_cached_profile(self._profile_cache, fallback_key)
        return None

    def describe(
        self,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        with self._lock:
            now = time.time()
            self._reap_stale_sessions_locked(now)
            self._refresh_admission_state_locked(now=now)
            self._purge_stale_profiles_locked(now)
            runtime_state = self._runtime_resource_state_locked(now=now)
            return DaemonResponse(
                ok=True,
                payload={
                    "jobs": {key: asdict(value) for key, value in self._jobs.items()},
                    "job_peer_identities": {
                        key: asdict(value)
                        for key, value in self._job_peer_identities.items()
                    },
                    "buffers": self._public_buffer_records_locked(),
                    "buffer_records_are_redacted": True,
                    "buffer_ownership": {
                        key: self._buffer_ownership_record_locked(key)
                        for key in sorted(self._buffers)
                    },
                    "sessions": {key: asdict(value) for key, value in self._sessions.items()},
                    "session_peer_identities": {
                        key: asdict(value)
                        for key, value in self._session_peer_identities.items()
                    },
                    "reservations": {
                        key: asdict(value) for key, value in self._reservations.items()
                    },
                    "staging_records": {
                        key: _redact_public_payload(dict(value))
                        for key, value in self._staging_records.items()
                    },
                    "audit_records": [
                        _redact_public_payload(dict(record))
                        for record in self._audit_records
                    ],
                    "connection_scoped_sessions": sorted(
                        self._connection_scoped_sessions
                    ),
                    "transfer_statuses": {
                        key: asdict(value) for key, value in self._transfer_statuses.items()
                    },
                    "transfer_queue": [
                        _redact_public_payload(
                            dict(self._transfer_queue_records[transfer_id])
                        )
                        for transfer_id in self._transfer_queue
                        if transfer_id in self._transfer_queue_records
                    ],
                    "runtime_resource_state": _redact_public_payload(runtime_state),
                    "hardware_monitoring": runtime_state.get("hardware_monitoring", {}),
                    "security_policy": {
                        "source": "daemon_security_policy",
                        "require_authenticated_peers": self._require_authenticated_peers,
                        "socket": self._socket_security_policy.as_dict(),
                    },
                    "socket_security": (
                        None
                        if self._last_socket_security_record is None
                        else self._last_socket_security_record.as_dict()
                    ),
                    "tenant_quota_policy": dict(self._tenant_quota_policy),
                    "tenant_usage": self._tenant_usage_snapshot_locked(),
                    "quota_rejections": tuple(self._quota_rejections[-128:]),
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
                        key: _redact_public_payload(dict(value))
                        for key, value in self._profile_cache.items()
                    },
                    "require_authenticated_peers": self._require_authenticated_peers,
                    "requester_peer_identity": (
                        None if peer_identity is None else asdict(peer_identity)
                    ),
                },
            )

    def runtime_telemetry(
        self,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        with self._lock:
            now = time.time()
            self._reap_stale_sessions_locked(now)
            self._refresh_admission_state_locked(now=now)
            runtime_state = self._runtime_resource_state_locked(now=now)
            telemetry = _daemon_runtime_telemetry_snapshot(
                runtime_state=runtime_state,
                relay_quotas=self._relay_quotas,
                sessions=self._sessions,
                jobs=self._jobs,
                requester_peer_identity=peer_identity,
            )
            return DaemonResponse(ok=True, payload={"runtime_telemetry": telemetry})

    def handle_request(
        self,
        request: DaemonRequest,
        connection_id: str | None = None,
    ) -> DaemonResponse:
        return self._request_router.handle_request(
            request,
            connection_id=connection_id,
        )

    def _requires_authenticated_peer_for_request(self, request: DaemonRequest) -> bool:
        if not self._require_authenticated_peers:
            return False
        protected = {
            RequestType.REGISTER_SESSION,
            RequestType.REGISTER_JOB,
            RequestType.REGISTER_BUFFER,
            RequestType.SUBMIT_TRANSFER_INTENT,
            RequestType.VALIDATE_LEASE,
            RequestType.AUTHORIZE_WORKER_TRANSFER,
            RequestType.CLEANUP,
        }
        if request.request_type not in protected:
            return False
        peer_identity = request.peer_identity
        return peer_identity is None or not peer_identity.authenticated

    def _handle_request_impl(
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
                eligible_relays.append(dict(item))
            else:
                filtered_relays.append(
                    {
                        **dict(item),
                        "relay_gpu": relay_gpu,
                        "reason": "relay not configured",
                    }
                )
        return {
            **relay_eligibility,
            "topology_snapshot_id": inventory.topology_snapshot_id(),
            "topology_version": inventory.version,
            "fabric_capability_summary": planning_helpers.fabric_capability_summary_with_snapshot(
                relay_eligibility.get("fabric_capability_summary", {}),
                inventory=inventory,
            ),
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
                "trusted_topology_relay_count": sum(
                    1
                    for item in relay_records
                    if bool(
                        item["inventory"]["path_capabilities"].get(
                            "topology_trusted",
                            False,
                        )
                    )
                ),
                "trusted_pcie_relay_count": sum(
                    1
                    for item in relay_records
                    if bool(
                        item["inventory"]["path_capabilities"].get(
                            "pcie_trusted",
                            False,
                        )
                    )
                ),
                "trusted_fabric_relay_count": sum(
                    1
                    for item in relay_records
                    if bool(
                        item["inventory"]["path_capabilities"].get(
                            "fabric_trusted",
                            False,
                        )
                    )
                ),
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
            "path_capabilities": planning_helpers.relay_path_capabilities(
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
        self._last_socket_security_record = secure_unix_socket(
            socket_path,
            policy=self._socket_security_policy,
        )
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


def _block_plan_runtime_record(plan: Mapping[str, object]) -> dict[str, object]:
    block_plan = plan.get("block_plan")
    if not isinstance(block_plan, Mapping):
        return {
            "source": "daemon_scheduler_block_plan",
            "available": False,
            "block_count": 0,
            "path_count": 0,
        }
    blocks = tuple(
        item for item in block_plan.get("blocks", ()) or () if isinstance(item, Mapping)
    )
    paths = tuple(
        item for item in block_plan.get("paths", ()) or () if isinstance(item, Mapping)
    )
    path_kinds = {
        str(path.get("path_id")): str(path.get("kind", "unknown"))
        for path in paths
    }
    block_bytes_by_kind: dict[str, int] = {}
    for block in blocks:
        kind = path_kinds.get(str(block.get("path_id")), "unknown")
        block_bytes_by_kind[kind] = block_bytes_by_kind.get(kind, 0) + int(
            block.get("bytes", 0) or 0
        )
    return {
        "source": "daemon_scheduler_block_plan",
        "available": True,
        "plan_id": block_plan.get("plan_id"),
        "direction": block_plan.get("direction"),
        "block_count": len(blocks),
        "path_count": len(paths),
        "bytes_by_path_kind": block_bytes_by_kind,
        "metadata": dict(block_plan.get("metadata", {}) or {}),
    }


def _block_queue_runtime_record(plan: Mapping[str, object]) -> dict[str, object]:
    block_plan = plan.get("block_plan")
    if not isinstance(block_plan, Mapping):
        return {
            "source": "daemon_scheduler_block_queue",
            "available": False,
            "block_count": 0,
            "states": {},
            "bytes_by_state": {},
        }
    queue_records = _queue_records_for_block_plan(
        _block_plan_from_mapping(block_plan),
    )
    return {
        **_block_queue_summary(queue_records),
        "available": True,
        "records": tuple(record.as_dict() for record in queue_records),
    }


def _empty_tenant_usage() -> dict[str, int]:
    return {
        "active_sessions": 0,
        "registered_jobs": 0,
        "registered_buffers": 0,
        "registered_buffer_bytes": 0,
        "active_leases": 0,
        "active_transfers": 0,
    }


def _redact_public_payload(value: object) -> object:
    if isinstance(value, Mapping):
        redacted: dict[str, object] = {}
        for raw_key, raw_item in value.items():
            key = str(raw_key)
            if _public_field_is_sensitive(key):
                redacted[key] = _PUBLIC_REDACTED_VALUE
            else:
                redacted[key] = _redact_public_payload(raw_item)
        return redacted
    if isinstance(value, tuple):
        return tuple(_redact_public_payload(item) for item in value)
    if isinstance(value, list):
        return [_redact_public_payload(item) for item in value]
    return value


def _public_payload_has_redaction(value: object) -> bool:
    if value == _PUBLIC_REDACTED_VALUE:
        return True
    if isinstance(value, Mapping):
        return any(_public_payload_has_redaction(item) for item in value.values())
    if isinstance(value, (tuple, list)):
        return any(_public_payload_has_redaction(item) for item in value)
    return False


def _public_field_is_sensitive(field_name: str) -> bool:
    normalized = str(field_name).strip().lower()
    return normalized in _PUBLIC_SENSITIVE_FIELD_NAMES or any(
        fragment in normalized for fragment in _PUBLIC_SENSITIVE_FIELD_FRAGMENTS
    )


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
        "admitted_transfers",
        "delayed_transfers",
        "running_transfers",
        "active_transfers",
        "recent_terminal_transfers",
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


def _intent_transfer_mode(intent: TransferIntent) -> str:
    for source in (intent.policy_hints, intent.metadata):
        if not isinstance(source, dict):
            continue
        value = source.get("transfer_mode")
        if value is None:
            continue
        mode = str(value).lower()
        if mode == "pooled":
            mode = "pool"
        if mode == "direct-only":
            mode = "direct"
        if mode not in {"auto", "pool", "direct", "relay"}:
            raise ValueError("transfer_mode must be one of auto, pool, direct, relay")
        return mode
    return "auto"


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
