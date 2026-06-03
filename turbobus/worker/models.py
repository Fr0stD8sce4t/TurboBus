from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field
from enum import Enum

from ..schema import (
    BufferRegistration,
    DaemonResponse,
    ExecutionTicket,
    SchedulingDecision,
    TransferStatusState,
    WorkerDataPlaneCompletion,
    WorkerDataPlaneRequest,
    WorkerTransferAuthorization,
    WorkerTransferAuthorizationRequest,
)
from .staging_pool import WorkerStagingSlot
from . import validation as worker_validation


class WorkerTransferState(str, Enum):
    FAILED = "failed"
    COMPLETE = "complete"


@dataclass(frozen=True)
class WorkerTransferRequest:
    authorization: WorkerTransferAuthorization
    ticket: ExecutionTicket
    data_plane: WorkerDataPlaneRequest | None = None

    @classmethod
    def from_authorization_payload(
        cls,
        payload: Mapping[str, object],
        *,
        now: float | None = None,
    ) -> "WorkerTransferRequest":
        if not isinstance(payload, Mapping):
            raise ValueError("authorization payload must be a mapping")
        if payload.get("ticket") is None:
            raise ValueError(
                "daemon worker authorization response must include execution ticket"
            )
        return cls.from_execution_ticket_payload(payload, now=now)

    @classmethod
    def from_execution_ticket_payload(
        cls,
        payload: Mapping[str, object],
        *,
        now: float | None = None,
    ) -> "WorkerTransferRequest":
        if not isinstance(payload, Mapping):
            raise ValueError("execution ticket payload must be a mapping")
        ticket_payload = payload.get("ticket")
        if not isinstance(ticket_payload, Mapping):
            raise ValueError("execution ticket payload must include ticket")
        decision_payload = payload.get("decision")
        return cls.from_execution_ticket(
            ExecutionTicket(**dict(ticket_payload)),
            src_buffer=buffer_from_payload(payload["src_buffer"]),
            dst_buffer=buffer_from_payload(payload["dst_buffer"]),
            relay_gpu=payload.get("relay_gpu"),
            relay_gpus=payload.get("relay_gpus"),
            lease_id=payload.get("lease_id"),
            lease_ids=payload.get("lease_ids"),
            transfer_id=payload.get("transfer_id"),
            decision=(
                None
                if decision_payload is None
                else SchedulingDecision(**dict(decision_payload))
            ),
            plan_generation=payload.get("plan_generation"),
            now=now,
        )

    @classmethod
    def from_authorization(
        cls,
        authorization: WorkerTransferAuthorization,
    ) -> "WorkerTransferRequest":
        raise ValueError(
            "worker transfer requests must be built from daemon-issued "
            "ExecutionTicket objects"
        )

    @classmethod
    def from_execution_ticket(
        cls,
        ticket: ExecutionTicket,
        *,
        src_buffer: BufferRegistration,
        dst_buffer: BufferRegistration,
        relay_gpu: int | None = None,
        relay_gpus: Iterable[int] | None = None,
        lease_id: str | None = None,
        lease_ids: Iterable[str] | None = None,
        transfer_id: str | None = None,
        decision: SchedulingDecision | None = None,
        plan_generation: object | None = None,
        now: float | None = None,
    ) -> "WorkerTransferRequest":
        worker_validation.validate_ticket_matches_buffers(ticket, src_buffer, dst_buffer)
        worker_validation.validate_daemon_issued_ticket(
            ticket,
            plan_generation=plan_generation,
            now=now,
        )
        if decision is not None:
            worker_validation.validate_ticket_matches_decision(ticket, decision)
        resolved_relays = worker_validation.relay_gpus_for_ticket(
            ticket,
            relay_gpu=relay_gpu,
            relay_gpus=relay_gpus,
        )
        relay = int(relay_gpu) if relay_gpu is not None else resolved_relays[0]
        if relay not in resolved_relays:
            raise ValueError("ticket relay does not match daemon plan")
        ranges = worker_validation.relay_ranges_from_ticket_plan(
            ticket,
            relay_gpus=resolved_relays,
        )
        resolved_lease_ids = worker_validation.lease_ids_for_ticket(
            ticket,
            lease_id=lease_id,
            lease_ids=lease_ids,
        )
        resolved_lease_id = (
            str(lease_id) if lease_id is not None else resolved_lease_ids[0]
        )
        resolved_transfer_id = worker_validation.transfer_id_for_ticket(
            ticket,
            transfer_id,
        )
        authorization = WorkerTransferAuthorization(
            transfer_id=resolved_transfer_id,
            lease_id=resolved_lease_id,
            session_id=ticket.session_id,
            job_id=ticket.job_id,
            src_buffer=src_buffer,
            dst_buffer=dst_buffer,
            direction=ticket.direction,
            ranges=ranges,
            relay_gpu=relay,
            plan=dict(ticket.plan),
        )
        metadata = {
            "relay_gpus": resolved_relays,
            "lease_ids": resolved_lease_ids,
            "primary_relay_gpu": relay,
            "primary_lease_id": resolved_lease_id,
            "relay_ranges_by_gpu": worker_validation.relay_ranges_by_gpu_for_ticket(
                ticket,
                relay_gpus=resolved_relays,
            ),
        }
        if now is not None:
            metadata["ticket_authorized_at"] = float(now)
        data_plane = WorkerDataPlaneRequest.from_authorization(
            authorization,
            metadata=metadata,
        )
        return cls(
            authorization=authorization,
            data_plane=data_plane,
            ticket=ticket,
        )

    def __post_init__(self) -> None:
        if not isinstance(self.authorization, WorkerTransferAuthorization):
            raise TypeError("authorization must be a WorkerTransferAuthorization")
        data_plane = self.data_plane
        if data_plane is None:
            data_plane = WorkerDataPlaneRequest.from_authorization(self.authorization)
        if not isinstance(data_plane, WorkerDataPlaneRequest):
            raise TypeError("data_plane must be a WorkerDataPlaneRequest")
        if data_plane.transfer_id != self.authorization.transfer_id:
            raise ValueError("data-plane transfer id does not match authorization")
        if data_plane.lease_id != self.authorization.lease_id:
            raise ValueError("data-plane lease id does not match authorization")
        if data_plane.session_id != self.authorization.session_id:
            raise ValueError("data-plane session id does not match authorization")
        if data_plane.job_id != self.authorization.job_id:
            raise ValueError("data-plane job id does not match authorization")
        if data_plane.relay_gpu != self.authorization.relay_gpu:
            raise ValueError("data-plane relay does not match authorization")
        if data_plane.direction != self.authorization.direction:
            raise ValueError("data-plane direction does not match authorization")
        if data_plane.src_handle.buffer_id != self.authorization.src_buffer.buffer_id:
            raise ValueError("data-plane src handle does not match authorization")
        if data_plane.dst_handle.buffer_id != self.authorization.dst_buffer.buffer_id:
            raise ValueError("data-plane dst handle does not match authorization")
        if data_plane.ranges != self.authorization.ranges:
            raise ValueError("data-plane ranges do not match authorization")
        if data_plane.plan != self.authorization.plan:
            raise ValueError("data-plane plan does not match authorization")
        if not isinstance(self.ticket, ExecutionTicket):
            raise TypeError("ticket must be an ExecutionTicket")
        worker_validation.validate_daemon_issued_ticket(self.ticket)
        worker_validation.validate_ticket_matches_worker_request(
            self.ticket,
            self.authorization,
            data_plane,
        )
        object.__setattr__(self, "data_plane", data_plane)

    @property
    def transfer_id(self) -> str:
        return self.authorization.transfer_id

    def as_dict(self) -> dict[str, object]:
        return {
            "authorization": asdict(self.authorization),
            "data_plane": asdict(self.data_plane),
            "ticket": asdict(self.ticket),
        }


@dataclass(frozen=True)
class WorkerTransferResult:
    transfer_id: str
    state: WorkerTransferState
    error: str | None = None
    bytes_completed: int = 0
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.transfer_id).strip():
            raise ValueError("transfer_id must be non-empty")
        bytes_completed = int(self.bytes_completed)
        if bytes_completed < 0:
            raise ValueError("bytes_completed must be non-negative")
        object.__setattr__(self, "transfer_id", str(self.transfer_id))
        object.__setattr__(self, "state", WorkerTransferState(self.state))
        object.__setattr__(self, "bytes_completed", bytes_completed)
        if self.error is not None:
            object.__setattr__(self, "error", str(self.error))
        object.__setattr__(self, "metadata", dict(self.metadata))

    def as_dict(self) -> dict[str, object]:
        return {
            "transfer_id": self.transfer_id,
            "state": self.state.value,
            "error": self.error,
            "bytes_completed": self.bytes_completed,
            "metadata": dict(self.metadata),
        }

    def data_plane_completion(self, lease_id: str) -> WorkerDataPlaneCompletion:
        status_update = daemon_status_update_for_result(self)
        return WorkerDataPlaneCompletion(
            transfer_id=self.transfer_id,
            lease_id=lease_id,
            state=status_update["state"],
            bytes_completed=self.bytes_completed,
            error=(
                status_update["error"]
                if status_update["state"] == TransferStatusState.FAILED.value
                else None
            ),
            metadata=dict(self.metadata),
        )


@dataclass(frozen=True)
class WorkerTransferLifecycleRecord:
    authorization_request: WorkerTransferAuthorizationRequest
    worker_request: WorkerTransferRequest | None = None
    staging_slot: WorkerStagingSlot | None = None
    staging_release: WorkerStagingSlot | None = None
    result: WorkerTransferResult | None = None
    status_update: Mapping[str, object] | None = None
    status_response: DaemonResponse | None = None
    cleanup_target_kind: str | None = None
    cleanup_target_id: str | None = None
    cleanup_response: DaemonResponse | None = None
    final_state: str = "created"
    error: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.authorization_request, WorkerTransferAuthorizationRequest):
            raise TypeError("authorization_request must be a WorkerTransferAuthorizationRequest")
        if self.worker_request is not None and not isinstance(
            self.worker_request,
            WorkerTransferRequest,
        ):
            raise TypeError("worker_request must be a WorkerTransferRequest")
        if self.staging_slot is not None and not isinstance(
            self.staging_slot,
            WorkerStagingSlot,
        ):
            raise TypeError("staging_slot must be a WorkerStagingSlot")
        if self.staging_release is not None and not isinstance(
            self.staging_release,
            WorkerStagingSlot,
        ):
            raise TypeError("staging_release must be a WorkerStagingSlot")
        if self.result is not None and not isinstance(self.result, WorkerTransferResult):
            raise TypeError("result must be a WorkerTransferResult")
        if self.status_update is not None and not isinstance(self.status_update, Mapping):
            raise TypeError("status_update must be a mapping")
        if self.status_response is not None and not isinstance(
            self.status_response,
            DaemonResponse,
        ):
            raise TypeError("status_response must be a DaemonResponse")
        if self.cleanup_response is not None and not isinstance(
            self.cleanup_response,
            DaemonResponse,
        ):
            raise TypeError("cleanup_response must be a DaemonResponse")
        final_state = str(self.final_state)
        if not final_state.strip():
            raise ValueError("final_state must be non-empty")
        object.__setattr__(self, "final_state", final_state)
        if self.cleanup_target_kind is not None:
            object.__setattr__(self, "cleanup_target_kind", str(self.cleanup_target_kind))
        if self.cleanup_target_id is not None:
            object.__setattr__(self, "cleanup_target_id", str(self.cleanup_target_id))
        if self.error is not None:
            object.__setattr__(self, "error", str(self.error))
        if self.status_update is not None:
            object.__setattr__(self, "status_update", dict(self.status_update))

    def as_dict(self) -> dict[str, object]:
        lease_ids = (
            worker_request_lease_ids(self.worker_request)
            if self.worker_request is not None
            else (self.authorization_request.lease_id,)
        )
        cleanup_target = None
        if self.cleanup_target_kind is not None or self.cleanup_target_id is not None:
            cleanup_target = {
                "target_kind": self.cleanup_target_kind,
                "target_id": self.cleanup_target_id,
            }
        return {
            "authorization_request": asdict(self.authorization_request),
            "worker_request": (
                self.worker_request.as_dict()
                if self.worker_request is not None
                else None
            ),
            "staging_slot": (
                self.staging_slot.as_dict()
                if self.staging_slot is not None
                else None
            ),
            "staging_release": (
                self.staging_release.as_dict()
                if self.staging_release is not None
                else None
            ),
            "result": self.result.as_dict() if self.result is not None else None,
            "status_update": (
                dict(self.status_update)
                if self.status_update is not None
                else None
            ),
            "status_response": (
                asdict(self.status_response)
                if self.status_response is not None
                else None
            ),
            "cleanup_target": cleanup_target,
            "cleanup_response": (
                asdict(self.cleanup_response)
                if self.cleanup_response is not None
                else None
            ),
            "lease_ids": lease_ids,
            "final_state": self.final_state,
            "error": self.error,
        }

    def completion_envelope(self) -> "WorkerDataPlaneCompletionEnvelope":
        return WorkerDataPlaneCompletionEnvelope.from_lifecycle(self)


@dataclass(frozen=True)
class WorkerDataPlaneCompletionEnvelope:
    ok: bool
    transfer_id: str | None = None
    lease_id: str | None = None
    lease_ids: tuple[str, ...] = ()
    final_state: str | None = None
    staging_slot: Mapping[str, object] | None = None
    worker_result: Mapping[str, object] | None = None
    daemon_status_update: Mapping[str, object] | None = None
    daemon_status_response: Mapping[str, object] | None = None
    daemon_cleanup_response: Mapping[str, object] | None = None
    staging_release: Mapping[str, object] | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "ok", bool(self.ok))
        for field_name in (
            "staging_slot",
            "worker_result",
            "daemon_status_update",
            "daemon_status_response",
            "daemon_cleanup_response",
            "staging_release",
        ):
            value = getattr(self, field_name)
            if value is None:
                continue
            if not isinstance(value, Mapping):
                raise TypeError(f"{field_name} must be a mapping")
            object.__setattr__(self, field_name, dict(value))
        if self.transfer_id is not None:
            object.__setattr__(self, "transfer_id", str(self.transfer_id))
        if self.lease_id is not None:
            object.__setattr__(self, "lease_id", str(self.lease_id))
        object.__setattr__(
            self,
            "lease_ids",
            tuple(str(item) for item in self.lease_ids),
        )
        if (
            self.lease_ids
            and self.lease_id is not None
            and self.lease_id not in self.lease_ids
        ):
            raise ValueError("lease_id must be included in lease_ids")
        if self.final_state is not None:
            object.__setattr__(self, "final_state", str(self.final_state))
        if self.error is not None:
            object.__setattr__(self, "error", str(self.error))

    @classmethod
    def from_lifecycle(
        cls,
        lifecycle: WorkerTransferLifecycleRecord,
    ) -> "WorkerDataPlaneCompletionEnvelope":
        if not isinstance(lifecycle, WorkerTransferLifecycleRecord):
            raise TypeError("lifecycle must be a WorkerTransferLifecycleRecord")
        payload = lifecycle.as_dict()
        return cls(
            ok=True,
            transfer_id=lifecycle_transfer_id(lifecycle),
            lease_id=lifecycle_lease_id(lifecycle),
            lease_ids=lifecycle_lease_ids(lifecycle),
            final_state=lifecycle.final_state,
            staging_slot=payload["staging_slot"],
            worker_result=payload["result"],
            daemon_status_update=payload["status_update"],
            daemon_status_response=payload["status_response"],
            daemon_cleanup_response=payload["cleanup_response"],
            staging_release=payload["staging_release"],
            error=lifecycle.error,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "transfer_id": self.transfer_id,
            "lease_id": self.lease_id,
            "lease_ids": self.lease_ids,
            "final_state": self.final_state,
            "staging_slot": (
                dict(self.staging_slot) if self.staging_slot is not None else None
            ),
            "worker_result": (
                dict(self.worker_result) if self.worker_result is not None else None
            ),
            "daemon_status_update": (
                dict(self.daemon_status_update)
                if self.daemon_status_update is not None
                else None
            ),
            "daemon_status_response": (
                dict(self.daemon_status_response)
                if self.daemon_status_response is not None
                else None
            ),
            "daemon_cleanup_response": (
                dict(self.daemon_cleanup_response)
                if self.daemon_cleanup_response is not None
                else None
            ),
            "staging_release": (
                dict(self.staging_release)
                if self.staging_release is not None
                else None
            ),
            "error": self.error,
        }


@dataclass(frozen=True)
class WorkerServiceRequestEnvelope:
    payload: Mapping[str, object]
    cleanup_target_kind: str = "reservation"

    def __post_init__(self) -> None:
        if not isinstance(self.payload, Mapping):
            raise ValueError("worker service payload must be a mapping")
        cleanup_target_kind = str(self.cleanup_target_kind)
        if cleanup_target_kind != "reservation":
            raise ValueError("cleanup_target_kind must be reservation")
        object.__setattr__(self, "payload", dict(self.payload))
        object.__setattr__(self, "cleanup_target_kind", cleanup_target_kind)

    def as_dict(self) -> dict[str, object]:
        return {
            "payload": dict(self.payload),
            "cleanup_target_kind": self.cleanup_target_kind,
        }


@dataclass(frozen=True)
class WorkerServiceResponseEnvelope:
    ok: bool
    completion: Mapping[str, object] | None = None
    error: str | None = None
    final_state: str | None = None

    def __post_init__(self) -> None:
        if self.completion is not None and not isinstance(self.completion, Mapping):
            raise TypeError("completion must be a mapping")
        object.__setattr__(self, "ok", bool(self.ok))
        if self.completion is not None:
            object.__setattr__(self, "completion", dict(self.completion))
        if self.error is not None:
            object.__setattr__(self, "error", str(self.error))
        if self.final_state is not None:
            object.__setattr__(self, "final_state", str(self.final_state))

    @classmethod
    def from_lifecycle(
        cls,
        lifecycle: WorkerTransferLifecycleRecord,
    ) -> "WorkerServiceResponseEnvelope":
        return cls(
            ok=True,
            completion=lifecycle.completion_envelope().as_dict(),
            final_state=lifecycle.final_state,
            error=lifecycle.error,
        )

    @classmethod
    def from_error(cls, error: str) -> "WorkerServiceResponseEnvelope":
        return cls(ok=False, error=str(error), final_state="parse_failed")

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "completion": (
                dict(self.completion) if self.completion is not None else None
            ),
            "error": self.error,
            "final_state": self.final_state,
        }


def buffer_from_payload(payload: object) -> BufferRegistration:
    if not isinstance(payload, Mapping):
        raise ValueError("buffer payload must be a mapping")
    return BufferRegistration(
        buffer_id=str(payload["buffer_id"]),
        job_id=str(payload["job_id"]),
        kind=str(payload["kind"]),
        size_bytes=int(payload["size_bytes"]),
        device_index=payload.get("device_index"),
        address=payload.get("address"),
        pinned=bool(payload.get("pinned", False)),
        handle_type=str(payload.get("handle_type", "registered_buffer")),
        metadata=dict(payload.get("metadata") or {}),
    )


def daemon_state_for_worker_state(state: WorkerTransferState) -> TransferStatusState:
    worker_state = WorkerTransferState(state)
    if worker_state == WorkerTransferState.COMPLETE:
        return TransferStatusState.COMPLETE
    return TransferStatusState.FAILED


def daemon_status_update_for_result(result: WorkerTransferResult) -> dict[str, object]:
    daemon_state = daemon_state_for_worker_state(result.state)
    error = result.error
    if result.state == WorkerTransferState.FAILED and error is None:
        error = "worker transfer failed"
    return {
        "transfer_id": result.transfer_id,
        "state": daemon_state.value,
        "bytes_completed": result.bytes_completed,
        "error": error,
    }


def worker_request_lease_ids(request: WorkerTransferRequest) -> tuple[str, ...]:
    lease_ids = request.data_plane.metadata.get("lease_ids")
    if lease_ids is None:
        return (request.authorization.lease_id,)
    resolved = tuple(str(item) for item in lease_ids)
    if not resolved:
        raise ValueError("worker request has no lease ids")
    if request.authorization.lease_id not in resolved:
        raise ValueError("worker request primary lease is not authorized")
    return resolved


def lifecycle_transfer_id(lifecycle: WorkerTransferLifecycleRecord) -> str:
    if lifecycle.result is not None:
        return lifecycle.result.transfer_id
    if lifecycle.worker_request is not None:
        return lifecycle.worker_request.transfer_id
    return lifecycle.authorization_request.transfer_id


def lifecycle_lease_id(lifecycle: WorkerTransferLifecycleRecord) -> str:
    if lifecycle.worker_request is not None:
        return lifecycle.worker_request.authorization.lease_id
    return lifecycle.authorization_request.lease_id


def lifecycle_lease_ids(lifecycle: WorkerTransferLifecycleRecord) -> tuple[str, ...]:
    if lifecycle.worker_request is not None:
        data_plane = lifecycle.worker_request.data_plane
        lease_ids = data_plane.metadata.get("lease_ids") if data_plane is not None else None
        if lease_ids is not None:
            resolved = tuple(str(item) for item in lease_ids)
            if resolved:
                return resolved
        return (lifecycle.worker_request.authorization.lease_id,)
    return (lifecycle.authorization_request.lease_id,)


__all__ = [
    "WorkerDataPlaneCompletionEnvelope",
    "WorkerServiceRequestEnvelope",
    "WorkerServiceResponseEnvelope",
    "WorkerTransferLifecycleRecord",
    "WorkerTransferRequest",
    "WorkerTransferResult",
    "WorkerTransferState",
    "buffer_from_payload",
    "daemon_state_for_worker_state",
    "daemon_status_update_for_result",
    "lifecycle_lease_id",
    "lifecycle_lease_ids",
    "lifecycle_transfer_id",
    "worker_request_lease_ids",
]
