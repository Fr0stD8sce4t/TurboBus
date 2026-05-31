from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict

from .protocol import JobIdentity, LeaseToken, RelayQuota, Session, TransferReservation


def active_buffer_lease_ids(
    *,
    lease_tokens: Mapping[str, LeaseToken],
    reservations: Mapping[str, TransferReservation],
    buffer_id: str,
) -> tuple[str, ...]:
    normalized = str(buffer_id)
    return tuple(
        lease_id
        for lease_id, lease in sorted(lease_tokens.items())
        if lease_id in reservations and normalized in lease.buffer_ids
    )


def runtime_reservation_record(
    *,
    reservation_id: str,
    reservation: TransferReservation,
    reservation_transfers: Mapping[str, str],
    lease_tokens: Mapping[str, LeaseToken],
) -> dict[str, object]:
    record = asdict(reservation)
    key = str(reservation_id)
    record["transfer_id"] = reservation_transfers.get(key)
    lease = lease_tokens.get(key)
    record["job_id"] = None if lease is None else lease.job_id
    record["buffer_ids"] = () if lease is None else lease.buffer_ids
    return record


def runtime_lease_record(
    *,
    lease_id: str,
    lease: LeaseToken,
    reservation_transfers: Mapping[str, str],
) -> dict[str, object]:
    return {
        "lease_id": lease.lease_id,
        "session_id": lease.session_id,
        "relay_gpu": lease.relay_gpu,
        "job_id": lease.job_id,
        "buffer_ids": lease.buffer_ids,
        "issued_at": lease.issued_at,
        "expires_at": lease.expires_at,
        "transfer_id": reservation_transfers.get(str(lease_id)),
    }


def relay_quota_record(quota: RelayQuota | None) -> dict[str, object] | None:
    if quota is None:
        return None
    return {
        "relay_gpu": quota.relay_gpu,
        "max_sessions": quota.max_sessions,
        "active_sessions": len(quota.sessions),
        "available_sessions": max(0, quota.max_sessions - len(quota.sessions)),
        "max_inflight_chunks": quota.max_inflight_chunks,
        "active_chunks": quota.active_chunks,
        "available_chunks": max(0, quota.max_inflight_chunks - quota.active_chunks),
    }


def relay_session_records(
    *,
    relay_gpu: int,
    quota: RelayQuota | None,
    sessions: Mapping[str, Session],
    jobs: Mapping[str, JobIdentity],
) -> list[dict[str, object]]:
    if quota is None:
        return []
    records = []
    for session_id in sorted(quota.sessions):
        session = sessions.get(session_id)
        if session is None:
            continue
        records.append(
            {
                "session_id": session.session_id,
                "target_gpu": session.target_gpu,
                "active": session.active,
                "active_chunks": session.active_chunks,
                "max_inflight_chunks": session.max_inflight_chunks,
                "job_ids": sorted(
                    job.job_id
                    for job in jobs.values()
                    if job.session_id == session.session_id
                ),
            }
        )
    return records


def relay_reservation_records(
    *,
    relay_gpu: int,
    reservations: Mapping[str, TransferReservation],
    lease_tokens: Mapping[str, LeaseToken],
    reservation_transfers: Mapping[str, str],
) -> list[dict[str, object]]:
    records = []
    for reservation_id, reservation in sorted(reservations.items()):
        if reservation.relay_gpu != relay_gpu:
            continue
        lease = lease_tokens.get(reservation_id)
        record = asdict(reservation)
        record["transfer_id"] = reservation_transfers.get(reservation_id)
        record["job_id"] = None if lease is None else lease.job_id
        records.append(record)
    return records


def relay_lease_records(
    *,
    relay_gpu: int,
    lease_tokens: Mapping[str, LeaseToken],
    reservations: Mapping[str, TransferReservation],
    reservation_transfers: Mapping[str, str],
) -> list[dict[str, object]]:
    records = []
    for lease_id, lease in sorted(lease_tokens.items()):
        if lease.relay_gpu != relay_gpu:
            continue
        if lease_id not in reservations:
            continue
        records.append(
            {
                "lease_id": lease.lease_id,
                "session_id": lease.session_id,
                "relay_gpu": lease.relay_gpu,
                "job_id": lease.job_id,
                "buffer_ids": lease.buffer_ids,
                "issued_at": lease.issued_at,
                "expires_at": lease.expires_at,
                "transfer_id": reservation_transfers.get(lease_id),
            }
        )
    return records


__all__ = [
    "active_buffer_lease_ids",
    "relay_lease_records",
    "relay_quota_record",
    "relay_reservation_records",
    "relay_session_records",
    "runtime_lease_record",
    "runtime_reservation_record",
]
