from __future__ import annotations

from collections.abc import Mapping

from ..schema import TransferStatusState


def runtime_mapping_records(value: object) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list | tuple):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def runtime_mapping_records_from_sources(
    *values: object,
) -> tuple[Mapping[str, object], ...]:
    records: list[Mapping[str, object]] = []
    for value in values:
        records.extend(runtime_mapping_records(value))
    return tuple(records)


def job_runtime_state_from_records(
    job_runtime_state: Mapping[str, object],
    transfers: object,
) -> dict[str, dict[str, object]]:
    records = runtime_mapping_records(transfers)
    return _job_runtime_state_from_mapping_records(job_runtime_state, records)


def runtime_transfer_summary_from_records(
    job_runtime_state: Mapping[str, object],
    transfer_records: object,
    recent_terminal_feedback: object,
) -> dict[str, object]:
    records = runtime_mapping_records(transfer_records)
    recent_records = runtime_mapping_records(recent_terminal_feedback)
    filtered_jobs = _initial_job_runtime_state(job_runtime_state)
    delayed_transfers: list[dict[str, object]] = []
    admitted_transfers: list[dict[str, object]] = []
    queued_transfers: list[dict[str, object]] = []
    running_transfers: list[dict[str, object]] = []
    active_transfers: list[dict[str, object]] = []
    queued_by_direction: dict[str, dict[str, int]] = {}
    active_by_direction: dict[str, dict[str, int]] = {}
    terminal_transfer_count = 0

    for item in records:
        record = dict(item)
        state = str(record.get("state", ""))
        if state in _TERMINAL_TRANSFER_STATE_VALUES:
            terminal_transfer_count += 1
        if state == TransferStatusState.SUBMITTED.value:
            queued_transfers.append(record)
            _accumulate_direction_bytes(
                queued_by_direction,
                record,
                include_remaining=False,
            )
            if _record_has_admitted_execution(record):
                admitted_transfers.append(record)
            else:
                delayed_transfers.append(record)
        elif state == TransferStatusState.RUNNING.value:
            running_transfers.append(record)
        if _record_has_active_execution(record):
            active_transfers.append(record)
            _accumulate_direction_bytes(
                active_by_direction,
                record,
                include_remaining=True,
            )
        _accumulate_job_runtime_record(filtered_jobs, record)

    for record in recent_records:
        if str(record.get("state", "")) in _TERMINAL_TRANSFER_STATE_VALUES:
            terminal_transfer_count += 1

    return {
        "transfer_groups": {
            "delayed_transfers": delayed_transfers,
            "admitted_transfers": admitted_transfers,
            "queued_transfers": queued_transfers,
            "running_transfers": running_transfers,
            "active_transfers": active_transfers,
        },
        "queued_bytes_by_direction": queued_by_direction,
        "active_bytes_by_direction": active_by_direction,
        "job_runtime_state": dict(sorted(filtered_jobs.items())),
        "terminal_transfer_count": terminal_transfer_count,
    }


def _job_runtime_state_from_mapping_records(
    job_runtime_state: Mapping[str, object],
    records: tuple[Mapping[str, object], ...],
) -> dict[str, dict[str, object]]:
    filtered_jobs = _initial_job_runtime_state(job_runtime_state)
    for item in records:
        _accumulate_job_runtime_record(filtered_jobs, item)
    return dict(sorted(filtered_jobs.items()))


def _initial_job_runtime_state(
    job_runtime_state: Mapping[str, object],
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
            "queued_bytes_total": 0,
            "admitted_bytes_total": 0,
            "delayed_bytes_total": 0,
            "active_bytes_total": 0,
            "active_bytes_remaining": 0,
        }
        for job_id, record in job_runtime_state.items()
    }
    return filtered_jobs


def _accumulate_job_runtime_record(
    filtered_jobs: dict[str, dict[str, object]],
    record: Mapping[str, object],
) -> None:
    job_id = record.get("job_id")
    if job_id is None:
        return
    job_key = str(job_id)
    job_record = filtered_jobs.setdefault(
        job_key,
        {
            "job_id": job_key,
            "weight": 1.0,
            "queued_transfer_count": 0,
            "running_transfer_count": 0,
            "active_transfer_count": 0,
            "queued_bytes_total": 0,
            "admitted_bytes_total": 0,
            "delayed_bytes_total": 0,
            "active_bytes_total": 0,
            "active_bytes_remaining": 0,
        },
    )
    state = str(record.get("state", ""))
    bytes_total = int(record.get("bytes_total", 0) or 0)
    if state == TransferStatusState.SUBMITTED.value:
        job_record["queued_transfer_count"] += 1
        job_record["queued_bytes_total"] += bytes_total
        if _record_has_admitted_execution(record):
            job_record["admitted_bytes_total"] += bytes_total
        else:
            job_record["delayed_bytes_total"] += bytes_total
    elif state == TransferStatusState.RUNNING.value:
        job_record["running_transfer_count"] += 1
    if _record_has_active_execution(record):
        bytes_completed = int(record.get("bytes_completed", 0) or 0)
        job_record["active_transfer_count"] += 1
        job_record["active_bytes_total"] += bytes_total
        job_record["active_bytes_remaining"] += max(
            0,
            bytes_total - bytes_completed,
        )


def _accumulate_direction_bytes(
    by_direction: dict[str, dict[str, int]],
    record: Mapping[str, object],
    *,
    include_remaining: bool,
) -> None:
    direction = str(record.get("direction", "unknown"))
    bucket = by_direction.setdefault(
        direction,
        (
            {"transfer_count": 0, "bytes_total": 0, "bytes_remaining": 0}
            if include_remaining
            else {"transfer_count": 0, "bytes_total": 0}
        ),
    )
    bucket["transfer_count"] += 1
    bucket["bytes_total"] += int(record.get("bytes_total", 0) or 0)
    if include_remaining:
        bucket["bytes_remaining"] += max(
            0,
            int(record.get("bytes_total", 0) or 0)
            - int(record.get("bytes_completed", 0) or 0),
        )


def _record_has_admitted_execution(record: Mapping[str, object]) -> bool:
    return str(record.get("admission_state", "admitted")) == "admitted"


def _record_has_active_execution(record: Mapping[str, object]) -> bool:
    state = str(record.get("state", ""))
    if state == TransferStatusState.RUNNING.value:
        return True
    if state != TransferStatusState.SUBMITTED.value:
        return False
    return _record_has_admitted_execution(record)


_TERMINAL_TRANSFER_STATE_VALUES = {
    TransferStatusState.COMPLETE.value,
    TransferStatusState.FAILED.value,
    TransferStatusState.CANCELED.value,
}


__all__ = [
    "job_runtime_state_from_records",
    "runtime_mapping_records",
    "runtime_mapping_records_from_sources",
    "runtime_transfer_summary_from_records",
]
