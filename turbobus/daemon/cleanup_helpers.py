from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict

from ..schema import PeerIdentity


def empty_removed_summary() -> dict[str, int]:
    return {
        "jobs": 0,
        "buffers": 0,
        "sessions": 0,
        "reservations": 0,
        "staging_records": 0,
        "transfers": 0,
    }


def merge_removed(
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


def session_cleanup_target_payload(
    cleanup_targets: Mapping[str, object] | None,
) -> dict[str, object]:
    if cleanup_targets is None:
        return {}
    job_ids = tuple(
        str(item["target_id"])
        for item in cleanup_targets.get("jobs", ()) or ()
        if isinstance(item, Mapping) and "target_id" in item
    )
    buffer_ids = tuple(
        str(item["target_id"])
        for item in cleanup_targets.get("buffers", ()) or ()
        if isinstance(item, Mapping) and "target_id" in item
    )
    if not job_ids and not buffer_ids:
        return {}
    return {
        "retired_cleanup_targets": {
            "job_ids": job_ids,
            "buffer_ids": buffer_ids,
        }
    }


def merge_retention_evidence(
    existing: object,
    incoming: Mapping[str, object] | None,
) -> dict[str, object] | None:
    existing_mapping = dict(existing) if isinstance(existing, Mapping) else {}
    incoming_mapping = dict(incoming) if isinstance(incoming, Mapping) else {}
    if not existing_mapping and not incoming_mapping:
        return None
    merged = dict(existing_mapping)
    for key, value in incoming_mapping.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), Mapping):
            nested = dict(merged[key])
            nested.update(dict(value))
            merged[key] = nested
        elif isinstance(value, Mapping):
            merged[key] = dict(value)
        else:
            merged[key] = value
    return merged


def jsonable_cleanup_target_record(record: Mapping[str, object]) -> dict[str, object]:
    result = dict(record)
    peer_identity = result.get("peer_identity")
    if isinstance(peer_identity, PeerIdentity):
        result["peer_identity"] = asdict(peer_identity)
    elif isinstance(peer_identity, Mapping):
        result["peer_identity"] = dict(peer_identity)
    buffer_snapshot = result.get("buffer_snapshot")
    if isinstance(buffer_snapshot, Mapping):
        result["buffer_snapshot"] = dict(buffer_snapshot)
    retention_evidence = result.get("retention_evidence")
    if isinstance(retention_evidence, Mapping):
        result["retention_evidence"] = dict(retention_evidence)
    transfer_ids = result.get("transfer_ids")
    if transfer_ids is not None:
        result["transfer_ids"] = tuple(str(item) for item in transfer_ids)
    return result


def buffer_snapshot_with_retention_evidence(
    snapshot: Mapping[str, object],
    *,
    retention_evidence: Mapping[str, object],
    archived_buffer_snapshot: object,
) -> dict[str, object]:
    updated = dict(snapshot)
    if "metadata" not in updated and isinstance(archived_buffer_snapshot, Mapping):
        archived_metadata = archived_buffer_snapshot.get("metadata")
        if isinstance(archived_metadata, Mapping):
            updated["metadata"] = dict(archived_metadata)
    merged_retention = merge_retention_evidence(
        updated.get("retention_evidence"),
        retention_evidence,
    )
    if merged_retention is not None:
        updated["retention_evidence"] = merged_retention
        local_cleanup = merged_retention.get("local_cpu_buffer_cleanup")
        if isinstance(local_cleanup, Mapping):
            updated["local_cpu_buffer_cleanup"] = dict(local_cleanup)
        owned_release = merged_retention.get("owned_cpu_buffer_release")
        if isinstance(owned_release, Mapping):
            updated["owned_cpu_buffer_release"] = dict(owned_release)
    return updated


__all__ = [
    "buffer_snapshot_with_retention_evidence",
    "empty_removed_summary",
    "jsonable_cleanup_target_record",
    "merge_removed",
    "merge_retention_evidence",
    "session_cleanup_target_payload",
]
