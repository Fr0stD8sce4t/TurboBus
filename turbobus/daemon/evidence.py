from __future__ import annotations

from collections.abc import Iterable, Mapping

from ..schema import ExecutionTicket


def normalize_completion_evidence(
    evidence: Mapping[str, object] | None,
    *,
    expected_bytes: int,
    completion_source: str,
    expected_ticket: ExecutionTicket | None = None,
) -> dict[str, object]:
    if not isinstance(evidence, Mapping):
        raise ValueError("complete intent transfer requires verified byte evidence")
    base = _normalize_verified_completion_base(
        evidence,
        expected_bytes=expected_bytes,
        completion_source=completion_source,
    )
    ticket_binding = _normalize_completion_ticket_binding(
        evidence,
        expected_ticket=expected_ticket,
    )
    path_evidence = _normalize_completion_path_evidence(
        evidence,
        expected_bytes=int(expected_bytes),
    )
    optional_evidence = _normalize_completion_optional_evidence(evidence)
    return {
        **base,
        **optional_evidence,
        **({} if not path_evidence else {"execution_path_evidence": path_evidence}),
        **ticket_binding,
    }


def _normalize_verified_completion_base(
    evidence: Mapping[str, object],
    *,
    expected_bytes: int,
    completion_source: str,
) -> dict[str, object]:
    expected = int(expected_bytes)
    try:
        verified_bytes = int(evidence["verified_bytes"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("complete intent transfer requires verified byte evidence") from exc
    if verified_bytes != expected:
        raise ValueError(
            f"verified byte evidence mismatch: {verified_bytes} != {expected}"
        )
    if not bool(evidence.get("content_match", False)):
        raise ValueError("complete intent transfer requires matching buffer evidence")
    source_digest = evidence.get("source_digest")
    destination_digest = evidence.get("destination_digest")
    if (
        source_digest is not None
        and destination_digest is not None
        and str(source_digest) != str(destination_digest)
    ):
        raise ValueError("verified byte evidence digest mismatch")
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
        **({} if source_digest is None else {"source_digest": str(source_digest)}),
        **(
            {}
            if destination_digest is None
            else {"destination_digest": str(destination_digest)}
        ),
    }


def _normalize_completion_path_evidence(
    evidence: Mapping[str, object],
    *,
    expected_bytes: int,
) -> dict[str, object]:
    path_evidence_source = evidence.get("execution_path_evidence")
    if not isinstance(path_evidence_source, Mapping):
        return normalize_execution_path_evidence(
            evidence,
            expected_bytes=int(expected_bytes),
        )
    explicit_path_evidence = dict(path_evidence_source)
    for field_name in (
        "executor",
        "path",
        "plan_source",
        "target_device",
        "relay_gpu",
        "relay_gpus",
        "src_buffer_id",
        "dst_buffer_id",
        "staging_slot_id",
        "direct_bytes",
        "direct_chunks",
        "relay_bytes",
        "relay_chunks",
        "path_level_evidence",
        "native_path_stats",
        "relay_device_stats",
    ):
        if field_name in evidence and field_name not in explicit_path_evidence:
            explicit_path_evidence[field_name] = evidence[field_name]
    return normalize_execution_path_evidence(
        explicit_path_evidence,
        expected_bytes=int(expected_bytes),
    )


def _normalize_completion_optional_evidence(
    evidence: Mapping[str, object],
) -> dict[str, object]:
    resource_evidence = evidence.get("resource_evidence")
    return {
        **_normalize_completion_mapping_attachments(evidence),
        **_normalize_completion_sequence_attachments(evidence),
        **_normalize_completion_cuda_ipc_lifecycle(
            evidence,
            resource_evidence=resource_evidence,
        ),
    }


def _normalize_completion_mapping_attachments(
    evidence: Mapping[str, object],
) -> dict[str, object]:
    normalized: dict[str, object] = {}
    for field_name in (
        "resource_evidence",
        "direct_completion_evidence",
        "relay_completion_evidence",
        "worker_completion_evidence",
        "cleanup",
        "worker_startup",
        "worker_async_pool",
        "worker_runtime_feedback",
        "async_data_plane",
        "path_level_evidence",
        "failure_cleanup_contract",
        "block_runtime",
    ):
        value = evidence.get(field_name)
        if isinstance(value, Mapping):
            normalized[field_name] = dict(value)
    return normalized


def _normalize_completion_sequence_attachments(
    evidence: Mapping[str, object],
) -> dict[str, object]:
    normalized: dict[str, object] = {}
    for field_name in ("native_path_stats", "relay_device_stats"):
        value = evidence.get(field_name)
        if isinstance(value, (list, tuple)):
            normalized[field_name] = tuple(
                dict(item) for item in value if isinstance(item, Mapping)
            )
    return normalized


def _normalize_completion_cuda_ipc_lifecycle(
    evidence: Mapping[str, object],
    *,
    resource_evidence: object,
) -> dict[str, object]:
    cuda_ipc_lifecycle = evidence.get("cuda_ipc_lifecycle")
    if not isinstance(cuda_ipc_lifecycle, Mapping) and isinstance(
        resource_evidence,
        Mapping,
    ):
        nested_cuda_ipc_lifecycle = resource_evidence.get("cuda_ipc_lifecycle")
        if isinstance(nested_cuda_ipc_lifecycle, Mapping):
            cuda_ipc_lifecycle = nested_cuda_ipc_lifecycle
    if not isinstance(cuda_ipc_lifecycle, Mapping):
        return {}
    return {"cuda_ipc_lifecycle": dict(cuda_ipc_lifecycle)}


def merge_completion_evidence(
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
        "worker_completion_evidence",
        "cleanup",
        "worker_async_pool",
        "worker_runtime_feedback",
        "async_data_plane",
        "path_level_evidence",
        "failure_cleanup_contract",
        "cuda_ipc_lifecycle",
        "block_runtime",
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


def normalize_execution_path_evidence(
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
    path_level_evidence = evidence.get("path_level_evidence")
    if isinstance(path_level_evidence, Mapping):
        result["path_level_evidence"] = dict(path_level_evidence)
    native_path_stats = evidence.get("native_path_stats")
    if isinstance(native_path_stats, (list, tuple)):
        result["native_path_stats"] = tuple(
            dict(item) for item in native_path_stats if isinstance(item, Mapping)
        )
    relay_device_stats = evidence.get("relay_device_stats")
    if isinstance(relay_device_stats, (list, tuple)):
        result["relay_device_stats"] = tuple(
            dict(item) for item in relay_device_stats if isinstance(item, Mapping)
        )
    direct_bytes = result.get("direct_bytes")
    relay_bytes = result.get("relay_bytes")
    if require_total_match and direct_bytes is not None and relay_bytes is not None:
        path_bytes = int(direct_bytes) + int(relay_bytes)
        if path_bytes != int(expected_bytes):
            raise ValueError(
                f"execution path byte evidence mismatch: {path_bytes} != {int(expected_bytes)}"
            )
    return result


def normalize_status_ticket_evidence(
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
    path_evidence = normalize_execution_path_evidence(
        evidence,
        expected_bytes=int(evidence.get("expected_bytes", 0) or 0),
        require_total_match=False,
    )
    if path_evidence:
        ticket_binding["execution_path_evidence"] = path_evidence
    cleanup = evidence.get("cleanup")
    if isinstance(cleanup, Mapping):
        ticket_binding["cleanup"] = dict(cleanup)
    direct_completion_evidence = evidence.get("direct_completion_evidence")
    if isinstance(direct_completion_evidence, Mapping):
        ticket_binding["direct_completion_evidence"] = dict(direct_completion_evidence)
    relay_completion_evidence = evidence.get("relay_completion_evidence")
    if isinstance(relay_completion_evidence, Mapping):
        ticket_binding["relay_completion_evidence"] = dict(relay_completion_evidence)
    worker_completion_evidence = evidence.get("worker_completion_evidence")
    if isinstance(worker_completion_evidence, Mapping):
        ticket_binding["worker_completion_evidence"] = dict(worker_completion_evidence)
    worker_startup = evidence.get("worker_startup")
    if isinstance(worker_startup, Mapping):
        ticket_binding["worker_startup"] = dict(worker_startup)
    worker_async_pool = evidence.get("worker_async_pool")
    if isinstance(worker_async_pool, Mapping):
        ticket_binding["worker_async_pool"] = dict(worker_async_pool)
    worker_runtime_feedback = evidence.get("worker_runtime_feedback")
    if isinstance(worker_runtime_feedback, Mapping):
        ticket_binding["worker_runtime_feedback"] = dict(worker_runtime_feedback)
    async_data_plane = evidence.get("async_data_plane")
    if isinstance(async_data_plane, Mapping):
        ticket_binding["async_data_plane"] = dict(async_data_plane)
    failure_cleanup_contract = evidence.get("failure_cleanup_contract")
    if isinstance(failure_cleanup_contract, Mapping):
        ticket_binding["failure_cleanup_contract"] = dict(failure_cleanup_contract)
    block_runtime = evidence.get("block_runtime")
    if isinstance(block_runtime, Mapping):
        ticket_binding["block_runtime"] = dict(block_runtime)
    cuda_ipc_lifecycle = evidence.get("cuda_ipc_lifecycle")
    if not isinstance(cuda_ipc_lifecycle, Mapping) and isinstance(
        resource_evidence,
        Mapping,
    ):
        nested_cuda_ipc_lifecycle = resource_evidence.get("cuda_ipc_lifecycle")
        if isinstance(nested_cuda_ipc_lifecycle, Mapping):
            cuda_ipc_lifecycle = nested_cuda_ipc_lifecycle
    if isinstance(cuda_ipc_lifecycle, Mapping):
        ticket_binding["cuda_ipc_lifecycle"] = dict(cuda_ipc_lifecycle)
    for field_name in (
        "executor",
        "path",
        "plan_source",
        "verification_source",
        "verification_method",
        "source_digest",
        "destination_digest",
        "failure_source",
    ):
        value = evidence.get(field_name)
        if value is not None:
            ticket_binding[field_name] = str(value)
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
    ticket_binding = {
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
    owner_binding = _normalize_evidence_owner_binding(
        evidence,
        expected_ticket=expected_ticket,
        evidence_name=evidence_name,
    )
    if owner_binding is not None:
        ticket_binding["owner_binding"] = owner_binding
    return ticket_binding


def _normalize_evidence_owner_binding(
    evidence: Mapping[str, object],
    *,
    expected_ticket: ExecutionTicket,
    evidence_name: str,
) -> dict[str, object] | None:
    expected_owner = expected_ticket.metadata.get("owner_binding")
    evidence_owner = evidence.get("owner_binding")
    if not isinstance(expected_owner, Mapping):
        return None
    if not isinstance(evidence_owner, Mapping):
        raise ValueError(f"{evidence_name} owner_binding is required")
    normalized_expected = _canonical_owner_binding(expected_owner)
    normalized_evidence = _canonical_owner_binding(evidence_owner)
    if normalized_expected != normalized_evidence:
        raise ValueError(f"{evidence_name} owner_binding does not match daemon ticket")
    return normalized_expected


def _canonical_owner_binding(owner_binding: Mapping[str, object]) -> dict[str, object]:
    cleanup_scope = owner_binding.get("cleanup_scope")
    if not isinstance(cleanup_scope, Mapping):
        raise ValueError("owner_binding cleanup_scope is required")
    peer_identity = owner_binding.get("peer_identity")
    canonical = {
        "job_id": str(owner_binding["job_id"]),
        "session_id": str(owner_binding["session_id"]),
        "transfer_id": str(owner_binding["transfer_id"]),
        "lease_ids": tuple(str(item) for item in owner_binding.get("lease_ids", ()) or ()),
        "relay_gpus": tuple(sorted({int(item) for item in owner_binding.get("relay_gpus", ()) or ()})),
        "cleanup_scope": {
            "target_kind": str(cleanup_scope.get("target_kind", "")).lower(),
            "target_ids": tuple(str(item) for item in cleanup_scope.get("target_ids", ()) or ()),
        },
    }
    if isinstance(peer_identity, Mapping):
        canonical["peer_identity"] = dict(peer_identity)
    return canonical


__all__ = [
    "_canonical_owner_binding",
    "merge_completion_evidence",
    "normalize_completion_evidence",
    "normalize_execution_path_evidence",
    "normalize_status_ticket_evidence",
]
