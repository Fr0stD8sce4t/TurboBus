from __future__ import annotations

import logging
from typing import Any, Iterable, Mapping

from ..schema import TransferReceipt

logger = logging.getLogger(__name__)


def unique_receipts_from_handles(handles: Iterable[object]) -> list[TransferReceipt]:
    receipts: list[TransferReceipt] = []
    seen = set()
    for handle in handles:
        receipt = getattr(handle, "receipt", None)
        if not isinstance(receipt, TransferReceipt):
            continue
        if receipt.receipt_id in seen:
            continue
        seen.add(receipt.receipt_id)
        receipts.append(receipt)
    return receipts


def receipt_trace_from_receipts(receipts: Iterable[TransferReceipt]) -> dict[str, Any]:
    direct_bytes = 0
    relay_bytes = 0
    receipt_ids: list[str] = []
    intent_ids: list[str] = []
    decision_ids: list[str] = []
    topology_snapshot_ids: list[str] = []
    ticket_ids: list[str] = []
    receipt_states: list[str] = []
    completion_sources: list[str] = []
    transfer_ids: list[str] = []
    fallback_reasons: list[str] = []
    receipt_list = list(receipts)
    receipt_contracts: list[dict[str, Any]] = []
    runtime_buffer_bindings: list[dict[str, Any]] = []
    for receipt in receipt_list:
        receipt_ids.append(receipt.receipt_id)
        intent_ids.append(receipt.intent_id)
        decision_ids.append(receipt.decision_id)
        topology_snapshot_ids.append(receipt.topology_snapshot_id)
        ticket_ids.append(receipt.ticket_id)
        receipt_states.append(str(receipt.state.value))
        metadata = receipt.metadata if isinstance(receipt.metadata, Mapping) else {}
        receipt_contracts.append(_receipt_contract_summary(receipt, metadata))
        runtime_buffer_bindings.extend(_runtime_buffer_bindings(receipt, metadata))
        completion_source = metadata.get("completion_source")
        if completion_source:
            completion_sources.append(str(completion_source))
        transfer_id = metadata.get("transfer_id")
        if transfer_id:
            transfer_ids.append(str(transfer_id))
        fallback_reason = metadata.get("fallback_reason")
        if fallback_reason:
            fallback_reasons.append(str(fallback_reason))
        for path in receipt.path_stats:
            path_bytes = int(path.get("bytes", 0) or 0)
            if str(path.get("kind", "")).lower() == "relay":
                relay_bytes += path_bytes
            else:
                direct_bytes += path_bytes
    return {
        "direct_bytes": direct_bytes,
        "relay_bytes": relay_bytes,
        "receipt_count": len(receipt_list),
        "intent_ids": join_unique(intent_ids),
        "receipt_ids": join_unique(receipt_ids),
        "decision_ids": join_unique(decision_ids),
        "topology_snapshot_ids": join_unique(topology_snapshot_ids),
        "ticket_ids": join_unique(ticket_ids),
        "receipt_states": join_unique(receipt_states),
        "completion_sources": join_unique(completion_sources),
        "transfer_ids": join_unique(transfer_ids),
        "fallback_reason": join_unique(fallback_reasons),
        "receipt_contracts": receipt_contracts,
        "runtime_buffer_bindings": runtime_buffer_bindings,
    }


def adapter_lifecycle_evidence_from_handles(
    *,
    evidence_id: str,
    operation: str,
    transfer_context,
    item_field: str,
    item_count_field: str,
    item_names: Iterable[str],
    handles: Iterable[object],
    transfer_stats: Mapping[str, Any],
    runtime_session,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 步骤1：生成 RuntimeSession 绑定证据
    #  * ========================================================================
    #  * 数据源：adapter transfer handles 与 TurboBusRuntimeSession snapshot
    #  * 操作：
    #  *   1) 从 handle 提取真实 TransferReceipt
    #  *   2) 生成 RuntimeSession entrypoint 合约
    #  *   3) 拒绝 adapter 覆盖核心生产边界字段
    #  */
    logger.info("开始生成 RuntimeSession 绑定证据...")

    # // 1.1 归一化 adapter item 与 handle
    names = tuple(str(name) for name in item_names)
    handle_list = list(handles)

    # // 1.2 提取唯一 TransferReceipt 证据
    receipts = unique_receipts_from_handles(handle_list)
    if names and not receipts:
        raise RuntimeError(
            f"{operation} completed without TransferReceipt evidence"
        )

    # // 1.3 校验 RuntimeSession 入口并生成 receipt trace
    _require_runtime_session_contract(runtime_session)
    trace = receipt_trace_from_receipts(receipts)
    recovery = _daemon_recovery_from_receipts(receipts, runtime_session)
    trace["daemon_recovery"] = recovery
    trace["daemon_recovery_count"] = len(recovery)
    trace["daemon_recovery_sources"] = join_unique(
        item.get("source") for item in recovery
    )
    runtime_entrypoint = runtime_entrypoint_contract(
        runtime_session,
        receipts=receipts,
        evidence_id=str(evidence_id),
        operation=str(operation),
    )
    extra_payload = _adapter_extra_without_contract_overrides(extra)
    evidence = {
        "evidence_id": str(evidence_id),
        "operation": str(operation),
        "job_id": transfer_context.job_id,
        "session_id": transfer_context.session_id,
        "workload_kind": str(transfer_context.workload_kind.value),
        "buffer_registration_source": "TurboBusRuntimeSession",
        "intent_source": "TransferIntent",
        "receipt_source": "TransferReceipt",
        "policy_source": "daemon_scheduler",
        "route_policy_visible_to_adapter": False,
        "physical_route_source": "daemon_scheduler",
        "daemon_recovery_source": "TurboBusRuntimeSession",
        "cpu_buffer_id": transfer_context.cpu_buffer_id,
        "gpu_buffer_id": transfer_context.gpu_buffer_id,
        item_field: names,
        item_count_field: len(names),
        "runtime_entrypoint": runtime_entrypoint,
        **trace,
        **dict(transfer_stats),
        **extra_payload,
    }
    logger.info(
        "RuntimeSession 绑定证据生成完成, evidence_id: %s, receipts: %s",
        evidence_id,
        len(receipts),
    )
    return evidence


def _adapter_extra_without_contract_overrides(
    extra: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if extra is None:
        return {}
    payload = dict(extra)
    protected = {
        "buffer_registration_source",
        "intent_source",
        "receipt_source",
        "policy_source",
        "physical_route_source",
        "route_policy_visible_to_adapter",
        "daemon_recovery_source",
        "runtime_entrypoint",
        "receipt_contracts",
    }
    overridden = sorted(key for key in payload if key in protected)
    if overridden:
        raise ValueError(
            "adapter lifecycle extra must not override production boundary fields: "
            + ", ".join(overridden)
        )
    return payload


def join_unique(values: Iterable[object]) -> str:
    seen = set()
    ordered = []
    for value in values:
        if value is None:
            continue
        text = str(value)
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ",".join(ordered)


def _daemon_recovery_from_receipts(
    receipts: Iterable[TransferReceipt],
    runtime_session,
) -> list[dict[str, Any]]:
    recover = getattr(runtime_session, "recover_transfer_state", None)
    if not callable(recover):
        return []
    recovered: list[dict[str, Any]] = []
    for receipt in receipts:
        metadata = receipt.metadata if isinstance(receipt.metadata, Mapping) else {}
        transfer_id = metadata.get("transfer_id")
        if transfer_id is None:
            continue
        recovery = recover(
            intent_id=receipt.intent_id,
            transfer_id=str(transfer_id),
        )
        recovered.append(
            {
                "intent_id": receipt.intent_id,
                "receipt_id": receipt.receipt_id,
                "transfer_id": str(transfer_id),
                "source": recovery.get("source"),
                "state": recovery.get("state"),
                "admission": recovery.get("admission"),
                "queue_record": recovery.get("queue_record"),
                "ticket": recovery.get("ticket"),
                "reservations": recovery.get("reservations"),
                "leases": recovery.get("leases"),
                "buffer_snapshots": recovery.get("buffer_snapshots"),
                "cleanup_targets": recovery.get("cleanup_targets"),
                "completion_source": recovery.get("completion_source"),
                "completion_evidence": recovery.get("completion_evidence"),
            }
        )
    return recovered


def runtime_entrypoint_contract(
    runtime_session,
    *,
    receipts: Iterable[TransferReceipt],
    evidence_id: str | None = None,
    operation: str | None = None,
) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 步骤2：构造 RuntimeSession 入口合约
    #  * ========================================================================
    #  * 数据源：TurboBusRuntimeSession.runtime_entrypoint_snapshot
    #  * 操作：
    #  *   1) 读取 RuntimeSession snapshot
    #  *   2) 核对 adapter receipt 与 intent 已进入 entrypoint record
    #  *   3) 回写 adapter evidence 绑定快照
    #  */
    logger.info("开始构造 RuntimeSession 入口合约...")

    # // 2.1 确认 runtime_session 暴露生产入口快照
    _require_runtime_session_contract(runtime_session)
    snapshotter = getattr(runtime_session, "runtime_entrypoint_snapshot", None)

    # // 2.2 读取当前 RuntimeSession snapshot
    snapshot = snapshotter()
    if not isinstance(snapshot, Mapping):
        raise TypeError("runtime entrypoint snapshot must be a mapping")

    # // 2.3 提取 receipt 与 intent 绑定
    receipt_list = list(receipts)
    receipt_ids = [receipt.receipt_id for receipt in receipt_list]
    intent_ids = [receipt.intent_id for receipt in receipt_list]
    receipts_view = snapshot.get("receipts")
    intents_view = snapshot.get("intents")
    adapter_contexts_view = snapshot.get("adapter_contexts")
    contract = {
        "schema": snapshot.get("schema"),
        "entrypoint": snapshot.get("entrypoint"),
        "job_id": snapshot.get("job_id"),
        "session_id": snapshot.get("session", {}).get("session_id")
        if isinstance(snapshot.get("session"), Mapping)
        else None,
        "plan_source": snapshot.get("plan_source"),
        "route_policy_visible_to_application": bool(
            snapshot.get("route_policy_visible_to_application", True)
        ),
        "route_policy_visible_to_adapter": bool(
            snapshot.get("route_policy_visible_to_adapter", True)
        ),
        "receipt_ids": receipt_ids,
        "intent_ids": intent_ids,
        "receipts_recorded": _snapshot_receipts_contain_all(receipts_view, receipt_ids),
        "intents_recorded": _snapshot_contains_all(intents_view, intent_ids),
        "adapter_context_recorded": _snapshot_adapter_context_for_receipts(
            adapter_contexts_view,
            receipt_list,
        ),
    }
    if evidence_id is not None:
        _record_runtime_adapter_evidence(
            runtime_session,
            evidence_id=str(evidence_id),
            operation=str(operation or ""),
            intent_ids=intent_ids,
            receipt_ids=receipt_ids,
        )
        refreshed = snapshotter()
        if isinstance(refreshed, Mapping):
            adapter_evidence = refreshed.get("adapter_evidence")
            contract["adapter_evidence_recorded"] = _snapshot_contains_all(
                adapter_evidence,
                [str(evidence_id)],
            )
    logger.info(
        "RuntimeSession 入口合约构造完成, receipts: %s",
        len(receipt_ids),
    )
    return contract


def _require_runtime_session_contract(runtime_session) -> None:
    snapshotter = getattr(runtime_session, "runtime_entrypoint_snapshot", None)
    if not callable(snapshotter):
        raise TypeError(
            "adapter lifecycle evidence requires TurboBusRuntimeSession "
            "entrypoint snapshots"
        )


def _record_runtime_adapter_evidence(
    runtime_session,
    *,
    evidence_id: str,
    operation: str,
    intent_ids: Iterable[str],
    receipt_ids: Iterable[str],
) -> None:
    recorder = getattr(runtime_session, "record_adapter_lifecycle_evidence", None)
    if not callable(recorder):
        raise TypeError(
            "adapter lifecycle evidence requires TurboBusRuntimeSession "
            "adapter evidence recording"
        )
    recorder(
        evidence_id=evidence_id,
        operation=operation,
        intent_ids=tuple(str(intent_id) for intent_id in intent_ids),
        receipt_ids=tuple(str(receipt_id) for receipt_id in receipt_ids),
    )


def _snapshot_contains_all(value: object, keys: Iterable[str]) -> bool:
    if not isinstance(value, Mapping):
        return False
    return all(str(key) in value for key in keys)


def _snapshot_receipts_contain_all(value: object, receipt_ids: Iterable[str]) -> bool:
    if not isinstance(value, Mapping):
        return False
    observed = {
        str(item.get("receipt_id"))
        for item in value.values()
        if isinstance(item, Mapping)
    }
    return all(str(receipt_id) in observed for receipt_id in receipt_ids)


def _snapshot_adapter_context_for_receipts(
    value: object,
    receipts: Iterable[TransferReceipt],
) -> bool:
    if not isinstance(value, Mapping):
        return False
    contexts = [item for item in value.values() if isinstance(item, Mapping)]
    if not contexts:
        return False
    receipt_list = list(receipts)
    if not receipt_list:
        return False
    for receipt in receipt_list:
        metadata = receipt.metadata if isinstance(receipt.metadata, Mapping) else {}
        lifetime = metadata.get("buffer_lifetime_evidence")
        if not isinstance(lifetime, Mapping):
            return False
        source_id = _runtime_buffer_id_from_lifetime(lifetime.get("source_buffer"))
        destination_id = _runtime_buffer_id_from_lifetime(
            lifetime.get("destination_buffer")
        )
        if not _contexts_include_buffer_pair(contexts, source_id, destination_id):
            return False
    return True


def _runtime_buffer_id_from_lifetime(value: object) -> str | None:
    if not isinstance(value, Mapping):
        return None
    buffer_id = value.get("buffer_id")
    return None if buffer_id is None else str(buffer_id)


def _contexts_include_buffer_pair(
    contexts: Iterable[Mapping[str, object]],
    source_id: str | None,
    destination_id: str | None,
) -> bool:
    if source_id is None or destination_id is None:
        return False
    for context in contexts:
        cpu_buffer_id = str(context.get("cpu_buffer_id"))
        gpu_buffer_id = str(context.get("gpu_buffer_id"))
        pair = {cpu_buffer_id, gpu_buffer_id}
        if source_id in pair and destination_id in pair:
            return True
    return False


def _receipt_contract_summary(
    receipt: TransferReceipt,
    metadata: Mapping[str, Any],
) -> dict[str, Any]:
    completion_contract = metadata.get("completion_contract")
    cuda_ipc_lifecycle = metadata.get("cuda_ipc_lifecycle")
    return {
        "receipt_id": receipt.receipt_id,
        "intent_id": receipt.intent_id,
        "decision_id": receipt.decision_id,
        "topology_snapshot_id": receipt.topology_snapshot_id,
        "ticket_id": receipt.ticket_id,
        "job_id": receipt.job_id,
        "session_id": receipt.session_id,
        "state": str(receipt.state.value),
        "bytes_total": int(receipt.bytes_total),
        "bytes_completed": int(receipt.bytes_completed),
        "completion_source": metadata.get("completion_source"),
        "transfer_id": metadata.get("transfer_id"),
        "verified": bool(metadata.get("verified", False)),
        "verified_bytes": int(metadata.get("verified_bytes", 0) or 0),
        "completion_contract": (
            dict(completion_contract)
            if isinstance(completion_contract, Mapping)
            else None
        ),
        "cuda_ipc_lifecycle": (
            dict(cuda_ipc_lifecycle)
            if isinstance(cuda_ipc_lifecycle, Mapping)
            else None
        ),
    }


def _runtime_buffer_bindings(
    receipt: TransferReceipt,
    metadata: Mapping[str, Any],
) -> list[dict[str, Any]]:
    lifetime = metadata.get("buffer_lifetime_evidence")
    if not isinstance(lifetime, Mapping):
        return []
    bindings: list[dict[str, Any]] = []
    for role, key in (
        ("source", "source_buffer"),
        ("destination", "destination_buffer"),
    ):
        record = lifetime.get(key)
        if not isinstance(record, Mapping):
            continue
        registration = record.get("registration")
        registration_mapping = (
            dict(registration) if isinstance(registration, Mapping) else {}
        )
        registration_metadata = registration_mapping.get("metadata")
        bindings.append(
            {
                "receipt_id": receipt.receipt_id,
                "intent_id": receipt.intent_id,
                "role": role,
                "buffer_id": record.get("buffer_id"),
                "handle_type": registration_mapping.get("handle_type"),
                "runtime_buffer_kind": record.get("runtime_buffer_kind"),
                "runtime_session_id": record.get("runtime_session_id"),
                "runtime_owned": bool(record.get("runtime_owned", False)),
                "registration_metadata": (
                    dict(registration_metadata)
                    if isinstance(registration_metadata, Mapping)
                    else None
                ),
                "resource_evidence": (
                    dict(record["resource_evidence"])
                    if isinstance(record.get("resource_evidence"), Mapping)
                    else None
                ),
                "cuda_ipc_lifecycle": (
                    dict(record["cuda_ipc_lifecycle"])
                    if isinstance(record.get("cuda_ipc_lifecycle"), Mapping)
                    else None
                ),
            }
        )
    return bindings


__all__ = [
    "adapter_lifecycle_evidence_from_handles",
    "join_unique",
    "receipt_trace_from_receipts",
    "runtime_entrypoint_contract",
    "unique_receipts_from_handles",
]
