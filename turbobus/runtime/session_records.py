from __future__ import annotations

import logging
import time
from collections.abc import Mapping, Sequence

from ..schema import TransferIntent, TransferReceipt
from .lifecycle import copy_lifecycle_mapping, copy_lifecycle_sequence

logger = logging.getLogger(__name__)


def initialize_runtime_entrypoint_record(session) -> None:
    session._runtime_entrypoint_record = {
        "schema": "turbobus.runtime_session_entrypoint.v1",
        "entrypoint": "TurboBusRuntimeSession",
        "job_id": str(session.job_id),
        "user_id": None if session.user_id is None else str(session.user_id),
        "daemon_client": session.daemon_client.__class__.__name__,
        "runtime_daemon_client": session.runtime_daemon_client.__class__.__name__,
        "execution_daemon_client": session.execution_daemon_client.__class__.__name__,
        "profile_daemon_client": session.profile_daemon_client.__class__.__name__,
        "worker_client": (
            None if session.worker_client is None else session.worker_client.__class__.__name__
        ),
        "plan_source": "daemon_scheduler",
        "route_policy_visible_to_application": False,
        "route_policy_visible_to_adapter": False,
        "created_at": time.time(),
        "session": {},
        "buffers": {},
        "intents": {},
        "receipts": {},
        "adapter_contexts": {},
        "adapter_evidence": {},
        "close": {},
    }


def runtime_entrypoint_snapshot(session) -> dict[str, object]:
    record = getattr(session, "_runtime_entrypoint_record", None)
    if not isinstance(record, Mapping):
        return {}
    snapshot = copy_lifecycle_mapping(record)
    snapshot["managed_services"] = (
        None
        if session.managed_service_snapshot() is None
        else session.managed_service_snapshot()
    )
    snapshot["buffer_lifecycle"] = session.buffer_lifecycle_snapshot()
    snapshot["profile_bootstrap"] = session.profile_bootstrap_snapshot()
    snapshot["closed"] = bool(session.closed)
    return snapshot


def record_runtime_session_open(
    record: dict[str, object],
    *,
    session_id: str,
    target_gpu: int,
    relay_gpus: Sequence[int],
    worker_relay_capable: bool,
    profile_bootstrap: Mapping[str, object],
) -> None:
    record["session"] = {
        "session_id": str(session_id),
        "job_registered": True,
        "target_gpu_source": "registered_cuda_buffer",
        "target_gpu": int(target_gpu),
        "relay_source": "daemon_register_session",
        "relay_gpus": [int(gpu) for gpu in relay_gpus],
        "worker_relay_capable": bool(worker_relay_capable),
        "profile_bootstrap": dict(profile_bootstrap),
        "opened_at": time.time(),
    }


def record_runtime_buffer_registered(
    record: dict[str, object],
    *,
    buffer_id: str,
    registration: Mapping[str, object],
) -> None:
    buffers = _entry(record, "buffers")
    existing = buffers.get(str(buffer_id), {})
    if not isinstance(existing, Mapping):
        existing = {}
    entry = dict(existing)
    entry["buffer_id"] = str(buffer_id)
    entry["registered_by"] = "TurboBusRuntimeSession"
    entry["daemon_registered"] = True
    entry["registered_at"] = time.time()
    entry["registration"] = dict(registration)
    buffers[str(buffer_id)] = entry


def record_runtime_intent_submitted(
    record: dict[str, object],
    intent: TransferIntent,
) -> None:
    intents = _entry(record, "intents")
    intents[str(intent.intent_id)] = {
        "intent_id": str(intent.intent_id),
        "job_id": str(intent.job_id),
        "session_id": str(intent.session_id),
        "source_buffer_id": str(intent.source_buffer_id),
        "destination_buffer_id": str(intent.destination_buffer_id),
        "direction": str(intent.direction),
        "bytes_total": int(intent.total_bytes),
        "range_count": len(tuple(intent.ranges)),
        "submitted_by": "TurboBusRuntimeSession",
        "plan_source": "daemon_scheduler",
        "physical_route_control": False,
        "state": "submitted",
        "submitted_at": time.time(),
    }


def record_runtime_daemon_execution(
    record: dict[str, object],
    intent: TransferIntent,
    *,
    receipt: TransferReceipt,
) -> None:
    intents = _entry(record, "intents")
    entry = dict(intents.get(str(intent.intent_id), {}))
    entry.update(
        {
            "state": str(getattr(receipt.state, "value", receipt.state)),
            "receipt_id": str(receipt.receipt_id),
            "ticket_id": str(receipt.ticket_id),
            "decision_id": str(receipt.decision_id),
            "topology_snapshot_id": str(receipt.topology_snapshot_id),
            "bytes_completed": int(receipt.bytes_completed),
            "completed_at": receipt.completed_at,
            "executed_by": "daemon_issued_worker_or_backend",
        }
    )
    intents[str(intent.intent_id)] = entry


def record_runtime_receipt_finalized(
    record: dict[str, object],
    receipt: TransferReceipt,
) -> None:
    receipts = _entry(record, "receipts")
    receipts[str(receipt.intent_id)] = {
        "intent_id": str(receipt.intent_id),
        "receipt_id": str(receipt.receipt_id),
        "state": str(getattr(receipt.state, "value", receipt.state)),
        "ticket_id": str(receipt.ticket_id),
        "decision_id": str(receipt.decision_id),
        "topology_snapshot_id": str(receipt.topology_snapshot_id),
        "bytes_total": int(receipt.bytes_total),
        "bytes_completed": int(receipt.bytes_completed),
        "receipt_source": "TransferReceipt",
        "finalized_by": "TurboBusRuntimeSession",
        "finalized_at": time.time(),
    }


def record_runtime_adapter_evidence(
    record: dict[str, object],
    *,
    evidence_id: str,
    operation: str,
    intent_ids: Sequence[str],
    receipt_ids: Sequence[str],
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤1：记录适配器证据边界
    #  * ========================================================================
    #  * 目标对象：runtime entrypoint record
    #  * 操作：
    #  *   1) 记录 adapter evidence 与 RuntimeSession 的 intent/receipt 对齐关系
    #  *   2) 只保存可验证快照，不暴露物理路径选择
    #  */
    logger.info("开始记录适配器证据边界...")

    # // 1.1 获取 RuntimeSession adapter evidence 记录表
    adapter_evidence = _entry(record, "adapter_evidence")

    # // 1.2 归一化 intent 与 receipt 标识
    normalized_intent_ids = [str(intent_id) for intent_id in intent_ids]
    normalized_receipt_ids = [str(receipt_id) for receipt_id in receipt_ids]

    # // 1.3 写入可验证的 RuntimeSession 对齐记录
    adapter_evidence[str(evidence_id)] = {
        "evidence_id": str(evidence_id),
        "operation": str(operation),
        "intent_ids": normalized_intent_ids,
        "receipt_ids": normalized_receipt_ids,
        "intents_recorded": _contains_all(_entry(record, "intents"), normalized_intent_ids),
        "receipts_recorded": _receipt_entries_contain_all(
            _entry(record, "receipts"),
            normalized_receipt_ids,
        ),
        "recorded_at": time.time(),
    }
    logger.info("适配器证据边界记录完成, evidence_id: %s", evidence_id)


def record_runtime_adapter_context(
    record: dict[str, object],
    *,
    context_id: str,
    workload_kind: str,
    cpu_buffer_id: str,
    gpu_buffer_id: str,
    intent_prefix: str,
    priority: int,
    policy_hints: Mapping[str, object],
    metadata: Mapping[str, object],
    state: str = "created",
    error: str | None = None,
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤1：记录适配器构造边界
    #  * ========================================================================
    #  * 目标对象：RuntimeSession entrypoint record
    #  * 操作：
    #  *   1) 记录 AdapterTransferContext 来源和 buffer 绑定
    #  *   2) 记录 policy/metadata 已由 RuntimeSession 去物理路径
    #  */
    logger.info("开始记录适配器构造边界...")

    # // 1.1 获取 adapter context 记录表
    adapter_contexts = _entry(record, "adapter_contexts")

    # // 1.2 写入 RuntimeSession 构造记录
    adapter_contexts[str(context_id)] = {
        "context_id": str(context_id),
        "factory": "TurboBusRuntimeSession.make_adapter_transfer_context",
        "state": str(state),
        "workload_kind": str(workload_kind),
        "cpu_buffer_id": str(cpu_buffer_id),
        "gpu_buffer_id": str(gpu_buffer_id),
        "intent_prefix": str(intent_prefix),
        "priority": int(priority),
        "policy_hints": dict(policy_hints),
        "metadata": dict(metadata),
        "policy_source": "daemon_scheduler",
        "buffer_registration_source": "TurboBusRuntimeSession",
        "route_policy_visible_to_adapter": False,
        "physical_route_control": False,
        "recorded_at": time.time(),
    }
    if error is not None:
        adapter_contexts[str(context_id)]["error"] = str(error)
    logger.info("适配器构造边界记录完成, context_id: %s", context_id)


def record_runtime_buffer_cleanup(
    record: dict[str, object],
    *,
    buffer_id: str,
    cleanup_record: Mapping[str, object],
) -> None:
    buffers = _entry(record, "buffers")
    entry = dict(buffers.get(str(buffer_id), {}))
    history = list(copy_lifecycle_sequence(entry.get("cleanup_history")))
    history.append(dict(cleanup_record))
    entry["buffer_id"] = str(buffer_id)
    entry["cleanup_history"] = tuple(history)
    entry["last_cleanup"] = dict(cleanup_record)
    entry["state"] = "cleaned" if bool(cleanup_record.get("ok", False)) else "cleanup_failed"
    buffers[str(buffer_id)] = entry


def record_runtime_session_close(
    record: dict[str, object],
    *,
    response_ok: bool,
    response_error: str | None,
    payload: Mapping[str, object],
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤1：记录关闭结果
    #  * ========================================================================
    #  * 目标对象：RuntimeSession entrypoint record
    #  * 操作：
    #  *   1) 合并写入 close 总结果
    #  *   2) 保留提前写入的恢复、清理和托管服务关闭证据
    #  */
    logger.info("开始记录关闭结果...")

    # // 1.1 获取已有 close 记录，避免覆盖恢复证据
    close_record = _entry(record, "close")

    # // 1.2 合并关闭响应摘要
    close_record.update(
        {
            "entrypoint": "TurboBusRuntimeSession.close",
            "plan_source": "daemon_scheduler",
            "route_policy_visible_to_adapter": False,
            "route_policy_visible_to_application": False,
            "closed_at": time.time(),
            "ok": bool(response_ok),
            "error": response_error,
            "payload_keys": sorted(str(key) for key in payload),
        }
    )
    logger.info("关闭结果记录完成, ok: %s", bool(response_ok))


def record_runtime_close_recovery(
    record: dict[str, object],
    *,
    intent_wait_evidence: Sequence[Mapping[str, object]],
    intent_recovery_evidence: Sequence[Mapping[str, object]],
    cleanup_evidence: Sequence[Mapping[str, object]],
    cleanup_errors: Sequence[Mapping[str, object]],
    local_cpu_cleanup: Sequence[Mapping[str, object]],
    direct_runtime_cache_evidence: Mapping[str, object] | None,
    managed_runtime_before_shutdown: Mapping[str, object] | None,
    managed_runtime_after_shutdown: Mapping[str, object] | None,
    runtime_control_evidence: Mapping[str, object] | None,
    managed_service_evidence: Sequence[Mapping[str, object]],
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤1：记录关闭恢复边界
    #  * ========================================================================
    #  * 目标对象：RuntimeSession entrypoint record
    #  * 操作：
    #  *   1) 记录 close 时 active intent wait/recovery 结果
    #  *   2) 记录 close cleanup 结果，避免只存在 response payload 中
    #  */
    logger.info("开始记录关闭恢复边界...")

    # // 1.1 获取 close 记录表
    close_record = _entry(record, "close")

    # // 1.2 写入 active intent wait/recovery 和 cleanup 证据
    close_record["active_intent_receipts"] = [
        dict(item) for item in intent_wait_evidence
    ]
    close_record["active_intent_recovery"] = [
        dict(item) for item in intent_recovery_evidence
    ]
    close_record["buffer_cleanup_evidence"] = [dict(item) for item in cleanup_evidence]
    close_record["buffer_cleanup_errors"] = [dict(item) for item in cleanup_errors]
    close_record["local_cpu_buffer_cleanup"] = [
        dict(item) for item in local_cpu_cleanup
    ]
    close_record["direct_runtime_cache_shutdown"] = (
        None
        if direct_runtime_cache_evidence is None
        else dict(direct_runtime_cache_evidence)
    )
    close_record["managed_service_runtime_before_shutdown"] = (
        None
        if managed_runtime_before_shutdown is None
        else copy_lifecycle_mapping(managed_runtime_before_shutdown)
    )
    close_record["managed_service_runtime_after_shutdown"] = (
        None
        if managed_runtime_after_shutdown is None
        else copy_lifecycle_mapping(managed_runtime_after_shutdown)
    )
    close_record["runtime_control_shutdown"] = (
        None if runtime_control_evidence is None else dict(runtime_control_evidence)
    )
    close_record["managed_service_shutdown"] = [
        dict(item) for item in managed_service_evidence
    ]
    close_record["recovery_source"] = "TurboBusRuntimeSession.recover_transfer_state"
    close_record["receipt_source"] = "TransferReceipt"
    close_record["route_policy_visible_to_adapter"] = False
    close_record["recorded_at"] = time.time()
    logger.info("关闭恢复边界记录完成, recovered: %s", len(intent_recovery_evidence))


def record_runtime_transfer_recovery(
    record: dict[str, object],
    *,
    intent_id: str | None,
    transfer_id: str | None,
    recovery: Mapping[str, object],
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤1：记录主动恢复边界
    #  * ========================================================================
    #  * 目标对象：RuntimeSession entrypoint record
    #  * 操作：
    #  *   1) 记录 recover_transfer_state 返回的 daemon recovery
    #  *   2) 绑定恢复入口来源，避免 recovery 只存在调用返回值中
    #  */
    logger.info("开始记录主动恢复边界...")

    # // 1.1 获取 recovery 记录表
    recoveries = _entry(record, "recoveries")

    # // 1.2 写入 daemon recovery 摘要
    key = str(intent_id or transfer_id or f"recovery-{len(recoveries) + 1}")
    receipt = recovery.get("receipt")
    recoveries[key] = {
        "intent_id": None if intent_id is None else str(intent_id),
        "transfer_id": None if transfer_id is None else str(transfer_id),
        "source": "TurboBusRuntimeSession.recover_transfer_state",
        "daemon_source": recovery.get("source"),
        "state": recovery.get("state"),
        "archived": bool(recovery.get("archived", False)),
        "receipt_recorded": isinstance(receipt, Mapping),
        "recorded_at": time.time(),
    }
    logger.info("主动恢复边界记录完成, key: %s", key)


def _entry(record: dict[str, object], key: str) -> dict[str, object]:
    value = record.get(key)
    if isinstance(value, dict):
        return value
    replacement: dict[str, object] = {}
    record[key] = replacement
    return replacement


def _contains_all(value: Mapping[str, object], keys: Sequence[str]) -> bool:
    return all(str(key) in value for key in keys)


def _receipt_entries_contain_all(
    value: Mapping[str, object],
    receipt_ids: Sequence[str],
) -> bool:
    if not receipt_ids:
        return False
    observed = {
        str(entry.get("receipt_id"))
        for entry in value.values()
        if isinstance(entry, Mapping)
    }
    return all(str(receipt_id) in observed for receipt_id in receipt_ids)


__all__ = [
    "initialize_runtime_entrypoint_record",
    "record_runtime_adapter_context",
    "record_runtime_adapter_evidence",
    "record_runtime_buffer_cleanup",
    "record_runtime_buffer_registered",
    "record_runtime_close_recovery",
    "record_runtime_daemon_execution",
    "record_runtime_transfer_recovery",
    "record_runtime_intent_submitted",
    "record_runtime_receipt_finalized",
    "record_runtime_session_close",
    "record_runtime_session_open",
    "runtime_entrypoint_snapshot",
]
