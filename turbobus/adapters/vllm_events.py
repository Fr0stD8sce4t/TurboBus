from __future__ import annotations

import logging
from typing import Any, Mapping


logger = logging.getLogger(__name__)

_CONNECTOR_EVENTS: list[dict[str, Any]] = []

_RUNTIME_EVIDENCE_KEYS = frozenset(
    {
        "lifecycle_evidence_id",
        "store_mutation_id",
        "cleanup_mutation_id",
        "receipt_ids",
        "receipt_count",
        "receipt_states",
        "direct_bytes",
        "relay_bytes",
        "decision_ids",
        "topology_snapshot_ids",
        "ticket_ids",
        "transfer_ids",
        "completion_sources",
        "fallback_reason",
        "prefix_cleanup_mutation_ids",
        "free_backing_cleanup_groups",
    }
)

_PRIVATE_EVENT_KEYS = frozenset(
    {
        "transfer_evidence_record",
        "transfer_evidence_records",
        "runtime_close_entrypoint",
        "receipt_ids",
        "decision_ids",
        "topology_snapshot_ids",
        "ticket_ids",
        "transfer_ids",
        "fallback_reason",
        "receipt_contracts",
        "runtime_entrypoint",
    }
)


def clear_connector_events() -> None:
    # /*
    #  * ========================================================================
    #  * 步骤1：拒绝公开清理 connector events
    #  * ========================================================================
    #  * 目标：防止外部代码删除 RuntimeSession-bound transfer event evidence
    #  * 数据源：公共 connector API
    #  * 操作：
    #  *   1) 拒绝公开清理
    #  *   2) 指向 TurboBusRuntimeSession connector lifecycle
    #  */
    logger.info("开始拒绝公开清理 connector events...")

    # // 1.1 拒绝公开事件清理
    raise RuntimeError(
        "connector events must be cleared through TurboBusRuntimeSession "
        "connector lifecycle"
    )


def _clear_connector_events_for_connector() -> None:
    # /*
    #  * ========================================================================
    #  * 步骤2：清理 connector 内部事件
    #  * ========================================================================
    #  * 目标：只允许 connector 生命周期内部重置事件缓存
    #  * 数据源：_CONNECTOR_EVENTS
    #  * 操作：
    #  *   1) 清空内部事件缓存
    #  *   2) 不作为公共 evidence 删除入口
    #  */
    logger.info("开始清理 connector 内部事件...")

    # // 2.1 清空内部事件缓存
    _CONNECTOR_EVENTS.clear()
    logger.info("connector 内部事件清理完成")


def get_connector_events() -> list[dict[str, Any]]:
    # /*
    #  * ========================================================================
    #  * 步骤1：读取 connector 事件快照
    #  * ========================================================================
    #  * 目标：只公开 RuntimeSession 证据绑定后的事件记录
    #  * 数据源：内存事件列表 _CONNECTOR_EVENTS
    #  * 操作：
    #  *   1) 校验 receipt/lifecycle/cleanup 摘要事件绑定 transfer evidence
    #  *   2) 返回复制后的事件快照，避免外部修改内部状态
    #  */
    logger.info("开始读取 connector 事件快照...")

    # // 1.1 校验所有 runtime-looking 事件
    events = [
        _public_connector_event(_validated_connector_event(event))
        for event in _CONNECTOR_EVENTS
    ]

    # // 1.2 返回隔离副本
    logger.info("connector 事件快照读取完成, count: %s", len(events))
    return events


def emit_event(event: str, **fields) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤2：记录 connector 事件
    #  * ========================================================================
    #  * 目标：把公开事件绑定到 RuntimeSession transfer evidence
    #  * 数据源：vLLM connector lifecycle/cleanup 调用点
    #  * 操作：
    #  *   1) 对 runtime-looking 事件做 evidence record 校验
    #  *   2) 控制台只输出标量字段，不输出 evidence record 对象
    #  */
    logger.info("开始记录 connector 事件, event: %s", event)

    # // 2.1 构造并校验事件记录
    record = _validated_connector_event({"event": event, **fields})
    _CONNECTOR_EVENTS.append(record)
    public_record = _public_connector_event(record)

    # // 2.2 输出控制台标量摘要
    parts = ["turbobus_kv_connector_event", f"event={event}"]
    for key, value in public_record.items():
        if key == "event":
            continue
        parts.append(f"{key}={value}")
    print(" ".join(parts), flush=True)
    logger.info("connector 事件记录完成, event: %s", event)


def _validated_connector_event(event: Mapping[str, Any]) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 步骤3：校验 connector 事件边界
    #  * ========================================================================
    #  * 目标：拒绝绕过 RuntimeSession evidence 的公开事件
    #  * 数据源：单条 connector event
    #  * 操作：
    #  *   1) 判断事件是否包含 receipt/lifecycle/cleanup 摘要字段
    #  *   2) 要求 runtime-looking 事件携带 transfer evidence record
    #  */
    logger.info("开始校验 connector 事件边界...")

    # // 3.1 复制事件并判断是否暴露 runtime 摘要字段
    record = dict(event)
    if not _has_runtime_summary(record):
        logger.info("connector 事件边界校验完成, runtime_summary: %s", False)
        return record

    # // 3.2 校验 route policy 不向 transfer 边界暴露
    if bool(record.get("route_policy_visible_to_transfer", False)):
        raise ValueError("connector event exposes route policy to transfer")

    # // 3.3 校验单条或多条 RuntimeSession transfer evidence record
    requires_transfer_record = _requires_transfer_record(record)
    transfer_record = record.get("transfer_evidence_record")
    transfer_records = record.get("transfer_evidence_records")
    if isinstance(transfer_record, Mapping):
        _require_transfer_evidence_record(transfer_record)
    elif isinstance(transfer_records, list):
        if not transfer_records and requires_transfer_record:
            raise ValueError("connector event missing transfer evidence records")
        for item in transfer_records:
            if not isinstance(item, Mapping):
                raise ValueError("connector event transfer evidence records must be mappings")
            _require_transfer_evidence_record(item)
    elif requires_transfer_record:
        raise ValueError("connector event missing transfer evidence record")

    if "free_backing_cleanup_groups" in record:
        _require_runtime_close_entrypoint(record.get("runtime_close_entrypoint"))

    logger.info("connector 事件边界校验完成, runtime_summary: %s", True)
    return record


def _public_connector_event(record: Mapping[str, Any]) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 步骤8：构造公开 connector event
    #  * ========================================================================
    #  * 目标：公开读面只暴露 RuntimeSession 绑定后的标量摘要
    #  * 数据源：已校验 connector event
    #  * 操作：
    #  *   1) 删除 receipt/ticket/decision/topology 原始标识
    #  *   2) 保留 transfer evidence 的 id 和数量摘要
    #  */
    logger.info("开始构造公开 connector event...")

    # // 8.1 复制非私有字段
    public = {
        key: value
        for key, value in record.items()
        if key not in _PRIVATE_EVENT_KEYS
    }

    # // 8.2 记录单条 transfer evidence 摘要
    transfer_record = record.get("transfer_evidence_record")
    if isinstance(transfer_record, Mapping):
        public["transfer_evidence_id"] = str(transfer_record.get("evidence_id", ""))

    # // 8.3 记录多条 transfer evidence 摘要
    transfer_records = record.get("transfer_evidence_records")
    if isinstance(transfer_records, list):
        evidence_ids = [
            str(item.get("evidence_id", ""))
            for item in transfer_records
            if isinstance(item, Mapping) and str(item.get("evidence_id", "")).strip()
        ]
        public["transfer_evidence_count"] = len(evidence_ids)

    # // 8.4 记录 close entrypoint 已绑定摘要
    if "runtime_close_entrypoint" in record:
        public["runtime_close_entrypoint_recorded"] = True

    _require_public_connector_event_no_identity_fields(public)
    logger.info("公开 connector event 构造完成, fields: %s", len(public))
    return public


def _has_runtime_summary(record: Mapping[str, Any]) -> bool:
    # /*
    #  * ========================================================================
    #  * 步骤4：识别 runtime 摘要事件
    #  * ========================================================================
    #  * 目标：区分普通 vLLM 状态事件和 transfer/cleanup 摘要事件
    #  * 数据源：connector event 字段
    #  * 操作：
    #  *   1) 检查 receipt/lifecycle/cleanup 字段
    #  *   2) 返回是否需要 RuntimeSession evidence
    #  */
    logger.info("开始识别 runtime 摘要事件...")

    # // 4.1 检查事件字段集合
    result = any(key in record for key in _RUNTIME_EVIDENCE_KEYS)
    logger.info("runtime 摘要事件识别完成, result: %s", result)
    return result


def _requires_transfer_record(record: Mapping[str, Any]) -> bool:
    # /*
    #  * ========================================================================
    #  * 步骤5：判断 transfer evidence 是否必需
    #  * ========================================================================
    #  * 目标：允许空 cleanup 汇总，但拒绝已有 runtime 摘要无证据
    #  * 数据源：connector event 字段
    #  * 操作：
    #  *   1) 空 prefix cleanup 汇总不要求 transfer record
    #  *   2) 其他 runtime-looking 事件必须有 transfer record
    #  */
    logger.info("开始判断 transfer evidence 是否必需...")

    # // 5.1 识别空 prefix cleanup 汇总
    cleanup_ids = record.get("prefix_cleanup_mutation_ids")
    if cleanup_ids in ("", (), [], None) and not any(
        key in record
        for key in _RUNTIME_EVIDENCE_KEYS - {"prefix_cleanup_mutation_ids"}
    ):
        logger.info("transfer evidence 必需判断完成, required: %s", False)
        return False

    # // 5.2 其他 runtime 摘要必须绑定证据
    logger.info("transfer evidence 必需判断完成, required: %s", True)
    return True


def _require_transfer_evidence_record(record: Mapping[str, Any]) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤6：校验 transfer evidence record
    #  * ========================================================================
    #  * 目标：确认事件来源于 RuntimeSession entrypoint 记录
    #  * 数据源：RuntimeSession transfer evidence record
    #  * 操作：
    #  *   1) 要求 evidence_id 存在
    #  *   2) 要求 intent 和 receipt 都已写入 RuntimeSession 记录
    #  */
    logger.info("开始校验 transfer evidence record...")

    # // 6.1 校验证据标识
    if not str(record.get("evidence_id", "")).strip():
        raise ValueError("connector event transfer evidence record missing evidence_id")

    # // 6.2 校验 RuntimeSession 已记录 intent 与 receipt
    if not bool(record.get("intents_recorded", False)):
        raise ValueError("connector event transfer intents were not recorded")
    if not bool(record.get("receipts_recorded", False)):
        raise ValueError("connector event transfer receipts were not recorded")
    logger.info(
        "transfer evidence record 校验完成, evidence_id: %s",
        record.get("evidence_id"),
    )


def _require_runtime_close_entrypoint(value: object) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤7：校验 connector close RuntimeSession entrypoint
    #  * ========================================================================
    #  * 目标：确认 free backing cleanup summary 来自 RuntimeSession close
    #  * 数据源：connector close event
    #  * 操作：
    #  *   1) 要求 close entrypoint 存在
    #  *   2) 要求 close record 已写入 RuntimeSession entrypoint
    #  */
    logger.info("开始校验 connector close RuntimeSession entrypoint...")

    # // 7.1 校验 RuntimeSession entrypoint 边界
    if not isinstance(value, Mapping):
        raise ValueError("connector close event missing RuntimeSession close entrypoint")
    if str(value.get("entrypoint")) != "TurboBusRuntimeSession":
        raise ValueError("connector close event RuntimeSession entrypoint mismatch")
    if str(value.get("plan_source")) != "daemon_scheduler":
        raise ValueError("connector close event plan_source mismatch")
    if bool(value.get("route_policy_visible_to_transfer", True)):
        raise ValueError("connector close event exposes route policy to transfer")
    if bool(value.get("route_policy_visible_to_application", True)):
        raise ValueError("connector close event exposes route policy to application")

    # // 7.2 校验 RuntimeSession close record
    close_record = value.get("close")
    if not isinstance(close_record, Mapping):
        raise ValueError("connector close event missing RuntimeSession close record")
    if str(close_record.get("entrypoint")) != "TurboBusRuntimeSession.close":
        raise ValueError("connector close event close record mismatch")
    if bool(close_record.get("route_policy_visible_to_transfer", True)):
        raise ValueError("connector close event close record exposes route policy")
    logger.info("connector close RuntimeSession entrypoint 校验完成")


def _require_public_connector_event_no_identity_fields(record: Mapping[str, Any]) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤9：校验公开 connector event 字段
    #  * ========================================================================
    #  * 目标：公开 event 只保留 RuntimeSession-bound 标量摘要
    #  * 操作：
    #  *   1) 递归拒绝 runtime entrypoint、transfer record、receipt contract
    #  *   2) 递归拒绝 receipt/ticket/decision/topology 原始标识
    #  */
    logger.info("开始校验公开 connector event 字段...")

    # // 9.1 递归拒绝公开事件携带运行态 identity
    forbidden = {
        "runtime_entrypoint",
        "transfer_evidence_record",
        "transfer_evidence_records",
        "receipt_contracts",
        "receipt_ids",
        "intent_ids",
        "decision_ids",
        "topology_snapshot_ids",
        "ticket_ids",
        "transfer_ids",
        "receipt_id",
        "intent_id",
        "decision_id",
        "topology_snapshot_id",
        "ticket_id",
        "transfer_id",
        "runtime_close_entrypoint",
    }
    stack: list[Any] = [record]
    while stack:
        value = stack.pop()
        if isinstance(value, Mapping):
            if bool(value.get("route_policy_visible_to_transfer", False)):
                raise ValueError("public connector event exposes route policy")
            leaked = sorted(key for key in forbidden if key in value)
            if leaked:
                raise ValueError(
                    "public connector event exposes RuntimeSession identity fields: "
                    + ", ".join(leaked)
                )
            stack.extend(value.values())
        elif isinstance(value, list | tuple):
            stack.extend(value)

    # // 9.2 完成公开事件校验
    logger.info("公开 connector event 字段校验完成")


__all__ = [
    "clear_connector_events",
    "emit_event",
    "get_connector_events",
]
