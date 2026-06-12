from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Iterable, Mapping

from ..schema import TransferReceipt
from .validation import validated_real_execution_evidence

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RuntimeEvidenceValidationReport:
    source: str
    receipt_count: int
    receipts: tuple[dict[str, object], ...] = field(default_factory=tuple)
    lifecycle_count: int = 0
    lifecycle_evidence_ids: tuple[str, ...] = field(default_factory=tuple)

    def as_dict(self) -> dict[str, object]:
        return {
            "schema": "turbobus.runtime_evidence_validation.v1",
            "source": self.source,
            "receipt_count": int(self.receipt_count),
            "receipts": [dict(item) for item in self.receipts],
            "lifecycle_count": int(self.lifecycle_count),
            "lifecycle_evidence_ids": list(self.lifecycle_evidence_ids),
            "fake_receipt": False,
            "synthetic_evidence": False,
            "dry_run": False,
        }


def validate_runtime_receipts(
    receipts: Iterable[TransferReceipt],
    *,
    source: str = "TransferReceipt",
) -> RuntimeEvidenceValidationReport:
    receipt_views = tuple(
        validated_real_execution_evidence(receipt)
        for receipt in _require_receipts(receipts)
    )
    if not receipt_views:
        raise ValueError("runtime evidence validation requires at least one receipt")
    return RuntimeEvidenceValidationReport(
        source=str(source),
        receipt_count=len(receipt_views),
        receipts=receipt_views,
    )


def validate_adapter_lifecycle_evidence(
    lifecycle_evidence: Mapping[str, object] | Iterable[Mapping[str, object]],
) -> RuntimeEvidenceValidationReport:
    lifecycles = _normalize_lifecycle_evidence(lifecycle_evidence)
    receipt_views: list[dict[str, object]] = []
    lifecycle_ids: list[str] = []
    for lifecycle in lifecycles:
        lifecycle_ids.append(str(lifecycle.get("evidence_id", "")))
        _require_adapter_lifecycle_contract(lifecycle)
        receipt_views.extend(_receipt_views_from_lifecycle(lifecycle))
    if not receipt_views:
        raise ValueError("adapter lifecycle evidence contains no receipt contracts")
    return RuntimeEvidenceValidationReport(
        source="adapter_lifecycle_evidence",
        receipt_count=len(receipt_views),
        receipts=tuple(receipt_views),
        lifecycle_count=len(lifecycles),
        lifecycle_evidence_ids=tuple(
            evidence_id for evidence_id in lifecycle_ids if evidence_id
        ),
    )


def _require_receipts(receipts: Iterable[TransferReceipt]) -> tuple[TransferReceipt, ...]:
    resolved = tuple(receipts)
    for index, receipt in enumerate(resolved):
        if not isinstance(receipt, TransferReceipt):
            raise TypeError(
                "runtime evidence validation accepts only TransferReceipt objects; "
                f"item {index} is {type(receipt).__name__}"
            )
    return resolved


def _normalize_lifecycle_evidence(
    lifecycle_evidence: Mapping[str, object] | Iterable[Mapping[str, object]],
) -> tuple[Mapping[str, object], ...]:
    if isinstance(lifecycle_evidence, Mapping):
        return (lifecycle_evidence,)
    resolved = tuple(lifecycle_evidence)
    for index, item in enumerate(resolved):
        if not isinstance(item, Mapping):
            raise TypeError(
                "adapter lifecycle validation accepts only mapping evidence; "
                f"item {index} is {type(item).__name__}"
            )
    return resolved


def _require_adapter_lifecycle_contract(lifecycle: Mapping[str, object]) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤1：校验适配器生命周期边界
    #  * ========================================================================
    #  * 数据源：adapter lifecycle evidence
    #  * 操作：
    #  *   1) 校验 RuntimeSession、TransferIntent、TransferReceipt 来源
    #  *   2) 拒绝 fake evidence 和 route policy 暴露
    #  */
    logger.info("开始校验适配器生命周期边界...")

    # // 1.1 校验生产来源字段
    required_sources = {
        "buffer_registration_source": "TurboBusRuntimeSession",
        "intent_source": "TransferIntent",
        "receipt_source": "TransferReceipt",
        "policy_source": "daemon_scheduler",
        "physical_route_source": "daemon_scheduler",
    }
    for key, expected in required_sources.items():
        observed = lifecycle.get(key)
        if str(observed) != expected:
            raise ValueError(
                "adapter lifecycle evidence "
                f"{key} must be {expected}, got {observed!r}"
            )

    # // 1.2 拒绝 adapter 可见物理路径策略
    if bool(lifecycle.get("route_policy_visible_to_adapter", True)):
        raise ValueError("adapter lifecycle exposes physical route policy")

    # // 1.3 校验 RuntimeSession entrypoint 合约
    _require_runtime_entrypoint_contract(
        lifecycle.get("runtime_entrypoint"),
        lifecycle=lifecycle,
    )
    logger.info("适配器生命周期边界校验完成")


def _require_runtime_entrypoint_contract(
    value: object,
    *,
    lifecycle: Mapping[str, object],
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤2：校验 RuntimeSession 入口合约
    #  * ========================================================================
    #  * 数据源：runtime_entrypoint contract
    #  * 操作：
    #  *   1) 确认唯一入口是 TurboBusRuntimeSession
    #  *   2) 确认 daemon_scheduler 是唯一 plan 来源
    #  *   3) 确认 adapter intent/receipt 已被 RuntimeSession 记录
    #  */
    logger.info("开始校验 RuntimeSession 入口合约...")

    # // 2.1 校验入口对象和 plan 来源
    if not isinstance(value, Mapping):
        raise ValueError("adapter lifecycle evidence missing runtime_entrypoint")
    expected = {
        "schema": "turbobus.runtime_session_entrypoint.v1",
        "entrypoint": "TurboBusRuntimeSession",
        "plan_source": "daemon_scheduler",
    }
    for key, expected_value in expected.items():
        observed = value.get(key)
        if str(observed) != expected_value:
            raise ValueError(
                "adapter lifecycle runtime_entrypoint "
                f"{key} must be {expected_value}, got {observed!r}"
            )
    if bool(value.get("route_policy_visible_to_application", True)):
        raise ValueError("runtime entrypoint exposes route policy to application")
    if bool(value.get("route_policy_visible_to_adapter", True)):
        raise ValueError("runtime entrypoint exposes route policy to adapter")

    # // 2.2 校验 RuntimeSession 已记录 adapter intent 与 receipt
    if not bool(value.get("intents_recorded", False)):
        raise ValueError("runtime entrypoint did not record adapter intents")
    if not bool(value.get("receipts_recorded", False)):
        raise ValueError("runtime entrypoint did not record adapter receipts")
    if not bool(value.get("adapter_context_recorded", False)):
        raise ValueError("runtime entrypoint did not record adapter construction")
    if not bool(value.get("adapter_evidence_recorded", False)):
        raise ValueError("runtime entrypoint did not record adapter lifecycle evidence")
    _require_runtime_entrypoint_adapter_evidence(value, lifecycle=lifecycle)
    logger.info("RuntimeSession 入口合约校验完成")


def _require_runtime_entrypoint_adapter_evidence(
    value: Mapping[str, object],
    *,
    lifecycle: Mapping[str, object],
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤3：校验 adapter evidence 记录明细
    #  * ========================================================================
    #  * 数据源：runtime_entrypoint.adapter_evidence_record
    #  * 操作：
    #  *   1) 核对 lifecycle evidence_id 已写入 RuntimeSession
    #  *   2) 核对 intent 与 receipt 合约没有脱离 entrypoint record
    #  */
    logger.info("开始校验 adapter evidence 记录明细...")

    # // 3.1 读取 lifecycle 标识与 RuntimeSession 记录
    evidence_id = lifecycle.get("evidence_id")
    if evidence_id is None:
        raise ValueError("adapter lifecycle evidence missing evidence_id")
    record = value.get("adapter_evidence_record")
    if not isinstance(record, Mapping):
        raise ValueError("runtime entrypoint missing adapter evidence record")
    if str(record.get("evidence_id")) != str(evidence_id):
        raise ValueError("runtime entrypoint adapter evidence_id mismatch")

    # // 3.2 核对 RuntimeSession 记录内的 intent 与 receipt 明细
    if not bool(record.get("intents_recorded", False)):
        raise ValueError("runtime entrypoint adapter evidence missing intents")
    if not bool(record.get("receipts_recorded", False)):
        raise ValueError("runtime entrypoint adapter evidence missing receipts")
    expected_intent_ids, expected_receipt_ids = _receipt_contract_identity_sets(
        lifecycle
    )
    recorded_intent_ids = _string_set(record.get("intent_ids"))
    recorded_receipt_ids = _string_set(record.get("receipt_ids"))
    if not expected_intent_ids.issubset(recorded_intent_ids):
        raise ValueError("runtime entrypoint adapter evidence intent_ids mismatch")
    if not expected_receipt_ids.issubset(recorded_receipt_ids):
        raise ValueError("runtime entrypoint adapter evidence receipt_ids mismatch")
    logger.info("adapter evidence 记录明细校验完成, evidence_id: %s", evidence_id)


def _receipt_contract_identity_sets(
    lifecycle: Mapping[str, object],
) -> tuple[set[str], set[str]]:
    # /*
    #  * ========================================================================
    #  * 步骤4：提取 receipt contract 标识集合
    #  * ========================================================================
    #  * 数据源：adapter lifecycle receipt_contracts
    #  * 操作：
    #  *   1) 读取每个 receipt contract 的 intent_id 和 receipt_id
    #  *   2) 返回用于 RuntimeSession adapter evidence 核对的集合
    #  */
    logger.info("开始提取 receipt contract 标识集合...")

    # // 4.1 校验 receipt_contracts 结构
    contracts = lifecycle.get("receipt_contracts")
    if not isinstance(contracts, list | tuple):
        raise ValueError("adapter lifecycle evidence missing receipt_contracts")

    # // 4.2 收集 intent_id 与 receipt_id
    intent_ids: set[str] = set()
    receipt_ids: set[str] = set()
    for index, contract in enumerate(contracts):
        if not isinstance(contract, Mapping):
            raise TypeError(
                "adapter lifecycle receipt_contracts must be mappings; "
                f"item {index} is {type(contract).__name__}"
            )
        intent_id = contract.get("intent_id")
        receipt_id = contract.get("receipt_id")
        if intent_id is None or receipt_id is None:
            raise ValueError(
                "adapter lifecycle receipt contract missing identity fields"
            )
        intent_ids.add(str(intent_id))
        receipt_ids.add(str(receipt_id))

    # // 4.3 拒绝空 receipt contract
    if not receipt_ids:
        raise ValueError("adapter lifecycle evidence contains no receipt contracts")
    logger.info("receipt contract 标识集合提取完成, receipts: %s", len(receipt_ids))
    return intent_ids, receipt_ids


def _string_set(value: object) -> set[str]:
    # /*
    #  * ========================================================================
    #  * 步骤5：归一化字符串集合
    #  * ========================================================================
    #  * 数据源：RuntimeSession adapter evidence 记录字段
    #  * 操作：
    #  *   1) 字符串按单个标识处理
    #  *   2) 其他可迭代对象转为字符串集合
    #  */
    logger.info("开始归一化字符串集合...")

    # // 5.1 字符串按单值处理
    if isinstance(value, str):
        result = {value}
        logger.info("字符串集合归一化完成, count: %s", len(result))
        return result

    # // 5.2 可迭代对象转为字符串集合
    if isinstance(value, Iterable):
        result = {str(item) for item in value}
        logger.info("字符串集合归一化完成, count: %s", len(result))
        return result

    # // 5.3 非序列值返回空集合
    logger.info("字符串集合归一化完成, count: %s", 0)
    return set()


def _receipt_views_from_lifecycle(
    lifecycle: Mapping[str, object],
) -> list[dict[str, object]]:
    contracts = lifecycle.get("receipt_contracts")
    if not isinstance(contracts, list | tuple):
        raise ValueError("adapter lifecycle evidence missing receipt_contracts")
    views: list[dict[str, object]] = []
    for index, contract in enumerate(contracts):
        if not isinstance(contract, Mapping):
            raise TypeError(
                "adapter lifecycle receipt_contracts must be mappings; "
                f"item {index} is {type(contract).__name__}"
            )
        views.append(_receipt_view_from_contract(contract))
    return views


def _receipt_view_from_contract(contract: Mapping[str, object]) -> dict[str, object]:
    required_fields = (
        "receipt_id",
        "intent_id",
        "decision_id",
        "topology_snapshot_id",
        "ticket_id",
        "job_id",
        "session_id",
        "state",
        "bytes_total",
        "bytes_completed",
        "completion_source",
        "transfer_id",
    )
    missing = [field_name for field_name in required_fields if contract.get(field_name) is None]
    if missing:
        raise ValueError(
            "adapter lifecycle receipt contract missing fields: "
            + ", ".join(missing)
        )
    completion_source = str(contract.get("completion_source", "")).lower()
    if completion_source not in {"worker", "backend"}:
        raise ValueError(
            "adapter lifecycle receipt contract missing worker/backend completion source"
        )
    if not bool(contract.get("verified", False)):
        raise ValueError("adapter lifecycle receipt contract missing verification")
    verified_bytes = int(contract.get("verified_bytes", 0) or 0)
    bytes_total = int(contract.get("bytes_total", 0) or 0)
    if verified_bytes != bytes_total:
        raise ValueError("adapter lifecycle receipt contract verified bytes mismatch")
    completion_contract = contract.get("completion_contract")
    if not isinstance(completion_contract, Mapping):
        raise ValueError("adapter lifecycle receipt contract missing completion contract")
    return {
        "source": "adapter_lifecycle_receipt_contract",
        "fake_receipt": False,
        "synthetic_evidence": False,
        "dry_run": False,
        "receipt_id": str(contract["receipt_id"]),
        "intent_id": str(contract["intent_id"]),
        "decision_id": str(contract["decision_id"]),
        "topology_snapshot_id": str(contract["topology_snapshot_id"]),
        "ticket_id": str(contract["ticket_id"]),
        "job_id": str(contract["job_id"]),
        "session_id": str(contract["session_id"]),
        "state": str(contract["state"]),
        "bytes_total": bytes_total,
        "bytes_completed": int(contract.get("bytes_completed", 0) or 0),
        "completion_source": completion_source,
        "transfer_id": str(contract["transfer_id"]),
        "verified": True,
        "verified_bytes": verified_bytes,
        "route_policy_source": "daemon_scheduler",
        "completion_contract": dict(completion_contract),
    }


__all__ = [
    "RuntimeEvidenceValidationReport",
    "validate_adapter_lifecycle_evidence",
    "validate_runtime_receipts",
]
