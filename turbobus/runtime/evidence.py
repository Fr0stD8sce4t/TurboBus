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
    _require_runtime_entrypoint_contract(lifecycle.get("runtime_entrypoint"))
    logger.info("适配器生命周期边界校验完成")


def _require_runtime_entrypoint_contract(value: object) -> None:
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
    logger.info("RuntimeSession 入口合约校验完成")


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
