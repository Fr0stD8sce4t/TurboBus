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
        _require_adapter_lifecycle_range_contract(lifecycle)
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


def validate_adapter_batch_snapshot(
    snapshot: Mapping[str, object],
) -> RuntimeEvidenceValidationReport:
    # /*
    #  * ========================================================================
    #  * 步骤1：校验 adapter batch 快照边界
    #  * ========================================================================
    #  * 数据源：OffloadBatch.as_dict public snapshot
    #  * 操作：
    #  *   1) 拒绝公开 batch 快照暴露 route policy
    #  *   2) 要求运行态 batch 绑定 RuntimeSession adapter evidence record
    #  */
    logger.info("开始校验 adapter batch 快照边界...")

    # // 1.1 校验 batch 快照基础结构
    if not isinstance(snapshot, Mapping):
        raise TypeError("adapter batch snapshot must be a mapping")
    if bool(snapshot.get("route_policy_visible_to_adapter", True)):
        raise ValueError("adapter batch snapshot exposes physical route policy")

    # // 1.2 空 batch 只能暴露结构状态，不能伪造 receipt evidence
    transfer_state = str(snapshot.get("transfer_state", ""))
    if transfer_state == "empty":
        _require_empty_batch_snapshot(snapshot)
        logger.info("adapter batch 快照边界校验完成, receipts: %s", 0)
        return RuntimeEvidenceValidationReport(
            source="adapter_batch_snapshot",
            receipt_count=0,
        )

    # // 1.3 运行态 batch 必须绑定 RuntimeSession entrypoint record
    if transfer_state != "runtime_session_bound":
        raise ValueError("adapter batch snapshot has unknown transfer_state")
    _require_public_runtime_snapshot_no_identity_fields(
        snapshot,
        source="adapter batch snapshot",
    )
    receipt_count = _require_public_runtime_snapshot_counts(
        snapshot,
        source="adapter batch snapshot",
    )
    logger.info(
        "adapter batch 快照边界校验完成, receipts: %s",
        receipt_count,
    )
    return RuntimeEvidenceValidationReport(
        source="adapter_batch_snapshot",
        receipt_count=receipt_count,
        lifecycle_count=1,
        lifecycle_evidence_ids=(str(snapshot.get("adapter_evidence_id")),),
    )


def validate_adapter_transfer_stats_snapshot(
    snapshot: Mapping[str, object],
) -> RuntimeEvidenceValidationReport:
    # /*
    #  * ========================================================================
    #  * 步骤1：校验 adapter transfer stats 快照
    #  * ========================================================================
    #  * 数据源：adapter-facing transfer_stats snapshot
    #  * 操作：
    #  *   1) 拒绝裸露 route policy 的 direct/relay 统计
    #  *   2) 要求统计来自 RuntimeSession adapter evidence record
    #  */
    logger.info("开始校验 adapter transfer stats 快照...")

    # // 1.1 校验 transfer stats 快照结构
    if not isinstance(snapshot, Mapping):
        raise TypeError("adapter transfer stats snapshot must be a mapping")
    if bool(snapshot.get("route_policy_visible_to_adapter", True)):
        raise ValueError("adapter transfer stats snapshot exposes physical route policy")
    if str(snapshot.get("transfer_state", "")) != "runtime_session_bound":
        raise ValueError("adapter transfer stats snapshot must be RuntimeSession-bound")

    # // 1.2 复用 batch snapshot 的 RuntimeSession entrypoint 校验
    _require_public_runtime_snapshot_no_identity_fields(
        snapshot,
        source="adapter transfer stats snapshot",
    )
    receipt_count = _require_public_runtime_snapshot_counts(
        snapshot,
        source="adapter transfer stats snapshot",
    )

    # // 1.3 校验公开字节摘要
    observed_bytes = int(snapshot.get("bytes", 0) or 0)
    direct_bytes = int(snapshot.get("direct_bytes", 0) or 0)
    relay_bytes = int(snapshot.get("relay_bytes", 0) or 0)
    if observed_bytes != direct_bytes + relay_bytes:
        raise ValueError("adapter transfer stats snapshot byte count mismatch")
    logger.info(
        "adapter transfer stats 快照校验完成, receipts: %s",
        receipt_count,
    )
    return RuntimeEvidenceValidationReport(
        source="adapter_transfer_stats_snapshot",
        receipt_count=receipt_count,
        lifecycle_count=1,
        lifecycle_evidence_ids=(
            str(snapshot.get("adapter_evidence_id")),
        ),
    )


def validate_adapter_transfer_stats_collection(
    snapshot: Mapping[str, object],
) -> RuntimeEvidenceValidationReport:
    # /*
    #  * ========================================================================
    #  * 步骤2：校验 adapter transfer stats 聚合快照
    #  * ========================================================================
    #  * 数据源：vLLM/group-level transfer stats snapshots
    #  * 操作：
    #  *   1) 要求每个 group stats 都已通过 RuntimeSession evidence 绑定
    #  *   2) 核对聚合字节和 receipt 数量不脱离子快照
    #  */
    logger.info("开始校验 adapter transfer stats 聚合快照...")

    # // 2.1 校验聚合快照结构
    if not isinstance(snapshot, Mapping):
        raise TypeError("adapter transfer stats collection must be a mapping")
    if bool(snapshot.get("route_policy_visible_to_adapter", True)):
        raise ValueError("adapter transfer stats collection exposes route policy")
    if str(snapshot.get("transfer_state", "")) != "runtime_session_bound":
        raise ValueError("adapter transfer stats collection must be RuntimeSession-bound")
    _require_public_runtime_snapshot_no_identity_fields(
        snapshot,
        source="adapter transfer stats collection",
    )
    groups = snapshot.get("groups")
    if not isinstance(groups, list | tuple) or not groups:
        raise ValueError("adapter transfer stats collection requires group snapshots")

    # // 2.2 校验每个 group 快照
    receipt_views: list[dict[str, object]] = []
    lifecycle_ids: list[str] = []
    total_receipts = 0
    total_bytes = 0
    for group_snapshot in groups:
        if not isinstance(group_snapshot, Mapping):
            raise TypeError("adapter transfer stats group snapshot must be a mapping")
        report = validate_adapter_transfer_stats_snapshot(group_snapshot)
        receipt_views.extend(dict(item) for item in report.receipts)
        total_receipts += int(report.receipt_count)
        lifecycle_ids.extend(report.lifecycle_evidence_ids)
        total_bytes += int(group_snapshot.get("bytes", 0) or 0)

    # // 2.3 核对聚合摘要
    if int(snapshot.get("receipt_count", 0) or 0) != total_receipts:
        raise ValueError("adapter transfer stats collection receipt_count mismatch")
    if int(snapshot.get("bytes", 0) or 0) != total_bytes:
        raise ValueError("adapter transfer stats collection byte count mismatch")
    logger.info(
        "adapter transfer stats 聚合快照校验完成, receipts: %s",
        total_receipts,
    )
    return RuntimeEvidenceValidationReport(
        source="adapter_transfer_stats_collection",
        receipt_count=total_receipts,
        receipts=tuple(receipt_views),
        lifecycle_count=len(lifecycle_ids),
        lifecycle_evidence_ids=tuple(lifecycle_ids),
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


def _require_empty_batch_snapshot(snapshot: Mapping[str, object]) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤2：校验空 batch 快照
    #  * ========================================================================
    #  * 数据源：OffloadBatch.as_dict empty snapshot
    #  * 操作：
    #  *   1) 允许 block 结构摘要
    #  *   2) 拒绝 receipt/runtime entrypoint 字段伪装成执行 evidence
    #  */
    logger.info("开始校验空 batch 快照...")

    # // 2.1 拒绝空 batch 携带运行态 evidence 字段
    forbidden = {
        "runtime_entrypoint",
        "adapter_evidence_record",
        "receipt_contracts",
        "receipt_ids",
        "intent_ids",
        "decision_ids",
        "topology_snapshot_ids",
        "ticket_ids",
    }
    leaked = sorted(key for key in forbidden if key in snapshot)
    if leaked:
        raise ValueError(
            "empty adapter batch snapshot must not expose runtime evidence: "
            + ", ".join(leaked)
        )
    logger.info("空 batch 快照校验完成")


def _require_public_runtime_snapshot_no_identity_fields(
    snapshot: Mapping[str, object],
    *,
    source: str,
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤3：拒绝公开 RuntimeSession identity 泄漏
    #  * ========================================================================
    #  * 目标：
    #  *   1) RuntimeSession entrypoint 明细只留在 lifecycle 校验内。
    #  *   2) receipt、intent、ticket、decision、topology id 不进入公开层。
    #  *   3) adapter 只能消费标量 evidence 摘要。
    #  */
    logger.info("开始校验公开 RuntimeSession 快照 identity 边界...")

    # // 3.1 拒绝 lifecycle-only 记录进入公开快照
    forbidden = {
        "runtime_entrypoint",
        "adapter_evidence_record",
        "receipt_contracts",
        "receipt_ids",
        "intent_ids",
        "decision_ids",
        "topology_snapshot_ids",
        "ticket_ids",
        "receipt_id",
        "intent_id",
        "decision_id",
        "topology_snapshot_id",
        "ticket_id",
    }
    leaked = sorted(key for key in forbidden if key in snapshot)
    if leaked:
        raise ValueError(
            f"{source} exposes RuntimeSession identity fields: "
            + ", ".join(leaked)
        )

    # // 3.2 确认 route policy 在公开边界保持隐藏
    if bool(snapshot.get("route_policy_visible_to_adapter", True)):
        raise ValueError(f"{source} exposes route policy to adapter")
    logger.info("公开 RuntimeSession 快照 identity 边界校验完成")


def _require_public_runtime_snapshot_counts(
    snapshot: Mapping[str, object],
    *,
    source: str,
) -> int:
    # /*
    #  * ========================================================================
    #  * 步骤4：校验公开 RuntimeSession 标量 evidence 计数
    #  * ========================================================================
    #  * 目标：
    #  *   1) 要求 RuntimeSession adapter evidence id。
    #  *   2) 要求 receipt 与 receipt-contract 计数一致。
    #  *   3) 校验公开字节摘要，不公开 route selection。
    #  */
    logger.info("开始校验公开 RuntimeSession 标量 evidence 计数...")

    # // 4.1 要求 RuntimeSession adapter evidence id
    evidence_id = snapshot.get("adapter_evidence_id")
    if not isinstance(evidence_id, str) or not evidence_id:
        raise ValueError(f"{source} missing adapter_evidence_id")

    # // 4.2 runtime-bound 快照必须有非空 receipt evidence
    receipt_count = int(snapshot.get("receipt_count", 0) or 0)
    contract_count = int(snapshot.get("receipt_contract_count", 0) or 0)
    if receipt_count <= 0:
        raise ValueError(f"{source} requires RuntimeSession receipt evidence")
    if contract_count != receipt_count:
        raise ValueError(f"{source} receipt contract count mismatch")

    # // 4.3 校验标量字节摘要
    direct_bytes = int(snapshot.get("direct_bytes", 0) or 0)
    relay_bytes = int(snapshot.get("relay_bytes", 0) or 0)
    if direct_bytes < 0 or relay_bytes < 0:
        raise ValueError(f"{source} byte counters must be non-negative")
    logger.info(
        "公开 RuntimeSession 标量 evidence 计数校验完成, receipts: %s",
        receipt_count,
    )
    return receipt_count


def _batch_snapshot_lifecycle_view(
    snapshot: Mapping[str, object],
) -> dict[str, object]:
    # /*
    #  * ========================================================================
    #  * 步骤3：构造 batch lifecycle 校验视图
    #  * ========================================================================
    #  * 数据源：batch snapshot runtime_entrypoint 与 receipt_contracts
    #  * 操作：
    #  *   1) 读取 RuntimeSession adapter evidence record
    #  *   2) 生成 runtime/evidence 内部复用的 lifecycle view
    #  */
    logger.info("开始构造 batch lifecycle 校验视图...")

    # // 3.1 提取 RuntimeSession entrypoint
    runtime_entrypoint = snapshot.get("runtime_entrypoint")
    if not isinstance(runtime_entrypoint, Mapping):
        raise ValueError("adapter batch snapshot missing runtime_entrypoint")

    # // 3.2 提取 adapter evidence record
    adapter_record = runtime_entrypoint.get("adapter_evidence_record")
    if not isinstance(adapter_record, Mapping):
        raise ValueError("adapter batch snapshot missing adapter evidence record")
    evidence_id = adapter_record.get("evidence_id")
    if evidence_id is None:
        raise ValueError("adapter batch snapshot adapter evidence missing evidence_id")

    # // 3.3 构造 receipt contract 校验视图
    lifecycle = {
        "evidence_id": str(evidence_id),
        "receipt_contracts": snapshot.get("receipt_contracts"),
    }
    logger.info("batch lifecycle 校验视图构造完成, evidence_id: %s", evidence_id)
    return lifecycle


def _require_batch_adapter_record(snapshot: Mapping[str, object]) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤4：核对 batch adapter evidence record
    #  * ========================================================================
    #  * 数据源：batch snapshot 与 runtime_entrypoint.adapter_evidence_record
    #  * 操作：
    #  *   1) 要求 public adapter_evidence_record 来自 RuntimeSession entrypoint
    #  *   2) 核对 intent/receipt 已在 RuntimeSession 记录中
    #  */
    logger.info("开始核对 batch adapter evidence record...")

    # // 4.1 读取 public 与 entrypoint 两份 record
    public_record = snapshot.get("adapter_evidence_record")
    runtime_entrypoint = snapshot.get("runtime_entrypoint")
    if not isinstance(public_record, Mapping) or not isinstance(
        runtime_entrypoint,
        Mapping,
    ):
        raise ValueError("adapter batch snapshot missing adapter evidence record")
    entrypoint_record = runtime_entrypoint.get("adapter_evidence_record")
    if not isinstance(entrypoint_record, Mapping):
        raise ValueError("adapter batch snapshot missing runtime adapter record")

    # // 4.2 核对 record 标识与记录状态
    if str(public_record.get("evidence_id")) != str(entrypoint_record.get("evidence_id")):
        raise ValueError("adapter batch snapshot adapter evidence_id mismatch")
    if not bool(public_record.get("intents_recorded", False)):
        raise ValueError("adapter batch snapshot adapter evidence missing intents")
    if not bool(public_record.get("receipts_recorded", False)):
        raise ValueError("adapter batch snapshot adapter evidence missing receipts")
    logger.info(
        "batch adapter evidence record 核对完成, evidence_id: %s",
        public_record.get("evidence_id"),
    )


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
    _require_adapter_lifecycle_recovery_contract(lifecycle)
    logger.info("适配器生命周期边界校验完成")


def _require_adapter_lifecycle_recovery_contract(
    lifecycle: Mapping[str, object],
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤8：校验 adapter recovery 摘要
    #  * ========================================================================
    #  * 数据源：adapter lifecycle daemon_recovery 字段
    #  * 操作：
    #  *   1) 允许 RuntimeSession recovery 标量摘要
    #  *   2) 拒绝 queue/ticket/lease/buffer 等 daemon 内部细节暴露给 adapter
    #  */
    logger.info("开始校验 adapter recovery 摘要...")

    # // 8.1 空 recovery 直接通过
    recovery = lifecycle.get("daemon_recovery")
    if recovery is None:
        logger.info("adapter recovery 摘要校验完成, count: %s", 0)
        return
    if not isinstance(recovery, list | tuple):
        raise TypeError("adapter lifecycle daemon_recovery must be a sequence")

    # // 8.2 校验每条 recovery 摘要不含 daemon 内部细节
    for item in recovery:
        if not isinstance(item, Mapping):
            raise TypeError("adapter lifecycle daemon_recovery items must be mappings")
        if bool(item.get("route_policy_visible_to_adapter", True)):
            raise ValueError("adapter recovery exposes route policy")
        _require_no_runtime_identity_fields(
            item,
            source="adapter lifecycle daemon_recovery",
        )
        leaked = sorted(
            key
            for key in (
                "admission",
                "queue_record",
                "ticket",
                "reservations",
                "leases",
                "buffer_snapshots",
                "cleanup_targets",
                "completion_evidence",
            )
            if key in item
        )
        if leaked:
            raise ValueError(
                "adapter recovery must use RuntimeSession recovery summary "
                "instead of " + ", ".join(leaked)
            )
    logger.info("adapter recovery 摘要校验完成, count: %s", len(recovery))


def _require_adapter_lifecycle_range_contract(
    lifecycle: Mapping[str, object],
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤6：校验 adapter range/binding 摘要
    #  * ========================================================================
    #  * 数据源：adapter lifecycle extra range 与 buffer binding 字段
    #  * 操作：
    #  *   1) 拒绝 range/binding 字段自带 receipt/ticket/decision/topology
    #  *   2) 要求运行态对齐只来自 RuntimeSession adapter evidence record
    #  */
    logger.info("开始校验 adapter range/binding 摘要...")

    # // 6.1 校验 runtime_buffer_binding
    binding = lifecycle.get("runtime_buffer_binding")
    if isinstance(binding, Mapping):
        _require_no_runtime_identity_fields(
            binding,
            source="adapter runtime_buffer_binding",
        )
        if bool(binding.get("route_policy_visible_to_adapter", True)):
            raise ValueError("adapter runtime_buffer_binding exposes route policy")

    # // 6.2 校验 range/binding 集合
    for field_name in (
        "bucket_ranges",
        "bucket_bindings",
        "tensor_bindings",
    ):
        value = lifecycle.get(field_name)
        if value is None:
            continue
        if not isinstance(value, list | tuple):
            raise TypeError(f"adapter lifecycle {field_name} must be a sequence")
        for item in value:
            if not isinstance(item, Mapping):
                raise TypeError(f"adapter lifecycle {field_name} items must be mappings")
            _require_no_runtime_identity_fields(
                item,
                source=f"adapter lifecycle {field_name}",
            )
            if bool(item.get("route_policy_visible_to_adapter", False)):
                raise ValueError(f"adapter lifecycle {field_name} exposes route policy")
    logger.info("adapter range/binding 摘要校验完成")


def _require_no_runtime_identity_fields(
    value: Mapping[str, object],
    *,
    source: str,
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤7：拒绝裸运行态标识字段
    #  * ========================================================================
    #  * 数据源：adapter lifecycle extra mapping
    #  * 操作：
    #  *   1) 检查 receipt/ticket/decision/topology 标识字段
    #  *   2) 发现裸运行态字段立即拒绝
    #  */
    logger.info("开始检查裸运行态标识字段...")

    # // 7.1 拒绝容易绕过 RuntimeSession record 的运行态字段
    forbidden = {
        "last_intent_id",
        "last_receipt_id",
        "last_ticket_id",
        "last_decision_id",
        "last_topology_snapshot_id",
        "last_receipt_state",
        "last_transfer_error",
        "intent_id",
        "receipt_id",
        "ticket_id",
        "decision_id",
        "topology_snapshot_id",
    }
    leaked = sorted(key for key in forbidden if key in value)
    if leaked:
        raise ValueError(
            f"{source} must use RuntimeSession adapter evidence instead of "
            + ", ".join(leaked)
        )
    logger.info("裸运行态标识字段检查完成")


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
    "validate_adapter_batch_snapshot",
    "validate_adapter_lifecycle_evidence",
    "validate_adapter_transfer_stats_collection",
    "validate_adapter_transfer_stats_snapshot",
    "validate_runtime_receipts",
]
