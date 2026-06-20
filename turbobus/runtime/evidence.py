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


def _validate_transfer_lifecycle_contracts(
    lifecycle_evidence: Mapping[str, object] | Iterable[Mapping[str, object]],
) -> RuntimeEvidenceValidationReport:
    lifecycles = _normalize_lifecycle_evidence(lifecycle_evidence)
    receipt_views: list[dict[str, object]] = []
    lifecycle_ids: list[str] = []
    for lifecycle in lifecycles:
        lifecycle_ids.append(str(lifecycle.get("evidence_id", "")))
        _require_transfer_lifecycle_contract(lifecycle)
        _require_transfer_lifecycle_range_contract(lifecycle)
        receipt_views.extend(_receipt_views_from_lifecycle(lifecycle))
    if not receipt_views:
        raise ValueError("transfer lifecycle evidence contains no receipt contracts")
    return RuntimeEvidenceValidationReport(
        source="transfer_lifecycle_evidence",
        receipt_count=len(receipt_views),
        receipts=tuple(receipt_views),
        lifecycle_count=len(lifecycles),
        lifecycle_evidence_ids=tuple(
            evidence_id for evidence_id in lifecycle_ids if evidence_id
        ),
    )


def validate_transfer_lifecycle_evidence(
    lifecycle_evidence: Mapping[str, object] | Iterable[Mapping[str, object]],
) -> RuntimeEvidenceValidationReport:
    # /*
    #  * ========================================================================
    #  * 步骤1：校验 transfer 生命周期证据
    #  * ========================================================================
    #  * 数据源：StateOffloadCore / OffloadStore lifecycle evidence
    #  * 操作：
    #  *   1) 复用现有 RuntimeSession evidence 合约
    #  *   2) 返回 transfer 命名的校验报告
    #  */
    logger.info("开始校验 transfer 生命周期证据...")

    # // 1.1 校验 transfer lifecycle 合约中的真实 receipt
    report = _validate_transfer_lifecycle_contracts(lifecycle_evidence)

    # // 1.2 强制校验 transfer 命名入口合约
    lifecycles = _normalize_lifecycle_evidence(lifecycle_evidence)
    for lifecycle in lifecycles:
        _require_transfer_lifecycle_entrypoint(lifecycle)

    # // 1.3 返回 transfer 命名报告
    result = RuntimeEvidenceValidationReport(
        source="transfer_lifecycle_evidence",
        receipt_count=report.receipt_count,
        receipts=report.receipts,
        lifecycle_count=report.lifecycle_count,
        lifecycle_evidence_ids=report.lifecycle_evidence_ids,
    )
    logger.info("transfer 生命周期证据校验完成, receipts: %s", result.receipt_count)
    return result


def _validate_transfer_batch_contract(
    snapshot: Mapping[str, object],
) -> RuntimeEvidenceValidationReport:
    # /*
    #  * ========================================================================
    #  * 步骤1：校验 transfer batch 快照边界
    #  * ========================================================================
    #  * 数据源：OffloadBatch.as_dict public snapshot
    #  * 操作：
    #  *   1) 拒绝公开 batch 快照暴露 route policy
    #  *   2) 要求运行态 batch 绑定 RuntimeSession transfer evidence record
    #  */
    logger.info("开始校验 transfer batch 快照边界...")

    # // 1.1 校验 batch 快照基础结构
    if not isinstance(snapshot, Mapping):
        raise TypeError("transfer batch snapshot must be a mapping")
    if bool(snapshot.get("route_policy_visible_to_transfer", True)):
        raise ValueError("transfer batch snapshot exposes physical route policy")

    # // 1.2 空 batch 只能暴露结构状态，不能伪造 receipt evidence
    transfer_state = str(snapshot.get("transfer_state", ""))
    if transfer_state == "empty":
        _require_empty_batch_snapshot(snapshot)
        logger.info("transfer batch 快照边界校验完成, receipts: %s", 0)
        return RuntimeEvidenceValidationReport(
            source="transfer_batch_snapshot",
            receipt_count=0,
        )

    # // 1.3 运行态 batch 必须绑定 RuntimeSession entrypoint record
    if transfer_state != "runtime_session_bound":
        raise ValueError("transfer batch snapshot has unknown transfer_state")
    _require_public_runtime_snapshot_no_identity_fields(
        snapshot,
        source="transfer batch snapshot",
    )
    receipt_count = _require_public_runtime_snapshot_counts(
        snapshot,
        source="transfer batch snapshot",
    )
    logger.info(
        "transfer batch 快照边界校验完成, receipts: %s",
        receipt_count,
    )
    return RuntimeEvidenceValidationReport(
        source="transfer_batch_snapshot",
        receipt_count=receipt_count,
        lifecycle_count=1,
        lifecycle_evidence_ids=(str(snapshot.get("transfer_evidence_id")),),
    )


def validate_transfer_batch_snapshot(
    snapshot: Mapping[str, object],
) -> RuntimeEvidenceValidationReport:
    # /*
    #  * ========================================================================
    #  * 步骤1：校验 transfer batch 快照
    #  * ========================================================================
    #  * 数据源：OffloadBatch.as_dict public snapshot
    #  * 操作：
    #  *   1) 复用现有 RuntimeSession 公开快照边界
    #  *   2) 返回 transfer 命名的校验报告
    #  */
    logger.info("开始校验 transfer batch 快照...")

    # // 1.1 校验 transfer batch 公开边界
    report = _validate_transfer_batch_contract(snapshot)

    # // 1.2 运行态快照必须暴露 transfer evidence id
    evidence_ids = _transfer_snapshot_evidence_ids(snapshot)

    # // 1.3 返回 transfer 命名报告
    result = RuntimeEvidenceValidationReport(
        source="transfer_batch_snapshot",
        receipt_count=report.receipt_count,
        receipts=report.receipts,
        lifecycle_count=report.lifecycle_count,
        lifecycle_evidence_ids=tuple(evidence_ids) or report.lifecycle_evidence_ids,
    )
    logger.info("transfer batch 快照校验完成, receipts: %s", result.receipt_count)
    return result


def _validate_transfer_stats_contract(
    snapshot: Mapping[str, object],
) -> RuntimeEvidenceValidationReport:
    # /*
    #  * ========================================================================
    #  * 步骤1：校验 transfer stats 快照
    #  * ========================================================================
    #  * 数据源：transfer-facing transfer_stats snapshot
    #  * 操作：
    #  *   1) 拒绝裸露 route policy 的 direct/relay 统计
    #  *   2) 要求统计来自 RuntimeSession transfer evidence record
    #  */
    logger.info("开始校验 transfer stats 快照...")

    # // 1.1 校验 transfer stats 快照结构
    if not isinstance(snapshot, Mapping):
        raise TypeError("transfer stats snapshot must be a mapping")
    if bool(snapshot.get("route_policy_visible_to_transfer", True)):
        raise ValueError("transfer stats snapshot exposes physical route policy")
    if str(snapshot.get("transfer_state", "")) != "runtime_session_bound":
        raise ValueError("transfer stats snapshot must be RuntimeSession-bound")

    # // 1.2 复用 batch snapshot 的 RuntimeSession entrypoint 校验
    _require_public_runtime_snapshot_no_identity_fields(
        snapshot,
        source="transfer stats snapshot",
    )
    receipt_count = _require_public_runtime_snapshot_counts(
        snapshot,
        source="transfer stats snapshot",
    )

    # // 1.3 校验公开字节摘要
    observed_bytes = int(snapshot.get("bytes", 0) or 0)
    direct_bytes = int(snapshot.get("direct_bytes", 0) or 0)
    relay_bytes = int(snapshot.get("relay_bytes", 0) or 0)
    if observed_bytes != direct_bytes + relay_bytes:
        raise ValueError("transfer stats snapshot byte count mismatch")
    logger.info(
        "transfer stats 快照校验完成, receipts: %s",
        receipt_count,
    )
    return RuntimeEvidenceValidationReport(
        source="transfer_stats_snapshot",
        receipt_count=receipt_count,
        lifecycle_count=1,
        lifecycle_evidence_ids=(
            str(snapshot.get("transfer_evidence_id")),
        ),
    )


def validate_transfer_stats_snapshot(
    snapshot: Mapping[str, object],
) -> RuntimeEvidenceValidationReport:
    # /*
    #  * ========================================================================
    #  * 步骤1：校验 transfer stats 快照
    #  * ========================================================================
    #  * 数据源：RuntimeSession-bound transfer stats snapshot
    #  * 操作：
    #  *   1) 复用现有 RuntimeSession 公开统计边界
    #  *   2) 返回 transfer 命名的校验报告
    #  */
    logger.info("开始校验 transfer stats 快照...")

    # // 1.1 校验 transfer stats 公开边界
    report = _validate_transfer_stats_contract(snapshot)

    # // 1.2 运行态快照必须暴露 transfer evidence id
    evidence_ids = _transfer_snapshot_evidence_ids(snapshot)

    # // 1.3 返回 transfer 命名报告
    result = RuntimeEvidenceValidationReport(
        source="transfer_stats_snapshot",
        receipt_count=report.receipt_count,
        receipts=report.receipts,
        lifecycle_count=report.lifecycle_count,
        lifecycle_evidence_ids=tuple(evidence_ids) or report.lifecycle_evidence_ids,
    )
    logger.info("transfer stats 快照校验完成, receipts: %s", result.receipt_count)
    return result


def _validate_transfer_stats_collection_contract(
    snapshot: Mapping[str, object],
) -> RuntimeEvidenceValidationReport:
    # /*
    #  * ========================================================================
    #  * 步骤2：校验 transfer stats 聚合快照
    #  * ========================================================================
    #  * 数据源：vLLM/group-level transfer stats snapshots
    #  * 操作：
    #  *   1) 要求每个 group stats 都已通过 RuntimeSession evidence 绑定
    #  *   2) 核对聚合字节和 receipt 数量不脱离子快照
    #  */
    logger.info("开始校验 transfer stats 聚合快照...")

    # // 2.1 校验聚合快照结构
    if not isinstance(snapshot, Mapping):
        raise TypeError("transfer stats collection must be a mapping")
    if bool(snapshot.get("route_policy_visible_to_transfer", True)):
        raise ValueError("transfer stats collection exposes route policy")
    if str(snapshot.get("transfer_state", "")) != "runtime_session_bound":
        raise ValueError("transfer stats collection must be RuntimeSession-bound")
    _require_public_runtime_snapshot_no_identity_fields(
        snapshot,
        source="transfer stats collection",
    )
    groups = snapshot.get("groups")
    if not isinstance(groups, list | tuple) or not groups:
        raise ValueError("transfer stats collection requires group snapshots")

    # // 2.2 校验每个 group 快照
    receipt_views: list[dict[str, object]] = []
    lifecycle_ids: list[str] = []
    total_receipts = 0
    total_bytes = 0
    for group_snapshot in groups:
        if not isinstance(group_snapshot, Mapping):
            raise TypeError("transfer stats group snapshot must be a mapping")
        report = _validate_transfer_stats_contract(group_snapshot)
        receipt_views.extend(dict(item) for item in report.receipts)
        total_receipts += int(report.receipt_count)
        lifecycle_ids.extend(report.lifecycle_evidence_ids)
        total_bytes += int(group_snapshot.get("bytes", 0) or 0)

    # // 2.3 核对聚合摘要
    if int(snapshot.get("receipt_count", 0) or 0) != total_receipts:
        raise ValueError("transfer stats collection receipt_count mismatch")
    if int(snapshot.get("bytes", 0) or 0) != total_bytes:
        raise ValueError("transfer stats collection byte count mismatch")
    logger.info(
        "transfer stats 聚合快照校验完成, receipts: %s",
        total_receipts,
    )
    return RuntimeEvidenceValidationReport(
        source="transfer_stats_collection",
        receipt_count=total_receipts,
        receipts=tuple(receipt_views),
        lifecycle_count=len(lifecycle_ids),
        lifecycle_evidence_ids=tuple(lifecycle_ids),
    )


def validate_transfer_stats_collection(
    snapshot: Mapping[str, object],
) -> RuntimeEvidenceValidationReport:
    # /*
    #  * ========================================================================
    #  * 步骤1：校验 transfer stats 聚合快照
    #  * ========================================================================
    #  * 数据源：group-level transfer stats snapshots
    #  * 操作：
    #  *   1) 复用现有 RuntimeSession 公开聚合边界
    #  *   2) 返回 transfer 命名的校验报告
    #  */
    logger.info("开始校验 transfer stats 聚合快照...")

    # // 1.1 校验 transfer stats collection 公开边界
    report = _validate_transfer_stats_collection_contract(snapshot)

    # // 1.2 每个 group 快照必须暴露 transfer evidence id
    evidence_ids: list[str] = []
    groups = snapshot.get("groups")
    if isinstance(groups, list | tuple):
        for group_snapshot in groups:
            if isinstance(group_snapshot, Mapping):
                evidence_ids.extend(_transfer_snapshot_evidence_ids(group_snapshot))

    # // 1.3 返回 transfer 命名报告
    result = RuntimeEvidenceValidationReport(
        source="transfer_stats_collection",
        receipt_count=report.receipt_count,
        receipts=report.receipts,
        lifecycle_count=report.lifecycle_count,
        lifecycle_evidence_ids=tuple(evidence_ids) or report.lifecycle_evidence_ids,
    )
    logger.info("transfer stats 聚合快照校验完成, receipts: %s", result.receipt_count)
    return result


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
        "transfer_evidence_record",
        "transfer_evidence_records",
        "runtime_close_entrypoint",
        "receipt_contracts",
        "receipt_ids",
        "intent_ids",
        "decision_ids",
        "topology_snapshot_ids",
        "ticket_ids",
        "transfer_ids",
        "daemon_recovery",
    }
    leaked = sorted(key for key in forbidden if key in snapshot)
    if leaked:
        raise ValueError(
            "empty transfer batch snapshot must not expose runtime evidence: "
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
    #  *   3) transfer callers only consume scalar evidence summaries.
    #  */
    logger.info("开始校验公开 RuntimeSession 快照 identity 边界...")

    # // 3.1 拒绝 lifecycle-only 记录进入公开快照
    forbidden = {
        "runtime_entrypoint",
        "transfer_evidence_record",
        "transfer_evidence_records",
        "runtime_close_entrypoint",
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
        "daemon_recovery",
    }
    leaked = sorted(key for key in forbidden if key in snapshot)
    if leaked:
        raise ValueError(
            f"{source} exposes RuntimeSession identity fields: "
            + ", ".join(leaked)
        )

    # // 3.2 确认 route policy 在公开边界保持隐藏
    if bool(snapshot.get("route_policy_visible_to_transfer", True)):
        raise ValueError(f"{source} exposes route policy to transfer")
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
    #  *   1) 要求 RuntimeSession transfer evidence id。
    #  *   2) 要求 receipt 与 receipt-contract 计数一致。
    #  *   3) 校验公开字节摘要，不公开 route selection。
    #  */
    logger.info("开始校验公开 RuntimeSession 标量 evidence 计数...")

    # // 4.1 要求 RuntimeSession transfer evidence id
    evidence_id = snapshot.get("transfer_evidence_id")
    if not isinstance(evidence_id, str) or not evidence_id:
        raise ValueError(f"{source} missing transfer_evidence_id")

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


def _transfer_snapshot_evidence_ids(snapshot: Mapping[str, object]) -> tuple[str, ...]:
    # /*
    #  * ========================================================================
    #  * 步骤5：提取公开 transfer evidence id
    #  * ========================================================================
    #  * 数据源：transfer-facing public snapshot
    #  * 操作：
    #  *   1) 要求新主字段 transfer_evidence_id 存在
    #  *   2) 返回统一 transfer evidence id
    #  */
    logger.info("开始提取公开 transfer evidence id...")

    # // 5.1 空 batch 不携带运行态 evidence
    if str(snapshot.get("transfer_state", "")) == "empty":
        logger.info("公开 transfer evidence id 提取完成, count: %s", 0)
        return ()

    # // 5.2 读取新主字段
    evidence_id = snapshot.get("transfer_evidence_id")
    if not isinstance(evidence_id, str) or not evidence_id:
        raise ValueError("transfer snapshot missing transfer_evidence_id")

    logger.info("公开 transfer evidence id 提取完成, count: %s", 1)
    return (evidence_id,)


def _require_transfer_lifecycle_entrypoint(lifecycle: Mapping[str, object]) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤6：校验 transfer lifecycle entrypoint 记录
    #  * ========================================================================
    #  * 数据源：lifecycle.runtime_entrypoint
    #  * 操作：
    #  *   1) 要求 RuntimeSession 记录 transfer evidence
    #  *   2) 拒绝旧 evidence 镜像重新进入 runtime 内核
    #  */
    logger.info("开始校验 transfer lifecycle entrypoint 记录...")

    # // 6.1 读取 lifecycle 与 entrypoint
    evidence_id = lifecycle.get("evidence_id")
    runtime_entrypoint = lifecycle.get("runtime_entrypoint")
    if evidence_id is None:
        raise ValueError("transfer lifecycle evidence missing evidence_id")
    if not isinstance(runtime_entrypoint, Mapping):
        raise ValueError("transfer lifecycle evidence missing runtime_entrypoint")

    # // 6.2 要求 transfer evidence 记录存在
    if not bool(runtime_entrypoint.get("transfer_evidence_recorded", False)):
        raise ValueError("runtime entrypoint did not record transfer lifecycle evidence")
    record = runtime_entrypoint.get("transfer_evidence_record")
    if not isinstance(record, Mapping):
        raise ValueError("runtime entrypoint missing transfer evidence record")
    if str(record.get("evidence_id")) != str(evidence_id):
        raise ValueError("runtime entrypoint transfer evidence_id mismatch")

    logger.info("transfer lifecycle entrypoint 记录校验完成, evidence_id: %s", evidence_id)


def _normalize_lifecycle_evidence(
    lifecycle_evidence: Mapping[str, object] | Iterable[Mapping[str, object]],
) -> tuple[Mapping[str, object], ...]:
    if isinstance(lifecycle_evidence, Mapping):
        return (lifecycle_evidence,)
    resolved = tuple(lifecycle_evidence)
    for index, item in enumerate(resolved):
        if not isinstance(item, Mapping):
            raise TypeError(
                "transfer lifecycle validation accepts only mapping evidence; "
                f"item {index} is {type(item).__name__}"
            )
    return resolved


def _require_transfer_lifecycle_contract(lifecycle: Mapping[str, object]) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤1：校验 transfer 生命周期边界
    #  * ========================================================================
    #  * 数据源：transfer lifecycle evidence
    #  * 操作：
    #  *   1) 校验 RuntimeSession、TransferIntent、TransferReceipt 来源
    #  *   2) 拒绝 fake evidence 和 route policy 暴露
    #  */
    logger.info("开始校验 transfer 生命周期边界...")

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
                "transfer lifecycle evidence "
                f"{key} must be {expected}, got {observed!r}"
            )

    # // 1.2 拒绝 transfer 可见物理路径策略
    if bool(lifecycle.get("route_policy_visible_to_transfer", True)):
        raise ValueError("transfer lifecycle exposes physical route policy")

    # // 1.3 校验 RuntimeSession entrypoint 合约
    _require_runtime_entrypoint_contract(
        lifecycle.get("runtime_entrypoint"),
        lifecycle=lifecycle,
    )
    _require_transfer_lifecycle_recovery_contract(lifecycle)
    logger.info("transfer 生命周期边界校验完成")


def _require_transfer_lifecycle_recovery_contract(
    lifecycle: Mapping[str, object],
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤8：校验 transfer recovery 摘要
    #  * ========================================================================
    #  * 数据源：transfer lifecycle daemon_recovery 字段
    #  * 操作：
    #  *   1) 允许 RuntimeSession recovery 标量摘要
    #  *   2) 拒绝 queue/ticket/lease/buffer 等 daemon 内部细节暴露给 transfer 层
    #  */
    logger.info("开始校验 transfer recovery 摘要...")

    # // 8.1 空 recovery 直接通过
    recovery = lifecycle.get("daemon_recovery")
    if recovery is None:
        logger.info("transfer recovery 摘要校验完成, count: %s", 0)
        return
    if not isinstance(recovery, list | tuple):
        raise TypeError("transfer lifecycle daemon_recovery must be a sequence")

    # // 8.2 校验每条 recovery 摘要不含 daemon 内部细节
    for item in recovery:
        if not isinstance(item, Mapping):
            raise TypeError("transfer lifecycle daemon_recovery items must be mappings")
        if bool(item.get("route_policy_visible_to_transfer", True)):
            raise ValueError("transfer recovery exposes route policy")
        _require_no_runtime_identity_fields(
            item,
            source="transfer lifecycle daemon_recovery",
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
                "transfer recovery must use RuntimeSession recovery summary "
                "instead of " + ", ".join(leaked)
            )
    logger.info("transfer recovery 摘要校验完成, count: %s", len(recovery))


def _require_transfer_lifecycle_range_contract(
    lifecycle: Mapping[str, object],
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤6：校验 transfer range/binding 摘要
    #  * ========================================================================
    #  * 数据源：transfer lifecycle extra range 与 buffer binding 字段
    #  * 操作：
    #  *   1) 拒绝 range/binding 字段自带 receipt/ticket/decision/topology
    #  *   2) 要求运行态对齐只来自 RuntimeSession transfer evidence record
    #  */
    logger.info("开始校验 transfer range/binding 摘要...")

    # // 6.1 校验 runtime_buffer_binding
    binding = lifecycle.get("runtime_buffer_binding")
    if isinstance(binding, Mapping):
        _require_no_runtime_identity_fields(
            binding,
            source="transfer runtime_buffer_binding",
        )
        if bool(binding.get("route_policy_visible_to_transfer", True)):
            raise ValueError("transfer runtime_buffer_binding exposes route policy")

    # // 6.2 校验 range/binding 集合
    for field_name in (
        "bucket_ranges",
        "bucket_bindings",
        "tensor_bindings",
        "request_binding",
        "runtime_buffer_bindings",
    ):
        value = lifecycle.get(field_name)
        if value is None:
            continue
        if isinstance(value, Mapping):
            items = (value,)
        elif isinstance(value, list | tuple):
            items = value
        else:
            raise TypeError(
                f"transfer lifecycle {field_name} must be a mapping or sequence"
            )
        for item in items:
            if not isinstance(item, Mapping):
                raise TypeError(f"transfer lifecycle {field_name} items must be mappings")
            _require_no_runtime_identity_fields(
                item,
                source=f"transfer lifecycle {field_name}",
            )
            if bool(item.get("route_policy_visible_to_transfer", False)):
                raise ValueError(f"transfer lifecycle {field_name} exposes route policy")
    logger.info("transfer range/binding 摘要校验完成")


def _require_no_runtime_identity_fields(
    value: Mapping[str, object],
    *,
    source: str,
) -> None:
    _require_no_nested_runtime_identity_fields(value, source=source)
    # /*
    #  * ========================================================================
    #  * 步骤7：拒绝裸运行态标识字段
    #  * ========================================================================
    #  * 数据源：transfer lifecycle extra mapping
    #  * 操作：
    #  *   1) 检查 receipt/ticket/decision/topology 标识字段
    #  *   2) 发现裸运行态字段立即拒绝
    #  */
    logger.info("开始检查裸运行态标识字段...")

    # // 7.1 拒绝容易绕过 RuntimeSession record 的运行态字段
    forbidden = {
        "runtime_entrypoint",
        "transfer_evidence_record",
        "receipt_contracts",
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
        "intent_ids",
        "receipt_ids",
        "ticket_ids",
        "decision_ids",
        "topology_snapshot_ids",
    }
    leaked = sorted(key for key in forbidden if key in value)
    if leaked:
        raise ValueError(
            f"{source} must use RuntimeSession transfer evidence instead of "
            + ", ".join(leaked)
        )
    logger.info("裸运行态标识字段检查完成")


def _require_no_nested_runtime_identity_fields(
    value: object,
    *,
    source: str,
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤8：递归拒绝嵌套运行态标识字段
    #  * ========================================================================
    #  * 目标：
    #  *   1) 检查 request binding 和 buffer binding 的嵌套摘要。
    #  *   2) 防止 transfer 层把 receipt/ticket/decision/topology 藏进子对象。
    #  */
    logger.info("开始递归检查嵌套运行态标识字段...")

    # // 8.1 深度优先检查 mapping 与序列
    nested_forbidden = {
        "runtime_entrypoint",
        "transfer_evidence_record",
        "receipt_contracts",
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
        "intent_ids",
        "receipt_ids",
        "ticket_ids",
        "decision_ids",
        "topology_snapshot_ids",
    }
    stack = [value]
    while stack:
        current = stack.pop()
        if isinstance(current, Mapping):
            if bool(current.get("route_policy_visible_to_transfer", False)):
                raise ValueError(f"{source} nested fields expose route policy")
            if bool(current.get("route_policy_visible_to_application", False)):
                raise ValueError(f"{source} nested fields expose route policy")
            leaked = sorted(key for key in nested_forbidden if key in current)
            if leaked:
                raise ValueError(
                    f"{source} nested fields must use RuntimeSession transfer "
                    "evidence instead of " + ", ".join(leaked)
                )
            stack.extend(current.values())
        elif isinstance(current, list | tuple):
            stack.extend(current)
    logger.info("嵌套运行态标识字段递归检查完成")


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
    #  *   3) 确认 transfer intent/receipt 已被 RuntimeSession 记录
    #  */
    logger.info("开始校验 RuntimeSession 入口合约...")

    # // 2.1 校验入口对象和 plan 来源
    if not isinstance(value, Mapping):
        raise ValueError("transfer lifecycle evidence missing runtime_entrypoint")
    expected = {
        "schema": "turbobus.runtime_session_entrypoint.v1",
        "entrypoint": "TurboBusRuntimeSession",
        "plan_source": "daemon_scheduler",
    }
    for key, expected_value in expected.items():
        observed = value.get(key)
        if str(observed) != expected_value:
            raise ValueError(
                "transfer lifecycle runtime_entrypoint "
                f"{key} must be {expected_value}, got {observed!r}"
            )
    if bool(value.get("route_policy_visible_to_application", True)):
        raise ValueError("runtime entrypoint exposes route policy to application")
    if bool(value.get("route_policy_visible_to_transfer", True)):
        raise ValueError("runtime entrypoint exposes route policy to transfer")

    # // 2.2 校验 RuntimeSession 已记录 transfer intent 与 receipt
    if not bool(value.get("intents_recorded", False)):
        raise ValueError("runtime entrypoint did not record transfer intents")
    if not bool(value.get("receipts_recorded", False)):
        raise ValueError("runtime entrypoint did not record transfer receipts")
    if not bool(value.get("transfer_context_recorded", False)):
        raise ValueError("runtime entrypoint did not record transfer construction")
    if not bool(value.get("transfer_evidence_recorded", False)):
        raise ValueError("runtime entrypoint did not record transfer lifecycle evidence")
    _require_runtime_entrypoint_transfer_evidence(value, lifecycle=lifecycle)
    logger.info("RuntimeSession 入口合约校验完成")


def _require_runtime_entrypoint_transfer_evidence(
    value: Mapping[str, object],
    *,
    lifecycle: Mapping[str, object],
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤3：校验 transfer evidence 记录明细
    #  * ========================================================================
    #  * 数据源：runtime_entrypoint.transfer_evidence_record
    #  * 操作：
    #  *   1) 核对 lifecycle evidence_id 已写入 RuntimeSession
    #  *   2) 核对 intent 与 receipt 合约没有脱离 entrypoint record
    #  */
    logger.info("开始校验 transfer evidence 记录明细...")

    # // 3.1 读取 lifecycle 标识与 RuntimeSession 记录
    evidence_id = lifecycle.get("evidence_id")
    if evidence_id is None:
        raise ValueError("transfer lifecycle evidence missing evidence_id")
    record = value.get("transfer_evidence_record")
    if not isinstance(record, Mapping):
        raise ValueError("runtime entrypoint missing transfer evidence record")
    if str(record.get("evidence_id")) != str(evidence_id):
        raise ValueError("runtime entrypoint transfer evidence_id mismatch")

    # // 3.2 核对 RuntimeSession 记录内的 intent 与 receipt 明细
    if not bool(record.get("intents_recorded", False)):
        raise ValueError("runtime entrypoint transfer evidence missing intents")
    if not bool(record.get("receipts_recorded", False)):
        raise ValueError("runtime entrypoint transfer evidence missing receipts")
    expected_intent_ids, expected_receipt_ids = _receipt_contract_identity_sets(
        lifecycle
    )
    recorded_intent_ids = _string_set(record.get("intent_ids"))
    recorded_receipt_ids = _string_set(record.get("receipt_ids"))
    if not expected_intent_ids.issubset(recorded_intent_ids):
        raise ValueError("runtime entrypoint transfer evidence intent_ids mismatch")
    if not expected_receipt_ids.issubset(recorded_receipt_ids):
        raise ValueError("runtime entrypoint transfer evidence receipt_ids mismatch")
    logger.info("transfer evidence 记录明细校验完成, evidence_id: %s", evidence_id)


def _receipt_contract_identity_sets(
    lifecycle: Mapping[str, object],
) -> tuple[set[str], set[str]]:
    # /*
    #  * ========================================================================
    #  * 步骤4：提取 receipt contract 标识集合
    #  * ========================================================================
    #  * 数据源：transfer lifecycle receipt_contracts
    #  * 操作：
    #  *   1) 读取每个 receipt contract 的 intent_id 和 receipt_id
    #  *   2) 返回用于 RuntimeSession transfer evidence 核对的集合
    #  */
    logger.info("开始提取 receipt contract 标识集合...")

    # // 4.1 校验 receipt_contracts 结构
    contracts = lifecycle.get("receipt_contracts")
    if not isinstance(contracts, list | tuple):
        raise ValueError("transfer lifecycle evidence missing receipt_contracts")

    # // 4.2 收集 intent_id 与 receipt_id
    intent_ids: set[str] = set()
    receipt_ids: set[str] = set()
    for index, contract in enumerate(contracts):
        if not isinstance(contract, Mapping):
            raise TypeError(
                "transfer lifecycle receipt_contracts must be mappings; "
                f"item {index} is {type(contract).__name__}"
            )
        intent_id = contract.get("intent_id")
        receipt_id = contract.get("receipt_id")
        if intent_id is None or receipt_id is None:
            raise ValueError(
                "transfer lifecycle receipt contract missing identity fields"
            )
        intent_ids.add(str(intent_id))
        receipt_ids.add(str(receipt_id))

    # // 4.3 拒绝空 receipt contract
    if not receipt_ids:
        raise ValueError("transfer lifecycle evidence contains no receipt contracts")
    logger.info("receipt contract 标识集合提取完成, receipts: %s", len(receipt_ids))
    return intent_ids, receipt_ids


def _string_set(value: object) -> set[str]:
    # /*
    #  * ========================================================================
    #  * 步骤5：归一化字符串集合
    #  * ========================================================================
    #  * 数据源：RuntimeSession transfer evidence 记录字段
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
        raise ValueError("transfer lifecycle evidence missing receipt_contracts")
    views: list[dict[str, object]] = []
    for index, contract in enumerate(contracts):
        if not isinstance(contract, Mapping):
            raise TypeError(
                "transfer lifecycle receipt_contracts must be mappings; "
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
            "transfer lifecycle receipt contract missing fields: "
            + ", ".join(missing)
        )
    completion_source = str(contract.get("completion_source", "")).lower()
    if completion_source not in {"worker", "backend"}:
        raise ValueError(
            "transfer lifecycle receipt contract missing worker/backend completion source"
        )
    if not bool(contract.get("verified", False)):
        raise ValueError("transfer lifecycle receipt contract missing verification")
    verified_bytes = int(contract.get("verified_bytes", 0) or 0)
    bytes_total = int(contract.get("bytes_total", 0) or 0)
    if verified_bytes != bytes_total:
        raise ValueError("transfer lifecycle receipt contract verified bytes mismatch")
    completion_contract = contract.get("completion_contract")
    if not isinstance(completion_contract, Mapping):
        raise ValueError("transfer lifecycle receipt contract missing completion contract")
    return {
        "source": "transfer_lifecycle_receipt_contract",
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
    "validate_transfer_batch_snapshot",
    "validate_transfer_lifecycle_evidence",
    "validate_transfer_stats_collection",
    "validate_transfer_stats_snapshot",
    "validate_runtime_receipts",
]

