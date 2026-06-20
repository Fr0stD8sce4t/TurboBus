from __future__ import annotations

import logging

from ..runtime_session import TurboBusRuntimeSession
from ..runtime.evidence import validate_transfer_stats_snapshot
from ..runtime.validation import validate_runtime_receipt
from ..schema import TransferIntent, TransferReceipt, TransferStatusState
from .context import TransferContext, require_runtime_session_open
from .lifecycle import _runtime_entrypoint_contract, runtime_session_receipt_trace_from_receipts
from .stats import TransferStats, TransferStatsSnapshot, transfer_stats_from_receipt

logger = logging.getLogger(__name__)


class RuntimeSessionTransferHandle:
    def __init__(self, handle: "_ReceiptTransferHandle") -> None:
        self._handle = handle

    @property
    def evidence_id(self) -> str | None:
        return self._handle.evidence_id

    @property
    def wait_calls(self) -> int:
        return int(getattr(self._handle, "wait_calls", 0) or 0)

    @property
    def stats(self) -> TransferStatsSnapshot:
        return self._handle.stats

    def wait(self) -> None:
        # /*
        #  * ========================================================================
        #  * 步骤1：等待 RuntimeSession 绑定 transfer handle
        #  * ========================================================================
        #  * 数据源：_ReceiptTransferHandle 内部 receipt-bearing handle
        #  * 操作：
        #  *   1) 委托内部 handle 等待真实 TransferReceipt
        #  *   2) 不把 TransferReceipt 对象作为公开返回值暴露给 adapter
        #  */
        logger.info("开始等待 RuntimeSession 绑定 transfer handle...")

        # // 1.1 等待内部 handle 并保留 receipt 在 RuntimeSession evidence 边界内
        self._handle.wait()
        logger.info(
            "RuntimeSession 绑定 transfer handle 等待完成, evidence_id: %s",
            self.evidence_id,
        )

    def as_dict(self) -> dict[str, object]:
        # /*
        #  * ========================================================================
        #  * 步骤2：导出公开 handle 摘要
        #  * ========================================================================
        #  * 数据源：RuntimeSession-bound TransferStatsSnapshot
        #  * 操作：
        #  *   1) 读取已校验的 stats snapshot
        #  *   2) 只返回 adapter evidence id、receipt 状态和字节摘要
        #  */
        logger.info("开始导出公开 handle 摘要...")

        # // 2.1 复制 RuntimeSession-bound stats 快照
        snapshot = self.stats.as_dict()
        logger.info(
            "公开 handle 摘要导出完成, evidence_id: %s",
            snapshot.get("transfer_evidence_id"),
        )
        return snapshot


class _ReceiptTransferHandle:
    def __init__(
        self,
        *,
        client: TurboBusRuntimeSession,
        intent: TransferIntent,
        receipt: TransferReceipt,
        transfer_context: TransferContext | None = None,
        wait_timeout_seconds: float | None = None,
        evidence_id: str | None = None,
    ) -> None:
        self._client = client
        self._intent = intent
        self._receipt = receipt
        self._transfer_context = transfer_context
        self._wait_timeout_seconds = wait_timeout_seconds
        self.evidence_id = evidence_id
        self.wait_calls = 0
        self._waited = False
        self._runtime_entrypoint: dict[str, object] = {}
        self._public_handle: RuntimeSessionTransferHandle | None = None
        # /*
        #  * ========================================================================
        #  * 步骤1：绑定提交阶段句柄证据
        #  * ========================================================================
        #  * 数据源：ReceiptTransferHandle 初始 TransferReceipt
        #  * 操作：
        #  *   1) 校验 handle receipt 与 TransferIntent、TransferContext 对齐
        #  *   2) 写入 RuntimeSession entrypoint record
        #  */
        logger.info("开始绑定提交阶段句柄证据...")

        # // 1.1 校验初始 receipt 来源与 adapter context
        validate_transfer_receipt(
            self._receipt,
            self._intent,
            transfer_context=self._transfer_context,
        )

        # // 1.2 生成稳定 evidence_id 并记录 RuntimeSession 入口合约
        if self.evidence_id is None:
            self.evidence_id = _transfer_handle_evidence_id(self._intent)
        self._record_runtime_entrypoint_binding(phase="submit")
        logger.info("提交阶段句柄证据绑定完成, evidence_id: %s", self.evidence_id)

    def public_handle(self) -> RuntimeSessionTransferHandle:
        if self._public_handle is None:
            self._public_handle = RuntimeSessionTransferHandle(self)
        return self._public_handle

    @property
    def stats(self) -> TransferStatsSnapshot:
        # /*
        #  * ========================================================================
        #  * 步骤4：读取 RuntimeSession 绑定 handle stats
        #  * ========================================================================
        #  * 数据源：ReceiptTransferHandle runtime_entrypoint 与 TransferReceipt
        #  * 操作：
        #  *   1) 不公开裸 direct/relay stats
        #  *   2) 返回带 RuntimeSession adapter evidence record 的统计快照
        #  */
        logger.info("开始读取 RuntimeSession 绑定 handle stats...")

        # // 4.1 生成 RuntimeSession-bound receipt trace
        trace = runtime_session_receipt_trace_from_receipts(
            (self._receipt,),
            self._client,
            evidence_id=f"{self.evidence_id}-stats",
            operation="transfer_handle_stats",
        )
        self._runtime_entrypoint = dict(trace["runtime_entrypoint"])

        # // 4.2 生成并校验 evidence-bound stats 快照
        raw_stats = self._raw_stats
        transfer_record = self._runtime_entrypoint.get("transfer_evidence_record")
        if not isinstance(transfer_record, dict):
            raise ValueError("handle stats missing RuntimeSession transfer evidence")
        transfer_evidence_id = transfer_record.get("evidence_id")
        if transfer_evidence_id is None:
            raise ValueError("handle stats missing transfer evidence_id")
        receipt_contracts = trace.get("receipt_contracts")
        if not isinstance(receipt_contracts, list | tuple):
            raise ValueError("handle stats missing receipt contracts")
        snapshot = {
            "transfer_state": "runtime_session_bound",
            "transfer_evidence_id": str(transfer_evidence_id),
            "bytes": int(raw_stats.bytes),
            "direct_chunks": int(raw_stats.direct_chunks),
            "relay_chunks": int(raw_stats.relay_chunks),
            "receipt_count": int(trace["receipt_count"]),
            "receipt_contract_count": len(receipt_contracts),
            "receipt_states": str(trace["receipt_states"]),
            "direct_bytes": int(trace["direct_bytes"]),
            "relay_bytes": int(trace["relay_bytes"]),
            "route_policy_visible_to_transfer": False,
        }
        validate_transfer_stats_snapshot(snapshot)
        logger.info(
            "RuntimeSession 绑定 handle stats 读取完成, receipt_id: %s",
            self._receipt.receipt_id,
        )
        return TransferStatsSnapshot(snapshot)

    @property
    def _raw_stats(self) -> TransferStats:
        return transfer_stats_from_receipt(self._receipt)

    def wait(self) -> TransferReceipt:
        # /*
        #  * ========================================================================
        #  * 步骤2：绑定等待阶段句柄证据
        #  * ========================================================================
        #  * 数据源：RuntimeSession wait_transfer_receipt 返回的 TransferReceipt
        #  * 操作：
        #  *   1) 通过 RuntimeSession 等待 daemon-issued receipt
        #  *   2) 校验并刷新 RuntimeSession entrypoint record
        #  */
        logger.info("开始绑定等待阶段句柄证据...")

        # // 2.1 已等待过则返回已记录 receipt
        if self._waited:
            logger.info("等待阶段句柄证据已绑定, evidence_id: %s", self.evidence_id)
            return self._receipt

        # // 2.2 从 RuntimeSession 等待真实 TransferReceipt
        require_runtime_session_open(self._client)
        self._receipt = self._client.wait_transfer_receipt(
            self._intent.intent_id,
            timeout_seconds=self._wait_timeout_seconds,
        )
        if not isinstance(self._receipt, TransferReceipt):
            raise TypeError("wait_transfer_receipt must return a TransferReceipt")

        # // 2.3 校验 receipt 并刷新 RuntimeSession 入口合约
        validate_transfer_receipt(
            self._receipt,
            self._intent,
            transfer_context=self._transfer_context,
        )
        self._record_runtime_entrypoint_binding(phase="wait")
        self.wait_calls += 1
        self._waited = True
        state = TransferStatusState(self._receipt.state)
        if state in {TransferStatusState.FAILED, TransferStatusState.CANCELED}:
            raise RuntimeError(self._receipt.error or f"transfer {state.value}")
        logger.info("等待阶段句柄证据绑定完成, evidence_id: %s", self.evidence_id)
        return self._receipt

    def _record_runtime_entrypoint_binding(self, *, phase: str) -> None:
        # /*
        #  * ========================================================================
        #  * 步骤3：写入 RuntimeSession 入口绑定
        #  * ========================================================================
        #  * 目标对象：TurboBusRuntimeSession entrypoint record
        #  * 操作：
        #  *   1) 从 receipt 构造 RuntimeSession entrypoint contract
        #  *   2) 保存到 handle，供 adapter 消费而不暴露路径策略
        #  */
        logger.info("开始写入 RuntimeSession 入口绑定...")

        # // 3.1 构造 adapter handle 操作名
        operation = _transfer_handle_operation(self._intent, phase=phase)

        # // 3.2 记录并缓存 RuntimeSession entrypoint contract
        self._runtime_entrypoint = _runtime_entrypoint_contract(
            self._client,
            receipts=(self._receipt,),
            evidence_id=str(self.evidence_id),
            operation=operation,
        )
        logger.info("RuntimeSession 入口绑定写入完成, evidence_id: %s", self.evidence_id)


def validate_transfer_receipt(
    receipt: TransferReceipt,
    intent: TransferIntent,
    *,
    transfer_context: TransferContext | None = None,
) -> None:
    if receipt.intent_id != intent.intent_id:
        raise ValueError("receipt intent_id does not match transfer intent")
    if receipt.job_id != intent.job_id:
        raise ValueError("receipt job_id does not match transfer intent")
    if receipt.session_id != intent.session_id:
        raise ValueError("receipt session_id does not match transfer intent")
    if transfer_context is not None:
        if receipt.job_id != transfer_context.job_id:
            raise ValueError("receipt job_id does not match transfer context")
        if receipt.session_id != transfer_context.session_id:
            raise ValueError("receipt session_id does not match transfer context")
    validate_runtime_receipt(
        receipt,
        intent_id=intent.intent_id,
        job_id=intent.job_id,
        session_id=intent.session_id,
    )


def _transfer_handle_evidence_id(intent: TransferIntent) -> str:
    return f"transfer-handle-{intent.intent_id}"


def _transfer_handle_operation(intent: TransferIntent, *, phase: str) -> str:
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    operation = str(metadata.get("operation") or intent.direction)
    return f"transfer_handle_{operation}_{phase}"


__all__ = [
    "RuntimeSessionTransferHandle",
    "validate_transfer_receipt",
]

