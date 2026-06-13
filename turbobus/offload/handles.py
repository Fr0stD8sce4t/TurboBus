from __future__ import annotations

from dataclasses import dataclass, field
import logging

from ..runtime_session import TurboBusRuntimeSession
from ..runtime.validation import validate_runtime_receipt
from ..schema import TransferIntent, TransferReceipt, TransferStatusState
from .context import AdapterTransferContext, require_runtime_session_open
from .lifecycle import _runtime_entrypoint_contract
from .stats import TransferStats, transfer_stats_from_receipt

logger = logging.getLogger(__name__)


@dataclass
class ReceiptTransferHandle:
    client: TurboBusRuntimeSession
    intent: TransferIntent
    receipt: TransferReceipt
    transfer_context: AdapterTransferContext | None = None
    wait_timeout_seconds: float | None = None
    evidence_id: str | None = None
    wait_calls: int = 0
    _waited: bool = field(default=False, init=False, repr=False)
    runtime_entrypoint: dict[str, object] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        # /*
        #  * ========================================================================
        #  * 步骤1：绑定提交阶段句柄证据
        #  * ========================================================================
        #  * 数据源：ReceiptTransferHandle 初始 TransferReceipt
        #  * 操作：
        #  *   1) 校验 handle receipt 与 TransferIntent、AdapterTransferContext 对齐
        #  *   2) 写入 RuntimeSession entrypoint record
        #  */
        logger.info("开始绑定提交阶段句柄证据...")

        # // 1.1 校验初始 receipt 来源与 adapter context
        validate_adapter_receipt(
            self.receipt,
            self.intent,
            transfer_context=self.transfer_context,
        )

        # // 1.2 生成稳定 evidence_id 并记录 RuntimeSession 入口合约
        if self.evidence_id is None:
            self.evidence_id = _adapter_handle_evidence_id(self.intent)
        self._record_runtime_entrypoint_binding(phase="submit")
        logger.info("提交阶段句柄证据绑定完成, evidence_id: %s", self.evidence_id)

    @property
    def stats(self) -> TransferStats:
        return transfer_stats_from_receipt(self.receipt)

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
            return self.receipt

        # // 2.2 从 RuntimeSession 等待真实 TransferReceipt
        require_runtime_session_open(self.client)
        self.receipt = self.client.wait_transfer_receipt(
            self.intent.intent_id,
            timeout_seconds=self.wait_timeout_seconds,
        )
        if not isinstance(self.receipt, TransferReceipt):
            raise TypeError("wait_transfer_receipt must return a TransferReceipt")

        # // 2.3 校验 receipt 并刷新 RuntimeSession 入口合约
        validate_adapter_receipt(
            self.receipt,
            self.intent,
            transfer_context=self.transfer_context,
        )
        self._record_runtime_entrypoint_binding(phase="wait")
        self.wait_calls += 1
        self._waited = True
        state = TransferStatusState(self.receipt.state)
        if state in {TransferStatusState.FAILED, TransferStatusState.CANCELED}:
            raise RuntimeError(self.receipt.error or f"transfer {state.value}")
        logger.info("等待阶段句柄证据绑定完成, evidence_id: %s", self.evidence_id)
        return self.receipt

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
        operation = _adapter_handle_operation(self.intent, phase=phase)

        # // 3.2 记录并缓存 RuntimeSession entrypoint contract
        self.runtime_entrypoint = _runtime_entrypoint_contract(
            self.client,
            receipts=(self.receipt,),
            evidence_id=str(self.evidence_id),
            operation=operation,
        )
        logger.info("RuntimeSession 入口绑定写入完成, evidence_id: %s", self.evidence_id)


def validate_adapter_receipt(
    receipt: TransferReceipt,
    intent: TransferIntent,
    *,
    transfer_context: AdapterTransferContext | None = None,
) -> None:
    if receipt.intent_id != intent.intent_id:
        raise ValueError("receipt intent_id does not match transfer intent")
    if receipt.job_id != intent.job_id:
        raise ValueError("receipt job_id does not match transfer intent")
    if receipt.session_id != intent.session_id:
        raise ValueError("receipt session_id does not match transfer intent")
    if transfer_context is not None:
        if receipt.job_id != transfer_context.job_id:
            raise ValueError("receipt job_id does not match adapter context")
        if receipt.session_id != transfer_context.session_id:
            raise ValueError("receipt session_id does not match adapter context")
    validate_runtime_receipt(
        receipt,
        intent_id=intent.intent_id,
        job_id=intent.job_id,
        session_id=intent.session_id,
    )


def _adapter_handle_evidence_id(intent: TransferIntent) -> str:
    return f"adapter-handle-{intent.intent_id}"


def _adapter_handle_operation(intent: TransferIntent, *, phase: str) -> str:
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    operation = str(metadata.get("operation") or intent.direction)
    return f"adapter_handle_{operation}_{phase}"


__all__ = [
    "ReceiptTransferHandle",
    "validate_adapter_receipt",
]
