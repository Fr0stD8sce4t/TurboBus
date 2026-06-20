from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Callable, Iterable

from .core import StateOffloadCore

logger = logging.getLogger(__name__)


@dataclass
class StateOffloadTransaction:
    core: StateOffloadCore
    names: tuple[str, ...]
    restore_before_step: Callable[[Iterable[str]], None] | None = None
    capture_after_step: Callable[[Iterable[str]], None] | None = None
    rollback_restore: Callable[[Iterable[str]], None] | None = None
    _prefetch_transfer: object | None = field(default=None, init=False)
    _commit_batch: object | None = field(default=None, init=False)
    _state: str = field(default="new", init=False)

    @property
    def state(self) -> str:
        return self._state

    @property
    def prefetch_transfer(self):
        return self._prefetch_transfer

    @property
    def commit_batch(self):
        return self._commit_batch

    def begin(self) -> "StateOffloadTransaction":
        # /*
        #  * ========================================================================
        #  * 步骤1：开始 state transaction
        #  * ========================================================================
        #  * 数据源：StateOffloadCore 与 state name 集合
        #  * 操作：
        #  *   1) 校验事务只进入一次
        #  *   2) 标记为 active
        #  */
        logger.info("开始 state transaction...")

        # // 1.1 校验事务状态
        if self._state != "new":
            raise RuntimeError("state offload transaction has already started")

        # // 1.2 标记 active
        self._state = "active"
        logger.info("state transaction 开始完成, count: %s", len(self.names))
        return self

    def prefetch_before_step(self):
        # /*
        #  * ========================================================================
        #  * 步骤2：step 前预取 state
        #  * ========================================================================
        #  * 数据源：StateOffloadCore registered states
        #  * 操作：
        #  *   1) 提交 H2D prefetch
        #  *   2) 等待后恢复应用侧 state tensor
        #  */
        logger.info("开始 step 前预取 state...")

        # // 2.1 校验 active 状态
        if self._state == "prefetched":
            raise RuntimeError("state offload transaction has already prefetched")
        if self._state != "active":
            raise RuntimeError("state offload transaction is not active")

        # // 2.2 提交并等待 prefetch
        self._prefetch_transfer = self.core.submit_prefetch_states(
            self.names,
            operation="transaction_prefetch_before_step",
        )
        self._prefetch_transfer.wait()
        if self.restore_before_step is not None:
            self.restore_before_step(self.names)
        self._state = "prefetched"
        logger.info("step 前 state 预取完成, count: %s", len(self.names))
        return self._prefetch_transfer

    def commit_after_step(self):
        # /*
        #  * ========================================================================
        #  * 步骤3：step 后提交 state
        #  * ========================================================================
        #  * 数据源：optimizer.step 后 state tensor
        #  * 操作：
        #  *   1) 捕获新的 CPU mirror
        #  *   2) 提交 D2H offload 并完成事务
        #  */
        logger.info("开始 step 后提交 state...")

        # // 3.1 校验 active 状态
        if self._state == "committed":
            raise RuntimeError("state offload transaction has already committed")
        if self._state == "rolled_back":
            raise RuntimeError("state offload transaction has rolled back")
        if self._state != "prefetched":
            raise RuntimeError("state offload transaction must prefetch before commit")

        # // 3.2 捕获新 state 并提交 offload
        if self.capture_after_step is not None:
            self.capture_after_step(self.names)
        self._commit_batch = self.core.submit_offload_states(
            self.names,
            operation="transaction_commit_after_step",
        )
        self._commit_batch.wait()
        self._state = "committed"
        logger.info("step 后 state 提交完成, count: %s", len(self.names))
        return self._commit_batch

    def rollback_on_error(self) -> None:
        # /*
        #  * ========================================================================
        #  * 步骤4：异常回滚 state transaction
        #  * ========================================================================
        #  * 数据源：step 前 CPU mirror
        #  * 操作：
        #  *   1) 恢复 step 前 state tensor
        #  *   2) 标记事务 rolled_back
        #  */
        logger.info("开始回滚 state transaction...")

        # // 4.1 仅 active 事务需要回滚
        if self._state == "committed":
            raise RuntimeError("state offload transaction cannot roll back after commit")
        if self._state == "rolled_back":
            raise RuntimeError("state offload transaction has already rolled back")
        if self._state == "new":
            raise RuntimeError("state offload transaction must begin before rollback")
        if self._state not in {"active", "prefetched"}:
            raise RuntimeError("state offload transaction cannot roll back")

        # // 4.2 恢复 step 前 snapshot
        if self.rollback_restore is not None:
            self.rollback_restore(self.names)
        self._state = "rolled_back"
        logger.info("state transaction 回滚完成, count: %s", len(self.names))


__all__ = ["StateOffloadTransaction"]
