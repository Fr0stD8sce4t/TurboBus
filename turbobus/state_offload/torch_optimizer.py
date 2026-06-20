from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Callable, Iterable, Mapping

from ..offload.store import OffloadBatch
from .core import StateDescriptor, StateOffloadCore
from .transaction import StateOffloadTransaction

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TorchOptimizerStateBucket:
    name: str
    param_index: int
    state_key: str
    tensor: object
    byte_count: int
    offset: int


class TorchOptimizerStateBatch:
    def __init__(
        self,
        batch: OffloadBatch,
        *,
        after_wait: Callable[[], None] | None = None,
    ) -> None:
        self._batch = batch
        self._after_wait = after_wait
        self._waited = False

    def __iter__(self):
        return iter(self._batch)

    def __len__(self) -> int:
        return len(self._batch)

    def __getitem__(self, index):
        return self._batch[index]

    @property
    def operation(self) -> str:
        return self._batch.operation

    @property
    def names(self) -> tuple[str, ...]:
        return self._batch.names

    @property
    def handles(self):
        return self._batch.handles

    @property
    def receipt_handles(self) -> tuple[object, ...]:
        return self._batch.receipt_handles

    @property
    def wait_calls(self) -> int:
        return self._batch.wait_calls

    def wait(self) -> None:
        self._batch.wait()
        if not self._waited and self._after_wait is not None:
            self._after_wait()
        self._waited = True

    def transfer_stats(self):
        return self._batch.transfer_stats()

    def block_infos(self):
        return self._batch.block_infos()

    def as_dict(self) -> dict[str, object]:
        return self._batch.as_dict()


class TorchOptimizerStateRegistry:
    def __init__(self, optimizer, *, name_prefix: str = "optimizer_state") -> None:
        self.optimizer = optimizer
        self.name_prefix = _normalize_name_part(name_prefix)
        self._buckets: tuple[TorchOptimizerStateBucket, ...] = ()
        self._bucket_by_name: dict[str, TorchOptimizerStateBucket] = {}
        self._param_indices: dict[int, int] = {}

    @property
    def buckets(self) -> tuple[TorchOptimizerStateBucket, ...]:
        return self._buckets

    @property
    def param_indices(self) -> dict[int, int]:
        return dict(self._param_indices)

    def names(self) -> list[str]:
        return [bucket.name for bucket in self._buckets]

    def rebuild(self) -> tuple[TorchOptimizerStateBucket, ...]:
        # /*
        #  * ========================================================================
        #  * 步骤1：重建 optimizer state registry
        #  * ========================================================================
        #  * 数据源：optimizer.param_groups 与 optimizer.state
        #  * 操作：
        #  *   1) 按 param_groups 建立稳定 param_index
        #  *   2) 只把 tensor state 转成 state bucket
        #  */
        logger.info("开始重建 optimizer state registry...")

        # 1.1 扫描 param_groups，生成稳定 param index
        torch = _require_torch()
        self._param_indices = _param_indices(self.optimizer)

        # 1.2 扫描 optimizer.state 中的 tensor state
        buckets: list[TorchOptimizerStateBucket] = []
        offset = 0
        for param, state in self.optimizer.state.items():
            if not isinstance(state, Mapping):
                continue
            param_index = self._param_indices.get(id(param))
            if param_index is None:
                param_index = len(self._param_indices)
                self._param_indices[id(param)] = param_index
            for state_key, value in state.items():
                if not isinstance(value, torch.Tensor):
                    continue
                byte_count = _tensor_nbytes(value)
                name = self.bucket_name(param_index, state_key)
                buckets.append(
                    TorchOptimizerStateBucket(
                        name=name,
                        param_index=param_index,
                        state_key=str(state_key),
                        tensor=value,
                        byte_count=byte_count,
                        offset=offset,
                    )
                )
                offset += byte_count

        # 1.3 刷新 name 索引
        self._buckets = tuple(buckets)
        self._bucket_by_name = {bucket.name: bucket for bucket in buckets}
        logger.info("optimizer state registry 重建完成, count: %s", len(buckets))
        return self._buckets

    def descriptors(self) -> tuple[StateDescriptor, ...]:
        if not self._buckets:
            self.rebuild()
        return tuple(
            StateDescriptor(
                name=bucket.name,
                state_id=bucket.name,
                cpu_tensor=bucket.tensor.detach().cpu().clone(),
                gpu_tensor=bucket.tensor,
                cpu_slot=f"cpu:{bucket.param_index}:{bucket.state_key}",
                gpu_slot=f"optimizer:{bucket.param_index}:{bucket.state_key}",
                cpu_offset=bucket.offset,
                gpu_offset=bucket.offset,
                byte_count=bucket.byte_count,
            )
            for bucket in self._buckets
        )

    def refresh(self) -> tuple[TorchOptimizerStateBucket, ...]:
        return self.rebuild()

    def select(self, names: Iterable[str] | None = None) -> list[str]:
        if not self._buckets:
            self.rebuild()
        if names is None:
            return self.names()
        selected = [str(name) for name in names]
        missing = [name for name in selected if name not in self._bucket_by_name]
        if missing:
            raise KeyError(f"unknown optimizer state bucket: {missing[0]}")
        return selected

    def bucket(self, name: str) -> TorchOptimizerStateBucket:
        try:
            return self._bucket_by_name[str(name)]
        except KeyError as exc:
            raise KeyError(f"unknown optimizer state bucket: {name}") from exc

    def bucket_name(self, param_index: int, state_key: object) -> str:
        key = _normalize_name_part(str(state_key))
        return f"{self.name_prefix}/param_{int(param_index)}/{key}"


TorchOptimizerStateIndex = TorchOptimizerStateRegistry


class TorchOptimizerStateMirror:
    def __init__(self, registry: TorchOptimizerStateRegistry) -> None:
        self.registry = registry
        self.index = registry
        self._cpu_mirrors: dict[str, object] = {}
        self._transaction_mirrors: dict[str, object] = {}

    def initialize(self, bucket: TorchOptimizerStateBucket):
        mirror = bucket.tensor.detach().cpu().clone()
        self._cpu_mirrors[bucket.name] = mirror
        return mirror

    def capture(self, names: Iterable[str]) -> None:
        torch = _require_torch()
        with torch.no_grad():
            for name in names:
                bucket = self.registry.bucket(str(name))
                source = bucket.tensor.detach().cpu()
                mirror = self._cpu_mirrors.get(bucket.name)
                if mirror is None or tuple(mirror.shape) != tuple(source.shape):
                    self._cpu_mirrors[bucket.name] = source.clone()
                    continue
                mirror.copy_(source)

    def restore(self, names: Iterable[str]) -> None:
        self._restore_from(self._cpu_mirrors, names)

    def begin_transaction(self, names: Iterable[str]) -> None:
        # /*
        #  * ========================================================================
        #  * 步骤2：捕获事务前 snapshot
        #  * ========================================================================
        #  * 数据源：当前 CPU mirror
        #  * 操作：
        #  *   1) 复制 step 前 mirror
        #  *   2) 供 rollback 使用
        #  */
        logger.info("开始捕获 optimizer state 事务前 snapshot...")

        # 2.1 复制当前 CPU mirror
        self._transaction_mirrors = {}
        for name in names:
            mirror = self._cpu_mirrors.get(str(name))
            if mirror is None:
                continue
            self._transaction_mirrors[str(name)] = mirror.clone()
        logger.info(
            "optimizer state 事务前 snapshot 捕获完成, count: %s",
            len(self._transaction_mirrors),
        )

    def rollback(self, names: Iterable[str]) -> None:
        if self._transaction_mirrors:
            selected = set(str(item) for item in names)
            self._cpu_mirrors = {
                **self._cpu_mirrors,
                **{
                    name: mirror.clone()
                    for name, mirror in self._transaction_mirrors.items()
                    if name in selected
                },
            }
        self._restore_from(self._cpu_mirrors, names)

    def _restore_from(self, mirrors: Mapping[str, object], names: Iterable[str]) -> None:
        torch = _require_torch()
        with torch.no_grad():
            for name in names:
                bucket = self.registry.bucket(str(name))
                mirror = mirrors.get(bucket.name)
                if mirror is None:
                    continue
                restored = mirror.to(
                    device=bucket.tensor.device,
                    dtype=bucket.tensor.dtype,
                )
                bucket.tensor.copy_(restored)


class TorchOptimizerTransactionAdapter:
    def __init__(
        self,
        optimizer,
        core: StateOffloadCore,
        *,
        name_prefix: str = "optimizer_state",
    ) -> None:
        if not isinstance(core, StateOffloadCore):
            raise TypeError("core must be a StateOffloadCore")
        self.optimizer = optimizer
        self.core = core
        self.state_registry = TorchOptimizerStateRegistry(
            optimizer,
            name_prefix=name_prefix,
        )
        self.state_index = self.state_registry
        self.state_mirror = TorchOptimizerStateMirror(self.state_registry)
        self._active_transaction: StateOffloadTransaction | None = None

    @property
    def state_buckets(self) -> tuple[TorchOptimizerStateBucket, ...]:
        return self.state_registry.buckets

    def names(self) -> list[str]:
        return self.state_registry.names()

    def register_optimizer_state(self) -> list[object]:
        # /*
        #  * ========================================================================
        #  * 步骤3：注册真实 optimizer tensor state
        #  * ========================================================================
        #  * 数据源：TorchOptimizerStateRegistry
        #  * 操作：
        #  *   1) 重建 registry
        #  *   2) 为新增 tensor state 建 offload state
        #  */
        logger.info("开始注册真实 optimizer tensor state...")

        # 3.1 重建 registry 并跳过已注册 state
        buckets = self.state_registry.rebuild()
        known_names = set(self.core.names())
        descriptors = []
        for bucket in buckets:
            if bucket.name in known_names:
                continue
            mirror = self.state_mirror.initialize(bucket)
            descriptors.append(
                StateDescriptor(
                    name=bucket.name,
                    state_id=bucket.name,
                    cpu_tensor=mirror,
                    gpu_tensor=bucket.tensor,
                    cpu_slot=f"cpu:{bucket.param_index}:{bucket.state_key}",
                    gpu_slot=f"optimizer:{bucket.param_index}:{bucket.state_key}",
                    cpu_offset=bucket.offset,
                    gpu_offset=bucket.offset,
                    byte_count=bucket.byte_count,
                )
            )
            known_names.add(bucket.name)

        # 3.2 绑定 registry 并通过 core 注册 descriptor
        self.core.state_registry = self.state_registry
        registered = self.core.register_states(descriptors)
        logger.info("真实 optimizer tensor state 注册完成, count: %s", len(registered))
        return registered

    def prefetch_state(
        self,
        names: Iterable[str] | None = None,
    ) -> TorchOptimizerStateBatch:
        selected = self.state_registry.select(names)
        batch = self.core.submit_prefetch_states(
            selected,
            operation="prefetch_optimizer_state",
        )
        return TorchOptimizerStateBatch(
            batch,
            after_wait=lambda: self.state_mirror.restore(selected),
        )

    def offload_state(
        self,
        names: Iterable[str] | None = None,
    ) -> TorchOptimizerStateBatch:
        selected = self.state_registry.select(names)
        batch = self.core.submit_offload_states(
            selected,
            operation="offload_optimizer_state",
        )
        return TorchOptimizerStateBatch(
            batch,
            after_wait=lambda: self.state_mirror.capture(selected),
        )

    def begin_state_transaction(
        self,
        names: Iterable[str] | None = None,
    ) -> StateOffloadTransaction:
        # /*
        #  * ========================================================================
        #  * 步骤4：创建 optimizer state transaction
        #  * ========================================================================
        #  * 数据源：已注册 optimizer state bucket
        #  * 操作：
        #  *   1) 确保 registry 与 core 同步
        #  *   2) 捕获 rollback snapshot
        #  */
        logger.info("开始创建 optimizer state transaction...")

        # 4.1 注册新出现的 optimizer state
        self.register_optimizer_state()
        selected = tuple(self.state_registry.select(names))

        # 4.2 捕获事务前 mirror 并创建 transaction
        self.state_mirror.begin_transaction(selected)
        transaction = StateOffloadTransaction(
            self.core,
            selected,
            restore_before_step=self.state_mirror.restore,
            capture_after_step=self.state_mirror.capture,
            rollback_restore=self.state_mirror.rollback,
        ).begin()
        self._active_transaction = transaction
        logger.info("optimizer state transaction 创建完成, count: %s", len(selected))
        return transaction

    def prefetch_before_step(self):
        if self._active_transaction is None:
            self.begin_state_transaction()
        return self._active_transaction.prefetch_before_step()

    def commit_after_step(self):
        if self._active_transaction is None:
            raise RuntimeError("optimizer state transaction has not started")
        batch = self._active_transaction.commit_after_step()
        self._active_transaction = None
        return batch

    def rollback_on_error(self) -> None:
        if self._active_transaction is None:
            return
        self._active_transaction.rollback_on_error()
        self._active_transaction = None

    def step(self, closure=None):
        # /*
        #  * ========================================================================
        #  * 步骤5：执行事务化 optimizer.step
        #  * ========================================================================
        #  * 数据源：真实 torch.optim.Optimizer
        #  * 操作：
        #  *   1) step 前恢复 state
        #  *   2) 执行 optimizer.step
        #  *   3) step 后提交新 state，异常时回滚
        #  */
        logger.info("开始执行事务化 optimizer.step...")

        # 5.1 打开事务并恢复 state
        transaction = self.begin_state_transaction()
        try:
            transaction.prefetch_before_step()

            # 5.2 执行真实 optimizer.step
            if closure is None:
                result = self.optimizer.step()
            else:
                result = self.optimizer.step(closure)

            # 5.3 提交 step 后 state
            transaction.commit_after_step()
            self._active_transaction = None
            logger.info("事务化 optimizer.step 执行完成")
            return result
        except Exception:
            # 5.4 异常时回滚到 step 前 snapshot
            transaction.rollback_on_error()
            self._active_transaction = None
            logger.info("optimizer.step transaction rolled back")
            raise


def _param_indices(optimizer) -> dict[int, int]:
    indices: dict[int, int] = {}
    for group in optimizer.param_groups:
        for param in group.get("params", ()):
            indices[id(param)] = len(indices)
    return indices


def _tensor_nbytes(tensor) -> int:
    return int(tensor.numel() * tensor.element_size())


def _normalize_name_part(value: str) -> str:
    normalized = str(value).strip().replace("\\", "_").replace("/", "_")
    if not normalized:
        raise ValueError("optimizer state bucket name part must be non-empty")
    return normalized


def _require_torch():
    try:
        import torch
    except ImportError as exc:
        raise RuntimeError("TorchOptimizerTransactionAdapter requires PyTorch") from exc
    return torch


__all__ = [
    "TorchOptimizerStateBatch",
    "TorchOptimizerStateBucket",
    "TorchOptimizerStateIndex",
    "TorchOptimizerStateMirror",
    "TorchOptimizerStateRegistry",
    "TorchOptimizerTransactionAdapter",
]
