from __future__ import annotations

import itertools
import logging
from typing import Any, Mapping

logger = logging.getLogger(__name__)

from ..client import SharedPinnedCpuBuffer
from .vllm_prefix_store import TurboBusSavedPrefix


class TurboBusCPUBackingPool:
    def __init__(
        self,
        *,
        job_id: str | None = None,
        buffer_id_prefix: str = "vllm-kv-cpu",
    ) -> None:
        self.job_id = None if job_id is None else str(job_id)
        self.buffer_id_prefix = str(buffer_id_prefix)
        self._next_buffer_id = itertools.count(1)
        self._free_by_shape: dict[tuple[tuple[int, int], ...], list[list[Any]]] = {}

    def acquire(self, block_count: int, kv_caches: list[Any]) -> tuple[list[Any], bool]:
        signature = backing_signature(block_count, kv_caches)
        available = self._free_by_shape.get(signature)
        if available:
            return available.pop(), True
        return self._allocate_for_pool(block_count, kv_caches), False

    def release(
        self,
        block_count: int,
        kv_caches: list[Any],
        cpu_backings: list[Any],
    ) -> dict[str, Any]:
        signature = backing_signature(block_count, kv_caches)
        self._free_by_shape.setdefault(signature, []).append(list(cpu_backings))
        return {
            "action": "release_to_pool",
            "block_count": int(block_count),
            "backing_count": len(cpu_backings),
            "signature": [list(item) for item in signature],
            "free_groups_for_signature": len(self._free_by_shape[signature]),
        }

    def release_prefix(
        self,
        prefix: TurboBusSavedPrefix,
        kv_caches: list[Any],
    ) -> dict[str, Any]:
        evidence = self.release(prefix.block_count, kv_caches, prefix.cpu_backings)
        lifecycle = _prefix_lifecycle_for_backing_evidence(prefix)
        evidence.update(
            {
                "prefix_key": prefix.key,
                "job_id": prefix.job_id,
                "session_id": prefix.session_id,
                "source_request_id": prefix.source_request_id,
                **lifecycle,
            }
        )
        return evidence

    def close_backings(self, cpu_backings: list[Any]) -> list[dict[str, Any]]:
        evidence = []
        for backing in cpu_backings:
            evidence.append(_close_backing(backing))
        return evidence

    def close_prefix(self, prefix: TurboBusSavedPrefix) -> dict[str, Any]:
        backing_evidence = self.close_backings(prefix.cpu_backings)
        lifecycle = _prefix_lifecycle_for_backing_evidence(prefix)
        return {
            "action": "close_prefix_backings",
            "prefix_key": prefix.key,
            "job_id": prefix.job_id,
            "session_id": prefix.session_id,
            "source_request_id": prefix.source_request_id,
            "backing_count": len(prefix.cpu_backings),
            "backings": backing_evidence,
            **lifecycle,
        }

    def close(
        self,
        *,
        runtime_close_entrypoint: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        # /*
        #  * ========================================================================
        #  * 步骤1：关闭空闲 backing 并绑定 RuntimeSession close
        #  * ========================================================================
        #  * 数据源：free backing pool 与 RuntimeSession close entrypoint
        #  * 操作：
        #  *   1) 校验 close entrypoint 来自 TurboBusRuntimeSession
        #  *   2) 关闭所有空闲 backing group
        #  *   3) 把 cleanup summary 绑定到 close record
        #  */
        logger.info("开始关闭空闲 backing 并绑定 RuntimeSession close...")

        # // 1.1 校验 RuntimeSession close entrypoint
        close_entrypoint = _runtime_close_entrypoint_for_free_backing_cleanup(
            runtime_close_entrypoint
        )

        # // 1.2 关闭 free backing group 并继承 close entrypoint
        evidence = []
        for groups in self._free_by_shape.values():
            for cpu_backings in groups:
                evidence.append(
                    {
                        "action": "close_free_backing_group",
                        "backings": self.close_backings(cpu_backings),
                        "runtime_close_entrypoint": close_entrypoint,
                        "route_policy_visible_to_adapter": False,
                    }
                )
        self._free_by_shape.clear()
        logger.info(
            "空闲 backing RuntimeSession close 绑定完成, groups: %s",
            len(evidence),
        )
        return evidence

    @staticmethod
    def _allocate(
        block_count: int,
        kv_caches: list[Any],
        *,
        job_id: str | None = None,
        buffer_id_prefix: str = "vllm-kv-cpu",
    ) -> list[Any]:
        if job_id is not None:
            return _allocate_shared_cpu_backings(
                block_count,
                kv_caches,
                job_id=str(job_id),
                buffer_id_prefix=str(buffer_id_prefix),
                next_buffer_id=itertools.count(1),
            )
        try:
            import torch
        except ImportError as exc:  # pragma: no cover - import-time convenience only
            raise RuntimeError("PyTorch is required to allocate vLLM CPU backings") from exc

        slots_per_layer = max(1, int(block_count) * max_lanes_per_layer(kv_caches))
        backings = []
        for kv_cache in kv_caches:
            from .vllm import block_bytes_from_vllm_kv_tensor

            block_bytes = block_bytes_from_vllm_kv_tensor(kv_cache)
            backings.append(
                torch.empty(
                    slots_per_layer * block_bytes,
                    dtype=torch.uint8,
                    pin_memory=True,
                )
            )
        return backings

    def _allocate_for_pool(self, block_count: int, kv_caches: list[Any]) -> list[Any]:
        if self.job_id is None:
            return self._allocate(block_count, kv_caches)
        return _allocate_shared_cpu_backings(
            block_count,
            kv_caches,
            job_id=self.job_id,
            buffer_id_prefix=self.buffer_id_prefix,
            next_buffer_id=self._next_buffer_id,
        )


def _allocate_shared_cpu_backings(
    block_count: int,
    kv_caches: list[Any],
    *,
    job_id: str,
    buffer_id_prefix: str,
    next_buffer_id,
) -> list[SharedPinnedCpuBuffer]:
    from .vllm import block_bytes_from_vllm_kv_tensor

    slots_per_layer = max(1, int(block_count) * max_lanes_per_layer(kv_caches))
    backings = []
    for kv_cache in kv_caches:
        block_bytes = block_bytes_from_vllm_kv_tensor(kv_cache)
        backings.append(
            SharedPinnedCpuBuffer.allocate(
                buffer_id=f"{buffer_id_prefix}-{next(next_buffer_id)}",
                job_id=job_id,
                size_bytes=slots_per_layer * block_bytes,
                name_prefix="turbobus-vllm",
            )
        )
    return backings


def max_lanes_per_layer(kv_caches: list[Any]) -> int:
    return max(
        (
            int(kv_cache.shape[0]) if len(getattr(kv_cache, "shape", ())) >= 3 else 1
            for kv_cache in kv_caches
        ),
        default=1,
    )


def backing_signature(block_count: int, kv_caches: list[Any]) -> tuple[tuple[int, int], ...]:
    from .vllm import block_bytes_from_vllm_kv_tensor

    slots_per_layer = max(1, int(block_count) * max_lanes_per_layer(kv_caches))
    return tuple(
        (slots_per_layer, block_bytes_from_vllm_kv_tensor(kv_cache))
        for kv_cache in kv_caches
    )


def _close_backing(backing: Any) -> dict[str, Any]:
    evidence = {
        "backing_type": type(backing).__name__,
        "buffer_id": getattr(backing, "buffer_id", None),
    }
    release = getattr(backing, "release", None)
    if callable(release):
        release()
        evidence["action"] = "release"
        evidence["closed"] = bool(getattr(backing, "closed", False))
        return evidence
    close = getattr(backing, "close", None)
    if callable(close):
        close()
        evidence["action"] = "close"
        evidence["closed"] = bool(getattr(backing, "closed", False))
        return evidence
    evidence["action"] = "none"
    evidence["closed"] = bool(getattr(backing, "closed", False))
    return evidence


def _runtime_close_entrypoint_for_free_backing_cleanup(
    runtime_close_entrypoint: Mapping[str, Any],
) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 步骤2：校验 free backing cleanup 的 RuntimeSession close entrypoint
    #  * ========================================================================
    #  * 数据源：TurboBusRuntimeSession.close response payload
    #  * 操作：
    #  *   1) 拒绝缺失 RuntimeSession close record
    #  *   2) 拒绝暴露 route policy 的 close summary
    #  */
    logger.info("开始校验 free backing RuntimeSession close entrypoint...")

    # // 2.1 校验 entrypoint 基本合约
    if not isinstance(runtime_close_entrypoint, Mapping):
        raise ValueError("free backing cleanup requires RuntimeSession close entrypoint")
    entrypoint = dict(runtime_close_entrypoint)
    if str(entrypoint.get("entrypoint")) != "TurboBusRuntimeSession":
        raise ValueError("free backing cleanup close entrypoint mismatch")
    if str(entrypoint.get("plan_source")) != "daemon_scheduler":
        raise ValueError("free backing cleanup close plan_source mismatch")
    if bool(entrypoint.get("route_policy_visible_to_adapter", True)):
        raise ValueError("free backing cleanup exposes route policy to adapter")
    if bool(entrypoint.get("route_policy_visible_to_application", True)):
        raise ValueError("free backing cleanup exposes route policy to application")

    # // 2.2 校验 close record 已写入 RuntimeSession entrypoint
    close_record = entrypoint.get("close")
    if not isinstance(close_record, Mapping):
        raise ValueError("free backing cleanup missing RuntimeSession close record")
    if str(close_record.get("entrypoint")) != "TurboBusRuntimeSession.close":
        raise ValueError("free backing cleanup close record mismatch")
    if bool(close_record.get("route_policy_visible_to_adapter", True)):
        raise ValueError("free backing close record exposes route policy")

    # // 2.3 返回隔离副本
    entrypoint["close"] = dict(close_record)
    logger.info("free backing RuntimeSession close entrypoint 校验完成")
    return entrypoint


def _prefix_lifecycle_for_backing_evidence(
    prefix: TurboBusSavedPrefix,
) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 步骤1：提取 backing lifecycle 边界
    #  * ========================================================================
    #  * 数据源：vLLM prefix lifecycle evidence
    #  * 操作：
    #  *   1) 优先读取 cleanup/store/save lifecycle evidence
    #  *   2) 保留 RuntimeSession entrypoint 和 receipt contract 证据链
    #  */
    logger.info("开始提取 backing lifecycle 边界...")

    # // 1.1 选择可用 lifecycle evidence
    source, evidence = _prefix_lifecycle_source(prefix)

    # // 1.2 提取 RuntimeSession entrypoint 合约
    runtime_entrypoint = _runtime_entrypoint_for_backing_evidence(
        evidence,
        source=source,
    )

    # // 1.3 提取 receipt contracts
    receipt_contracts = _receipt_contracts_for_backing_evidence(
        evidence,
        runtime_entrypoint=runtime_entrypoint,
        source=source,
    )

    # // 1.4 返回 backing evidence 继承字段
    adapter_record = runtime_entrypoint.get("adapter_evidence_record")
    if not isinstance(adapter_record, Mapping):
        raise ValueError(f"{source} missing adapter evidence record")
    result = {
        "lifecycle_source": source,
        "adapter_evidence_id": str(adapter_record.get("evidence_id", "")),
        "receipt_contract_count": len(receipt_contracts),
        "runtime_entrypoint_recorded": True,
        "receipt_contracts_recorded": True,
        "route_policy_visible_to_adapter": False,
    }
    logger.info("backing lifecycle 边界提取完成, source: %s", source)
    return result


def _prefix_lifecycle_source(
    prefix: TurboBusSavedPrefix,
) -> tuple[str, Mapping[str, Any]]:
    # /*
    #  * ========================================================================
    #  * 步骤2：选择 prefix lifecycle 来源
    #  * ========================================================================
    #  * 数据源：TurboBusSavedPrefix lifecycle evidence
    #  * 操作：
    #  *   1) 按 cleanup -> store -> save 的顺序选择证据
    #  *   2) 拒绝没有 RuntimeSession 证据链的 prefix
    #  */
    logger.info("开始选择 prefix lifecycle 来源...")

    # // 2.1 按优先级选择 lifecycle evidence
    candidates = (
        ("cleanup_lifecycle_evidence", prefix.cleanup_lifecycle_evidence),
        ("store_lifecycle_evidence", prefix.store_lifecycle_evidence),
        ("save_lifecycle_evidence", prefix.save_lifecycle_evidence),
    )
    for source, evidence in candidates:
        if isinstance(evidence, Mapping) and evidence.get("runtime_entrypoint") is not None:
            logger.info("prefix lifecycle 来源选择完成, source: %s", source)
            return source, evidence

    # // 2.2 拒绝缺失 RuntimeSession 证据链
    raise ValueError("prefix backing evidence missing RuntimeSession lifecycle")


def _runtime_entrypoint_for_backing_evidence(
    evidence: Mapping[str, Any],
    *,
    source: str,
) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 步骤3：提取 backing RuntimeSession 合约
    #  * ========================================================================
    #  * 数据源：prefix lifecycle evidence
    #  * 操作：
    #  *   1) 拒绝缺失 RuntimeSession entrypoint 的 backing evidence
    #  *   2) 拒绝缺失 adapter evidence 记录明细或暴露 route policy 的合约
    #  */
    logger.info("开始提取 backing RuntimeSession 合约...")

    # // 3.1 读取 RuntimeSession entrypoint 合约
    runtime_entrypoint = evidence.get("runtime_entrypoint")
    if not isinstance(runtime_entrypoint, Mapping):
        raise ValueError(f"{source} missing RuntimeSession entrypoint")
    contract = dict(runtime_entrypoint)

    # // 3.2 拒绝 route policy 暴露
    if bool(contract.get("route_policy_visible_to_adapter", True)):
        raise ValueError(f"{source} exposes route policy to adapter")
    if bool(contract.get("route_policy_visible_to_application", True)):
        raise ValueError(f"{source} exposes route policy to application")

    # // 3.3 校验 adapter evidence 记录明细
    adapter_record = contract.get("adapter_evidence_record")
    if not isinstance(adapter_record, Mapping):
        raise ValueError(f"{source} missing adapter evidence record")
    if not bool(adapter_record.get("intents_recorded", False)):
        raise ValueError(f"{source} adapter intents were not recorded")
    if not bool(adapter_record.get("receipts_recorded", False)):
        raise ValueError(f"{source} adapter receipts were not recorded")

    # // 3.4 保留 RuntimeSession 记录摘要
    contract["adapter_evidence_record"] = dict(adapter_record)
    logger.info("backing RuntimeSession 合约提取完成, source: %s", source)
    return contract


def _receipt_contracts_for_backing_evidence(
    evidence: Mapping[str, Any],
    *,
    runtime_entrypoint: Mapping[str, Any],
    source: str,
) -> list[dict[str, Any]]:
    # /*
    #  * ========================================================================
    #  * 步骤4：提取 backing receipt contracts
    #  * ========================================================================
    #  * 数据源：prefix lifecycle evidence
    #  * 操作：
    #  *   1) 拒绝缺失 receipt contracts 的 backing evidence
    #  *   2) 核对 receipt contracts 已进入 RuntimeSession adapter evidence
    #  */
    logger.info("开始提取 backing receipt contracts...")

    # // 4.1 校验 receipt contracts 结构
    contracts = evidence.get("receipt_contracts")
    if not isinstance(contracts, list):
        raise ValueError(f"{source} missing receipt contracts")

    # // 4.2 复制 receipt contracts
    copied = [dict(item) for item in contracts if isinstance(item, Mapping)]
    if len(copied) != len(contracts) or not copied:
        raise ValueError(f"{source} contains invalid receipt contracts")

    # // 4.3 核对 receipt contracts 与 RuntimeSession 记录
    _require_backing_adapter_record_receipts(
        runtime_entrypoint,
        receipt_contracts=copied,
        source=source,
    )
    logger.info("backing receipt contracts 提取完成, count: %s", len(copied))
    return copied


def _require_backing_adapter_record_receipts(
    runtime_entrypoint: Mapping[str, Any],
    *,
    receipt_contracts: list[dict[str, Any]],
    source: str,
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤5：核对 backing receipt 记录
    #  * ========================================================================
    #  * 数据源：receipt contracts 与 RuntimeSession adapter evidence record
    #  * 操作：
    #  *   1) 从 receipt contracts 提取 intent_id 和 receipt_id
    #  *   2) 确认 RuntimeSession adapter evidence record 包含这些标识
    #  */
    logger.info("开始核对 backing receipt 记录...")

    # // 5.1 读取 RuntimeSession adapter evidence 记录
    adapter_record = runtime_entrypoint.get("adapter_evidence_record")
    if not isinstance(adapter_record, Mapping):
        raise ValueError(f"{source} missing adapter evidence record")

    # // 5.2 提取 receipt contract 标识
    expected_intent_ids, expected_receipt_ids = _backing_receipt_contract_ids(
        receipt_contracts,
        source=source,
    )

    # // 5.3 提取 RuntimeSession adapter evidence 标识
    recorded_intent_ids = _backing_string_set(adapter_record.get("intent_ids"))
    recorded_receipt_ids = _backing_string_set(adapter_record.get("receipt_ids"))

    # // 5.4 核对 receipt contract 是否都进入 RuntimeSession 记录
    if not expected_intent_ids.issubset(recorded_intent_ids):
        raise ValueError(f"{source} adapter intent_ids mismatch")
    if not expected_receipt_ids.issubset(recorded_receipt_ids):
        raise ValueError(f"{source} adapter receipt_ids mismatch")
    logger.info("backing receipt 记录核对完成, receipts: %s", len(expected_receipt_ids))


def _backing_receipt_contract_ids(
    receipt_contracts: list[dict[str, Any]],
    *,
    source: str,
) -> tuple[set[str], set[str]]:
    # /*
    #  * ========================================================================
    #  * 步骤6：提取 backing receipt contract 标识
    #  * ========================================================================
    #  * 数据源：backing receipt contracts
    #  * 操作：
    #  *   1) 读取每个 receipt contract 的 intent_id 和 receipt_id
    #  *   2) 返回用于 RuntimeSession adapter evidence 核对的集合
    #  */
    logger.info("开始提取 backing receipt contract 标识...")

    # // 6.1 收集 intent_id 与 receipt_id
    intent_ids: set[str] = set()
    receipt_ids: set[str] = set()
    for contract in receipt_contracts:
        intent_id = contract.get("intent_id")
        receipt_id = contract.get("receipt_id")
        if intent_id is None or receipt_id is None:
            raise ValueError(f"{source} receipt contract missing identity fields")
        intent_ids.add(str(intent_id))
        receipt_ids.add(str(receipt_id))

    # // 6.2 拒绝空 receipt contract
    if not receipt_ids:
        raise ValueError(f"{source} contains no receipt contracts")
    logger.info(
        "backing receipt contract 标识提取完成, receipts: %s",
        len(receipt_ids),
    )
    return intent_ids, receipt_ids


def _backing_string_set(value: object) -> set[str]:
    # /*
    #  * ========================================================================
    #  * 步骤7：归一化 backing 字符串集合
    #  * ========================================================================
    #  * 数据源：RuntimeSession adapter evidence record
    #  * 操作：
    #  *   1) 字符串按单个标识处理
    #  *   2) 列表和元组转为字符串集合
    #  */
    logger.info("开始归一化 backing 字符串集合...")

    # // 7.1 字符串按单值处理
    if isinstance(value, str):
        result = {value}
        logger.info("backing 字符串集合归一化完成, count: %s", len(result))
        return result

    # // 7.2 列表和元组转为字符串集合
    if isinstance(value, list | tuple):
        result = {str(item) for item in value}
        logger.info("backing 字符串集合归一化完成, count: %s", len(result))
        return result

    # // 7.3 非序列值返回空集合
    logger.info("backing 字符串集合归一化完成, count: %s", 0)
    return set()


__all__ = [
    "TurboBusCPUBackingPool",
    "backing_signature",
    "max_lanes_per_layer",
]
