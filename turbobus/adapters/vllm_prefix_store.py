from __future__ import annotations

from dataclasses import dataclass, field
import time
import logging
from typing import Any, Mapping

logger = logging.getLogger(__name__)


@dataclass
class TurboBusRequestMetadata:
    request_id: str
    prefix_key: str
    block_ids: tuple[int, ...]
    matched_tokens: int
    block_count: int
    cpu_slot_start: int = 0


@dataclass
class TurboBusSavedPrefix:
    key: str
    cpu_backings: list[Any]
    block_count: int
    matched_tokens: int
    job_id: str = "default"
    session_id: str = "default"
    source_request_id: str = ""
    bytes: int = 0
    elapsed_ms: float = 0.0
    client_init_ms: float = 0.0
    prepare_ms: float = 0.0
    cpu_alloc_ms: float = 0.0
    reused_backing: bool = False
    group_ms: float = 0.0
    adapter_ms: float = 0.0
    refs_ms: float = 0.0
    transfer_ms: float = 0.0
    register_ms: float = 0.0
    total_ms: float = 0.0
    direct_chunks: int = 0
    relay_chunks: int = 0
    direct_bytes: int = 0
    relay_bytes: int = 0
    receipt_ids: str = ""
    decision_ids: str = ""
    topology_snapshot_ids: str = ""
    ticket_ids: str = ""
    fallback_reason: str = ""
    save_layer_count: int = 0
    save_layer_ranges: int = 0
    save_lifecycle_evidence: dict[str, Any] = field(default_factory=dict)
    last_restore_lifecycle_evidence: dict[str, Any] = field(default_factory=dict)
    store_lifecycle_evidence: dict[str, Any] = field(default_factory=dict)
    cleanup_lifecycle_evidence: dict[str, Any] = field(default_factory=dict)
    daemon_recovery_evidence: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TurboBusPrefixStoreRemoval:
    prefix: TurboBusSavedPrefix
    reason: str
    cleanup_evidence: dict[str, Any]


@dataclass(frozen=True)
class TurboBusPrefixStoreMutation:
    prefix: TurboBusSavedPrefix
    evidence: dict[str, Any]
    removals: tuple[TurboBusPrefixStoreRemoval, ...] = field(default_factory=tuple)

    @property
    def removed_prefixes(self) -> list[TurboBusSavedPrefix]:
        return [removal.prefix for removal in self.removals]


@dataclass(frozen=True)
class TurboBusPrefixStoreDrain:
    prefixes: tuple[TurboBusSavedPrefix, ...]
    evidence: dict[str, Any]
    removals: tuple[TurboBusPrefixStoreRemoval, ...] = field(default_factory=tuple)


class TurboBusPrefixStore:
    def __init__(self, max_prefixes: int = 0) -> None:
        self._prefixes: dict[str, TurboBusSavedPrefix] = {}
        self.max_prefixes = max(0, int(max_prefixes))
        self._generation = 0

    def put(self, prefix: TurboBusSavedPrefix) -> list[TurboBusSavedPrefix]:
        if not prefix.key:
            raise ValueError("prefix key must not be empty")
        evicted = []
        store_key = self._store_key(prefix.key, prefix.session_id, prefix.job_id)
        previous = self._prefixes.pop(store_key, None)
        if previous is not None:
            evicted.append(previous)
        self._prefixes[store_key] = prefix
        while self.max_prefixes > 0 and len(self._prefixes) > self.max_prefixes:
            oldest_key = next(iter(self._prefixes))
            removed = self._prefixes.pop(oldest_key)
            evicted.append(removed)
        return evicted

    def put_with_lifecycle(
        self,
        prefix: TurboBusSavedPrefix,
        *,
        reason: str,
    ) -> TurboBusPrefixStoreMutation:
        if not prefix.key:
            raise ValueError("prefix key must not be empty")
        self._generation += 1
        generation = self._generation
        removals: list[TurboBusPrefixStoreRemoval] = []
        store_key = self._store_key(prefix.key, prefix.session_id, prefix.job_id)
        previous = self._prefixes.pop(store_key, None)
        if previous is not None:
            removals.append(
                self._removal(
                    previous,
                    reason="replaced",
                    generation=generation,
                    mutation_reason=reason,
                )
            )
        self._prefixes[store_key] = prefix
        while self.max_prefixes > 0 and len(self._prefixes) > self.max_prefixes:
            oldest_key = next(iter(self._prefixes))
            removed = self._prefixes.pop(oldest_key)
            removals.append(
                self._removal(
                    removed,
                    reason="capacity_evicted",
                    generation=generation,
                    mutation_reason=reason,
                )
            )
        evidence = self._store_evidence(
            prefix,
            reason=reason,
            generation=generation,
            removed_count=len(removals),
        )
        prefix.store_lifecycle_evidence = evidence
        return TurboBusPrefixStoreMutation(
            prefix=prefix,
            evidence=evidence,
            removals=tuple(removals),
        )

    def get(
        self,
        key: str,
        session_id: str = "default",
        job_id: str | None = None,
    ) -> TurboBusSavedPrefix | None:
        if job_id is not None:
            return self._prefixes.get(self._store_key(key, session_id, job_id))
        matches = [
            prefix
            for prefix in self._prefixes.values()
            if prefix.key == str(key) and prefix.session_id == str(session_id)
        ]
        if len(matches) == 1:
            return matches[0]
        return None

    def remove(
        self,
        key: str,
        session_id: str = "default",
        job_id: str | None = None,
    ) -> TurboBusSavedPrefix | None:
        if job_id is not None:
            return self._prefixes.pop(self._store_key(key, session_id, job_id), None)
        matches = [
            store_key
            for store_key, prefix in self._prefixes.items()
            if prefix.key == str(key) and prefix.session_id == str(session_id)
        ]
        if len(matches) == 1:
            return self._prefixes.pop(matches[0])
        return None

    def clear(self, session_id: str | None = None, job_id: str | None = None) -> None:
        self.drain(session_id=session_id, job_id=job_id)

    def drain(
        self,
        session_id: str | None = None,
        job_id: str | None = None,
    ) -> list[TurboBusSavedPrefix]:
        removed: list[TurboBusSavedPrefix] = []
        if session_id is None:
            if job_id is None:
                removed = list(self._prefixes.values())
                self._prefixes.clear()
                return removed
            for key, prefix in list(self._prefixes.items()):
                if key.startswith(f"{str(job_id)}\0"):
                    removed.append(prefix)
                    self._prefixes.pop(key)
            return removed
        for key, prefix in list(self._prefixes.items()):
            if prefix.session_id != str(session_id):
                continue
            if job_id is not None and prefix.job_id != str(job_id):
                continue
            removed.append(prefix)
            self._prefixes.pop(key)
        return removed

    def drain_with_lifecycle(
        self,
        session_id: str | None = None,
        job_id: str | None = None,
        *,
        reason: str,
    ) -> TurboBusPrefixStoreDrain:
        self._generation += 1
        generation = self._generation
        prefixes = tuple(self.drain(session_id=session_id, job_id=job_id))
        removals = tuple(
            self._removal(
                prefix,
                reason="drained",
                generation=generation,
                mutation_reason=reason,
            )
            for prefix in prefixes
        )
        evidence = {
            "mutation_id": f"prefix-drain-{generation}",
            "action": "drain",
            "reason": str(reason),
            "generation": generation,
            "session_id": None if session_id is None else str(session_id),
            "job_id": None if job_id is None else str(job_id),
            "prefix_count": len(prefixes),
            "store_size_after": len(self._prefixes),
            "created_at": time.time(),
        }
        return TurboBusPrefixStoreDrain(
            prefixes=prefixes,
            evidence=evidence,
            removals=removals,
        )

    def __len__(self) -> int:
        return len(self._prefixes)

    @staticmethod
    def _store_key(
        key: str,
        session_id: str = "default",
        job_id: str = "default",
    ) -> str:
        return f"{str(job_id)}\0{str(session_id)}\0{str(key)}"

    def _store_evidence(
        self,
        prefix: TurboBusSavedPrefix,
        *,
        reason: str,
        generation: int,
        removed_count: int,
    ) -> dict[str, Any]:
        save_evidence = dict(prefix.save_lifecycle_evidence)
        runtime_entrypoint = _runtime_entrypoint_for_prefix_store(
            save_evidence,
            source="save_lifecycle_evidence",
        )
        receipt_contracts = _receipt_contracts_for_prefix_store(
            save_evidence,
            runtime_entrypoint=runtime_entrypoint,
            source="save_lifecycle_evidence",
        )
        return {
            "mutation_id": f"prefix-put-{generation}",
            "action": "put",
            "reason": str(reason),
            "generation": generation,
            "key": prefix.key,
            "job_id": prefix.job_id,
            "session_id": prefix.session_id,
            "source_request_id": prefix.source_request_id,
            "block_count": prefix.block_count,
            "matched_tokens": prefix.matched_tokens,
            "receipt_ids": save_evidence.get("receipt_ids", prefix.receipt_ids),
            "ticket_ids": save_evidence.get("ticket_ids", prefix.ticket_ids),
            "decision_ids": save_evidence.get("decision_ids", prefix.decision_ids),
            "topology_snapshot_ids": save_evidence.get(
                "topology_snapshot_ids",
                prefix.topology_snapshot_ids,
            ),
            "runtime_entrypoint": runtime_entrypoint,
            "receipt_contracts": receipt_contracts,
            "route_policy_visible_to_adapter": False,
            "removed_count": int(removed_count),
            "capacity": self.max_prefixes,
            "store_size_after": len(self._prefixes),
            "created_at": time.time(),
        }

    def _removal(
        self,
        prefix: TurboBusSavedPrefix,
        *,
        reason: str,
        generation: int,
        mutation_reason: str,
    ) -> TurboBusPrefixStoreRemoval:
        runtime_entrypoint = _runtime_entrypoint_for_prefix_store(
            prefix.store_lifecycle_evidence,
            source="store_lifecycle_evidence",
        )
        receipt_contracts = _receipt_contracts_for_prefix_store(
            prefix.store_lifecycle_evidence,
            runtime_entrypoint=runtime_entrypoint,
            source="store_lifecycle_evidence",
        )
        evidence = {
            "mutation_id": f"prefix-remove-{generation}-{prefix.key}",
            "action": "remove",
            "reason": str(reason),
            "mutation_reason": str(mutation_reason),
            "generation": generation,
            "key": prefix.key,
            "job_id": prefix.job_id,
            "session_id": prefix.session_id,
            "source_request_id": prefix.source_request_id,
            "receipt_ids": prefix.receipt_ids,
            "ticket_ids": prefix.ticket_ids,
            "store_lifecycle_evidence_id": prefix.store_lifecycle_evidence.get(
                "mutation_id"
            ),
            "runtime_entrypoint": runtime_entrypoint,
            "receipt_contracts": receipt_contracts,
            "route_policy_visible_to_adapter": False,
            "created_at": time.time(),
        }
        prefix.cleanup_lifecycle_evidence = evidence
        return TurboBusPrefixStoreRemoval(
            prefix=prefix,
            reason=str(reason),
            cleanup_evidence=evidence,
        )


def _runtime_entrypoint_for_prefix_store(
    evidence: Mapping[str, Any],
    *,
    source: str,
) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 步骤1：提取 prefix store RuntimeSession 合约
    #  * ========================================================================
    #  * 数据源：vLLM save/store lifecycle evidence
    #  * 操作：
    #  *   1) 拒绝缺失 RuntimeSession entrypoint 的 prefix store evidence
    #  *   2) 拒绝缺失 adapter evidence 记录明细或暴露 route policy 的合约
    #  */
    logger.info("开始提取 prefix store RuntimeSession 合约...")

    # // 1.1 读取 RuntimeSession entrypoint 合约
    runtime_entrypoint = evidence.get("runtime_entrypoint")
    if not isinstance(runtime_entrypoint, Mapping):
        raise ValueError(f"{source} missing RuntimeSession entrypoint")
    contract = dict(runtime_entrypoint)

    # // 1.2 拒绝 route policy 暴露
    if bool(contract.get("route_policy_visible_to_adapter", True)):
        raise ValueError(f"{source} exposes route policy to adapter")
    if bool(contract.get("route_policy_visible_to_application", True)):
        raise ValueError(f"{source} exposes route policy to application")

    # // 1.3 校验 adapter evidence 记录明细
    adapter_record = contract.get("adapter_evidence_record")
    if not isinstance(adapter_record, Mapping):
        raise ValueError(f"{source} missing adapter evidence record")
    if not bool(adapter_record.get("intents_recorded", False)):
        raise ValueError(f"{source} adapter intents were not recorded")
    if not bool(adapter_record.get("receipts_recorded", False)):
        raise ValueError(f"{source} adapter receipts were not recorded")

    # // 1.4 保留 RuntimeSession 记录摘要
    contract["adapter_evidence_record"] = dict(adapter_record)
    logger.info("prefix store RuntimeSession 合约提取完成, source: %s", source)
    return contract


def _receipt_contracts_for_prefix_store(
    evidence: Mapping[str, Any],
    *,
    runtime_entrypoint: Mapping[str, Any],
    source: str,
) -> list[dict[str, Any]]:
    # /*
    #  * ========================================================================
    #  * 步骤2：提取 prefix store receipt contracts
    #  * ========================================================================
    #  * 数据源：vLLM save/store lifecycle evidence
    #  * 操作：
    #  *   1) 拒绝缺失 receipt contracts 的 prefix store evidence
    #  *   2) 复制 receipt contracts 供 store/remove 证据链继续校验
    #  */
    logger.info("开始提取 prefix store receipt contracts...")

    # // 2.1 校验 receipt contracts 结构
    contracts = evidence.get("receipt_contracts")
    if not isinstance(contracts, list):
        raise ValueError(f"{source} missing receipt contracts")

    # // 2.2 复制 receipt contracts
    copied = [dict(item) for item in contracts if isinstance(item, Mapping)]
    if len(copied) != len(contracts) or not copied:
        raise ValueError(f"{source} contains invalid receipt contracts")

    # // 2.3 核对 receipt contracts 与 RuntimeSession 记录
    _require_prefix_store_adapter_record_receipts(
        runtime_entrypoint,
        receipt_contracts=copied,
        source=source,
    )
    logger.info("prefix store receipt contracts 提取完成, count: %s", len(copied))
    return copied


def _require_prefix_store_adapter_record_receipts(
    runtime_entrypoint: Mapping[str, Any],
    *,
    receipt_contracts: list[dict[str, Any]],
    source: str,
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤3：核对 prefix store receipt 记录
    #  * ========================================================================
    #  * 数据源：receipt contracts 与 RuntimeSession adapter evidence record
    #  * 操作：
    #  *   1) 从 receipt contracts 提取 intent_id 和 receipt_id
    #  *   2) 确认 RuntimeSession adapter evidence record 包含这些标识
    #  */
    logger.info("开始核对 prefix store receipt 记录...")

    # // 3.1 读取 RuntimeSession adapter evidence 记录
    adapter_record = runtime_entrypoint.get("adapter_evidence_record")
    if not isinstance(adapter_record, Mapping):
        raise ValueError(f"{source} missing adapter evidence record")

    # // 3.2 提取 receipt contract 标识
    expected_intent_ids, expected_receipt_ids = _prefix_store_receipt_contract_ids(
        receipt_contracts,
        source=source,
    )

    # // 3.3 提取 RuntimeSession adapter evidence 标识
    recorded_intent_ids = _prefix_store_string_set(adapter_record.get("intent_ids"))
    recorded_receipt_ids = _prefix_store_string_set(adapter_record.get("receipt_ids"))

    # // 3.4 核对 receipt contract 是否都进入 RuntimeSession 记录
    if not expected_intent_ids.issubset(recorded_intent_ids):
        raise ValueError(f"{source} adapter intent_ids mismatch")
    if not expected_receipt_ids.issubset(recorded_receipt_ids):
        raise ValueError(f"{source} adapter receipt_ids mismatch")
    logger.info("prefix store receipt 记录核对完成, receipts: %s", len(expected_receipt_ids))


def _prefix_store_receipt_contract_ids(
    receipt_contracts: list[dict[str, Any]],
    *,
    source: str,
) -> tuple[set[str], set[str]]:
    # /*
    #  * ========================================================================
    #  * 步骤4：提取 prefix store receipt contract 标识
    #  * ========================================================================
    #  * 数据源：prefix store receipt contracts
    #  * 操作：
    #  *   1) 读取每个 receipt contract 的 intent_id 和 receipt_id
    #  *   2) 返回用于 RuntimeSession adapter evidence 核对的集合
    #  */
    logger.info("开始提取 prefix store receipt contract 标识...")

    # // 4.1 收集 intent_id 与 receipt_id
    intent_ids: set[str] = set()
    receipt_ids: set[str] = set()
    for contract in receipt_contracts:
        intent_id = contract.get("intent_id")
        receipt_id = contract.get("receipt_id")
        if intent_id is None or receipt_id is None:
            raise ValueError(f"{source} receipt contract missing identity fields")
        intent_ids.add(str(intent_id))
        receipt_ids.add(str(receipt_id))

    # // 4.2 拒绝空 receipt contract
    if not receipt_ids:
        raise ValueError(f"{source} contains no receipt contracts")
    logger.info(
        "prefix store receipt contract 标识提取完成, receipts: %s",
        len(receipt_ids),
    )
    return intent_ids, receipt_ids


def _prefix_store_string_set(value: object) -> set[str]:
    # /*
    #  * ========================================================================
    #  * 步骤5：归一化 prefix store 字符串集合
    #  * ========================================================================
    #  * 数据源：RuntimeSession adapter evidence record
    #  * 操作：
    #  *   1) 字符串按单个标识处理
    #  *   2) 列表和元组转为字符串集合
    #  */
    logger.info("开始归一化 prefix store 字符串集合...")

    # // 5.1 字符串按单值处理
    if isinstance(value, str):
        result = {value}
        logger.info("prefix store 字符串集合归一化完成, count: %s", len(result))
        return result

    # // 5.2 列表和元组转为字符串集合
    if isinstance(value, list | tuple):
        result = {str(item) for item in value}
        logger.info("prefix store 字符串集合归一化完成, count: %s", len(result))
        return result

    # // 5.3 非序列值返回空集合
    logger.info("prefix store 字符串集合归一化完成, count: %s", 0)
    return set()


_PREFIX_STORE = TurboBusPrefixStore()


def clear_saved_prefixes(
    session_id: str | None = None,
    job_id: str | None = None,
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤6：拒绝公共 prefix 清空
    #  * ========================================================================
    #  * 目标：防止外部代码绕过 connector cleanup lifecycle
    #  * 数据源：公共 adapter API
    #  * 操作：
    #  *   1) 统一拒绝公共清空
    #  *   2) 指向 TurboBusRuntimeSession connector lifecycle
    #  */
    logger.info("开始拒绝公共 prefix 清空...")

    # // 6.1 拒绝公共清空
    raise RuntimeError(
        "saved prefix cleanup must go through TurboBusRuntimeSession connector lifecycle"
    )


def _clear_saved_prefixes_for_connector(
    session_id: str | None = None,
    job_id: str | None = None,
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤7：清空 connector 内部 prefix 缓存
    #  * ========================================================================
    #  * 目标：只供 connector 完成 lifecycle cleanup 后删除全局缓存
    #  * 数据源：全局 prefix store
    #  * 操作：
    #  *   1) 按 session/job 清空内部缓存
    #  *   2) 不作为公共导出暴露
    #  */
    logger.info("开始清空 connector 内部 prefix 缓存...")

    # // 7.1 清理内部缓存
    _PREFIX_STORE.clear(session_id, job_id=job_id)
    logger.info("connector 内部 prefix 缓存清空完成")


def get_saved_prefix(
    key: str,
    session_id: str = "default",
    job_id: str | None = None,
) -> dict[str, Any] | None:
    # /*
    #  * ========================================================================
    #  * 步骤8：读取 saved prefix 公开快照
    #  * ========================================================================
    #  * 目标：禁止公共入口返回可变 prefix 对象和完整 lifecycle evidence
    #  * 数据源：全局 prefix store
    #  * 操作：
    #  *   1) 读取 connector 私有 prefix 记录
    #  *   2) 返回 RuntimeSession adapter evidence 绑定后的标量快照
    #  */
    logger.info("开始读取 saved prefix 公开快照...")

    # // 8.1 读取内部 prefix 对象
    prefix = _get_saved_prefix_for_connector(str(key), str(session_id), job_id=job_id)
    if prefix is None:
        logger.info("saved prefix 公开快照读取完成, found: %s", False)
        return None

    # // 8.2 构造公开快照
    snapshot = saved_prefix_runtime_snapshot(prefix)
    logger.info("saved prefix 公开快照读取完成, found: %s", True)
    return snapshot


def _get_saved_prefix_for_connector(
    key: str,
    session_id: str = "default",
    job_id: str | None = None,
) -> TurboBusSavedPrefix | None:
    # /*
    #  * ========================================================================
    #  * 步骤9：读取 connector 内部 prefix 对象
    #  * ========================================================================
    #  * 目标：只供 vLLM connector 内部继续执行 restore/save lifecycle
    #  * 数据源：全局 prefix store
    #  * 操作：
    #  *   1) 按 job/session/key 精确读取对象
    #  *   2) 不作为公共导出暴露
    #  */
    logger.info("开始读取 connector 内部 prefix 对象...")

    # // 9.1 读取内部对象
    prefix = _PREFIX_STORE.get(str(key), str(session_id), job_id=job_id)
    logger.info("connector 内部 prefix 对象读取完成, found: %s", prefix is not None)
    return prefix


def _store_saved_prefix_for_connector(prefix: TurboBusSavedPrefix) -> list[TurboBusSavedPrefix]:
    # /*
    #  * ========================================================================
    #  * 步骤10：写入 connector 内部 prefix 对象
    #  * ========================================================================
    #  * 目标：只允许已带 RuntimeSession lifecycle evidence 的 connector 对象进入全局缓存
    #  * 数据源：TurboBusSavedPrefix.save_lifecycle_evidence
    #  * 操作：
    #  *   1) 校验 save evidence 继承 RuntimeSession entrypoint
    #  *   2) 写入全局 prefix store
    #  */
    logger.info("开始写入 connector 内部 prefix 对象...")

    # // 10.1 校验 save lifecycle evidence
    save_entrypoint = _runtime_entrypoint_for_prefix_store(
        prefix.save_lifecycle_evidence,
        source="save_lifecycle_evidence",
    )
    _receipt_contracts_for_prefix_store(
        prefix.save_lifecycle_evidence,
        runtime_entrypoint=save_entrypoint,
        source="save_lifecycle_evidence",
    )

    # // 10.2 写入全局 store
    evicted = _PREFIX_STORE.put(prefix)
    logger.info("connector 内部 prefix 对象写入完成, evicted: %s", len(evicted))
    return evicted


def store_saved_prefix(prefix: TurboBusSavedPrefix) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 步骤11：拒绝公共 prefix 写入
    #  * ========================================================================
    #  * 目标：防止外部代码伪造 saved prefix 和 receipt evidence
    #  * 数据源：公共 adapter API
    #  * 操作：
    #  *   1) 统一拒绝公共写入
    #  *   2) 指向 TurboBusRuntimeSession connector lifecycle
    #  */
    logger.info("开始拒绝公共 prefix 写入...")

    # // 11.1 拒绝公共写入
    raise RuntimeError(
        "saved prefix writes must go through TurboBusRuntimeSession connector lifecycle"
    )


def saved_prefix_runtime_snapshot(prefix: TurboBusSavedPrefix) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 步骤12：构造 saved prefix RuntimeSession 快照
    #  * ========================================================================
    #  * 目标：公开只含标量摘要和 adapter evidence record 的 prefix 视图
    #  * 数据源：TurboBusSavedPrefix save/store lifecycle evidence
    #  * 操作：
    #  *   1) 校验 save/store evidence 继承 RuntimeSession entrypoint
    #  *   2) 复制 adapter evidence record 并删除完整 runtime_entrypoint
    #  */
    logger.info("开始构造 saved prefix RuntimeSession 快照...")

    # // 12.1 校验 store lifecycle evidence
    store_entrypoint = _runtime_entrypoint_for_prefix_store(
        prefix.store_lifecycle_evidence,
        source="store_lifecycle_evidence",
    )
    store_receipt_contracts = _receipt_contracts_for_prefix_store(
        prefix.store_lifecycle_evidence,
        runtime_entrypoint=store_entrypoint,
        source="store_lifecycle_evidence",
    )

    # // 12.2 校验 save lifecycle evidence
    save_entrypoint = _runtime_entrypoint_for_prefix_store(
        prefix.save_lifecycle_evidence,
        source="save_lifecycle_evidence",
    )
    _receipt_contracts_for_prefix_store(
        prefix.save_lifecycle_evidence,
        runtime_entrypoint=save_entrypoint,
        source="save_lifecycle_evidence",
    )

    # // 12.3 返回公开标量快照
    restore_summary = _optional_lifecycle_summary_for_prefix_store(
        prefix.last_restore_lifecycle_evidence,
        source="last_restore_lifecycle_evidence",
    )
    recovery_summary = _optional_daemon_recovery_summary_for_prefix_store(
        prefix.daemon_recovery_evidence,
        source="daemon_recovery_evidence",
    )
    snapshot = {
        "schema": "turbobus.vllm_saved_prefix.runtime_snapshot.v1",
        "key": prefix.key,
        "job_id": prefix.job_id,
        "session_id": prefix.session_id,
        "source_request_id": prefix.source_request_id,
        "block_count": int(prefix.block_count),
        "matched_tokens": int(prefix.matched_tokens),
        "bytes": int(prefix.bytes),
        "direct_chunks": int(prefix.direct_chunks),
        "relay_chunks": int(prefix.relay_chunks),
        "direct_bytes": int(prefix.direct_bytes),
        "relay_bytes": int(prefix.relay_bytes),
        "receipt_ids": str(prefix.receipt_ids),
        "decision_ids": str(prefix.decision_ids),
        "topology_snapshot_ids": str(prefix.topology_snapshot_ids),
        "ticket_ids": str(prefix.ticket_ids),
        "fallback_reason": str(prefix.fallback_reason),
        "save_lifecycle_evidence_id": str(
            prefix.save_lifecycle_evidence.get("evidence_id", "")
        ),
        "store_mutation_id": str(
            prefix.store_lifecycle_evidence.get("mutation_id", "")
        ),
        "adapter_evidence_record": dict(
            store_entrypoint["adapter_evidence_record"]
        ),
        "receipt_contract_count": len(store_receipt_contracts),
        "route_policy_visible_to_adapter": False,
    }
    logger.info("saved prefix RuntimeSession 快照构造完成, key: %s", prefix.key)
    if restore_summary is not None:
        snapshot["last_restore_lifecycle"] = restore_summary
    if recovery_summary is not None:
        snapshot["daemon_recovery"] = recovery_summary
    return snapshot


def _optional_lifecycle_summary_for_prefix_store(
    evidence: Mapping[str, Any],
    *,
    source: str,
) -> dict[str, Any] | None:
    # /*
    #  * ========================================================================
    #  * 步骤15：读取可选 lifecycle 公开摘要
    #  * ========================================================================
    #  * 数据源：TurboBusSavedPrefix lifecycle evidence
    #  * 操作：
    #  *   1) 空 evidence 直接跳过
    #  *   2) 校验 RuntimeSession adapter evidence record
    #  *   3) 只返回标量摘要和 adapter evidence record
    #  */
    logger.info("开始读取可选 lifecycle 公开摘要...")

    # // 15.1 空 evidence 不进入公开摘要
    if not evidence:
        logger.info("可选 lifecycle 公开摘要读取完成, present: %s", False)
        return None

    # // 15.2 校验 RuntimeSession entrypoint 和 receipt contracts
    entrypoint = _runtime_entrypoint_for_prefix_store(evidence, source=source)
    receipt_contracts = _receipt_contracts_for_prefix_store(
        evidence,
        runtime_entrypoint=entrypoint,
        source=source,
    )

    # // 15.3 返回公开标量摘要
    summary = {
        "evidence_id": str(evidence.get("evidence_id", "")),
        "operation": str(evidence.get("operation", "")),
        "receipt_ids": str(evidence.get("receipt_ids", "")),
        "receipt_count": int(evidence.get("receipt_count", 0) or 0),
        "daemon_recovery_count": int(
            evidence.get("daemon_recovery_count", 0) or 0
        ),
        "daemon_recovery_sources": str(
            evidence.get("daemon_recovery_sources", "")
        ),
        "adapter_evidence_record": dict(entrypoint["adapter_evidence_record"]),
        "receipt_contract_count": len(receipt_contracts),
        "route_policy_visible_to_adapter": False,
    }
    logger.info("可选 lifecycle 公开摘要读取完成, present: %s", True)
    return summary


def _optional_daemon_recovery_summary_for_prefix_store(
    evidence: Mapping[str, Any],
    *,
    source: str,
) -> dict[str, Any] | None:
    # /*
    #  * ========================================================================
    #  * 步骤16：读取可选 daemon recovery 公开摘要
    #  * ========================================================================
    #  * 数据源：TurboBusSavedPrefix daemon recovery evidence
    #  * 操作：
    #  *   1) 空 recovery evidence 直接跳过
    #  *   2) 校验 RuntimeSession adapter evidence record
    #  *   3) 只返回 recovery 标量摘要
    #  */
    logger.info("开始读取可选 daemon recovery 公开摘要...")

    # // 16.1 空 evidence 不进入公开摘要
    if not evidence:
        logger.info("可选 daemon recovery 公开摘要读取完成, present: %s", False)
        return None

    # // 16.2 校验 RuntimeSession entrypoint 和 adapter evidence record
    entrypoint = _runtime_entrypoint_for_prefix_store(evidence, source=source)
    adapter_record = evidence.get("adapter_evidence_record")
    if not isinstance(adapter_record, Mapping):
        raise ValueError(f"{source} missing adapter evidence record")
    if str(adapter_record.get("evidence_id")) != str(
        entrypoint["adapter_evidence_record"].get("evidence_id")
    ):
        raise ValueError(f"{source} adapter evidence_id mismatch")
    if bool(evidence.get("route_policy_visible_to_adapter", True)):
        raise ValueError(f"{source} exposes route policy to adapter")

    # // 16.3 返回公开标量摘要
    summary = {
        "operation": str(evidence.get("operation", "")),
        "request_id": str(evidence.get("request_id", "")),
        "daemon_recovery_count": int(
            evidence.get("daemon_recovery_count", 0) or 0
        ),
        "daemon_recovery_sources": str(
            evidence.get("daemon_recovery_sources", "")
        ),
        "adapter_evidence_record": dict(adapter_record),
        "route_policy_visible_to_adapter": False,
    }
    logger.info("可选 daemon recovery 公开摘要读取完成, present: %s", True)
    return summary


def _remove_saved_prefix_for_connector(
    key: str,
    session_id: str = "default",
    job_id: str | None = None,
) -> TurboBusSavedPrefix | None:
    # /*
    #  * ========================================================================
    #  * 步骤13：删除 connector 内部 prefix 对象
    #  * ========================================================================
    #  * 目标：只供 connector 在 lifecycle cleanup 后移除全局对象
    #  * 数据源：全局 prefix store
    #  * 操作：
    #  *   1) 按 job/session/key 删除对象
    #  *   2) 不作为公共导出暴露
    #  */
    logger.info("开始删除 connector 内部 prefix 对象...")

    # // 13.1 删除内部对象
    prefix = _PREFIX_STORE.remove(key, session_id, job_id=job_id)
    logger.info("connector 内部 prefix 对象删除完成, found: %s", prefix is not None)
    return prefix


def remove_saved_prefix(
    key: str,
    session_id: str = "default",
    job_id: str | None = None,
) -> None:
    # /*
    #  * ========================================================================
    #  * 步骤14：拒绝公共 prefix 删除
    #  * ========================================================================
    #  * 目标：防止外部代码绕过 connector cleanup lifecycle
    #  * 数据源：公共 adapter API
    #  * 操作：
    #  *   1) 统一拒绝公共删除
    #  *   2) 指向 TurboBusRuntimeSession connector lifecycle
    #  */
    logger.info("开始拒绝公共 prefix 删除...")

    # // 14.1 拒绝公共删除
    raise RuntimeError(
        "saved prefix removal must go through TurboBusRuntimeSession connector lifecycle"
    )


__all__ = [
    "TurboBusPrefixStore",
    "TurboBusPrefixStoreDrain",
    "TurboBusPrefixStoreMutation",
    "TurboBusPrefixStoreRemoval",
    "TurboBusRequestMetadata",
    "TurboBusSavedPrefix",
    "get_saved_prefix",
    "saved_prefix_runtime_snapshot",
]
