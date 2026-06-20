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
    binding_ms: float = 0.0
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
            "route_policy_visible_to_transfer": False,
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
            "route_policy_visible_to_transfer": False,
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
    #  * 姝ラ1锛氭彁鍙?prefix store RuntimeSession 鍚堢害
    #  * ========================================================================
    #  * 鏁版嵁婧愶細vLLM save/store lifecycle evidence
    #  * 鎿嶄綔锛?
    #  *   1) 鎷掔粷缂哄け RuntimeSession entrypoint 鐨?prefix store evidence
    #  *   2) 鎷掔粷缂哄け transfer evidence 璁板綍鏄庣粏鎴栨毚闇?route policy 鐨勫悎绾?
    #  */
    logger.info("寮€濮嬫彁鍙?prefix store RuntimeSession 鍚堢害...")

    # // 1.1 璇诲彇 RuntimeSession entrypoint 鍚堢害
    runtime_entrypoint = evidence.get("runtime_entrypoint")
    if not isinstance(runtime_entrypoint, Mapping):
        raise ValueError(f"{source} missing RuntimeSession entrypoint")
    contract = dict(runtime_entrypoint)

    # // 1.2 鎷掔粷 route policy 鏆撮湶
    if bool(contract.get("route_policy_visible_to_transfer", True)):
        raise ValueError(f"{source} exposes route policy to transfer")
    if bool(contract.get("route_policy_visible_to_application", True)):
        raise ValueError(f"{source} exposes route policy to application")

    # // 1.3 鏍￠獙 transfer evidence 璁板綍鏄庣粏
    transfer_record = contract.get("transfer_evidence_record")
    if not isinstance(transfer_record, Mapping):
        raise ValueError(f"{source} missing transfer evidence record")
    if not bool(transfer_record.get("intents_recorded", False)):
        raise ValueError(f"{source} transfer intents were not recorded")
    if not bool(transfer_record.get("receipts_recorded", False)):
        raise ValueError(f"{source} transfer receipts were not recorded")

    # // 1.4 淇濈暀 RuntimeSession 璁板綍鎽樿
    contract["transfer_evidence_record"] = dict(transfer_record)
    logger.info("prefix store RuntimeSession 鍚堢害鎻愬彇瀹屾垚, source: %s", source)
    return contract


def _receipt_contracts_for_prefix_store(
    evidence: Mapping[str, Any],
    *,
    runtime_entrypoint: Mapping[str, Any],
    source: str,
) -> list[dict[str, Any]]:
    # /*
    #  * ========================================================================
    #  * 姝ラ2锛氭彁鍙?prefix store receipt contracts
    #  * ========================================================================
    #  * 鏁版嵁婧愶細vLLM save/store lifecycle evidence
    #  * 鎿嶄綔锛?
    #  *   1) 鎷掔粷缂哄け receipt contracts 鐨?prefix store evidence
    #  *   2) 澶嶅埗 receipt contracts 渚?store/remove 璇佹嵁閾剧户缁牎楠?
    #  */
    logger.info("寮€濮嬫彁鍙?prefix store receipt contracts...")

    # // 2.1 鏍￠獙 receipt contracts 缁撴瀯
    contracts = evidence.get("receipt_contracts")
    if not isinstance(contracts, list):
        raise ValueError(f"{source} missing receipt contracts")

    # // 2.2 澶嶅埗 receipt contracts
    copied = [dict(item) for item in contracts if isinstance(item, Mapping)]
    if len(copied) != len(contracts) or not copied:
        raise ValueError(f"{source} contains invalid receipt contracts")

    # // 2.3 鏍稿 receipt contracts 涓?RuntimeSession 璁板綍
    _require_prefix_store_transfer_record_receipts(
        runtime_entrypoint,
        receipt_contracts=copied,
        source=source,
    )
    logger.info("prefix store receipt contracts 鎻愬彇瀹屾垚, count: %s", len(copied))
    return copied


def _require_prefix_store_transfer_record_receipts(
    runtime_entrypoint: Mapping[str, Any],
    *,
    receipt_contracts: list[dict[str, Any]],
    source: str,
) -> None:
    # /*
    #  * ========================================================================
    #  * 姝ラ3锛氭牳瀵?prefix store receipt 璁板綍
    #  * ========================================================================
    #  * 鏁版嵁婧愶細receipt contracts 涓?RuntimeSession transfer evidence record
    #  * 鎿嶄綔锛?
    #  *   1) 浠?receipt contracts 鎻愬彇 intent_id 鍜?receipt_id
    #  *   2) 纭 RuntimeSession transfer evidence record 鍖呭惈杩欎簺鏍囪瘑
    #  */
    logger.info("寮€濮嬫牳瀵?prefix store receipt 璁板綍...")

    # // 3.1 璇诲彇 RuntimeSession transfer evidence 璁板綍
    transfer_record = runtime_entrypoint.get("transfer_evidence_record")
    if not isinstance(transfer_record, Mapping):
        raise ValueError(f"{source} missing transfer evidence record")

    # // 3.2 鎻愬彇 receipt contract 鏍囪瘑
    expected_intent_ids, expected_receipt_ids = _prefix_store_receipt_contract_ids(
        receipt_contracts,
        source=source,
    )

    # // 3.3 鎻愬彇 RuntimeSession transfer evidence 鏍囪瘑
    recorded_intent_ids = _prefix_store_string_set(transfer_record.get("intent_ids"))
    recorded_receipt_ids = _prefix_store_string_set(transfer_record.get("receipt_ids"))

    # // 3.4 鏍稿 receipt contract 鏄惁閮借繘鍏?RuntimeSession 璁板綍
    if not expected_intent_ids.issubset(recorded_intent_ids):
        raise ValueError(f"{source} transfer intent_ids mismatch")
    if not expected_receipt_ids.issubset(recorded_receipt_ids):
        raise ValueError(f"{source} transfer receipt_ids mismatch")
    logger.info("prefix store receipt 璁板綍鏍稿瀹屾垚, receipts: %s", len(expected_receipt_ids))


def _prefix_store_receipt_contract_ids(
    receipt_contracts: list[dict[str, Any]],
    *,
    source: str,
) -> tuple[set[str], set[str]]:
    # /*
    #  * ========================================================================
    #  * 姝ラ4锛氭彁鍙?prefix store receipt contract 鏍囪瘑
    #  * ========================================================================
    #  * 鏁版嵁婧愶細prefix store receipt contracts
    #  * 鎿嶄綔锛?
    #  *   1) 璇诲彇姣忎釜 receipt contract 鐨?intent_id 鍜?receipt_id
    #  *   2) 杩斿洖鐢ㄤ簬 RuntimeSession transfer evidence 鏍稿鐨勯泦鍚?
    #  */
    logger.info("寮€濮嬫彁鍙?prefix store receipt contract 鏍囪瘑...")

    # // 4.1 鏀堕泦 intent_id 涓?receipt_id
    intent_ids: set[str] = set()
    receipt_ids: set[str] = set()
    for contract in receipt_contracts:
        intent_id = contract.get("intent_id")
        receipt_id = contract.get("receipt_id")
        if intent_id is None or receipt_id is None:
            raise ValueError(f"{source} receipt contract missing identity fields")
        intent_ids.add(str(intent_id))
        receipt_ids.add(str(receipt_id))

    # // 4.2 鎷掔粷绌?receipt contract
    if not receipt_ids:
        raise ValueError(f"{source} contains no receipt contracts")
    logger.info(
        "prefix store receipt contract 鏍囪瘑鎻愬彇瀹屾垚, receipts: %s",
        len(receipt_ids),
    )
    return intent_ids, receipt_ids


def _prefix_store_string_set(value: object) -> set[str]:
    # /*
    #  * ========================================================================
    #  * 姝ラ5锛氬綊涓€鍖?prefix store 瀛楃涓查泦鍚?
    #  * ========================================================================
    #  * 鏁版嵁婧愶細RuntimeSession transfer evidence record
    #  * 鎿嶄綔锛?
    #  *   1) 瀛楃涓叉寜鍗曚釜鏍囪瘑澶勭悊
    #  *   2) 鍒楄〃鍜屽厓缁勮浆涓哄瓧绗︿覆闆嗗悎
    #  */
    logger.info("寮€濮嬪綊涓€鍖?prefix store 瀛楃涓查泦鍚?..")

    # // 5.1 瀛楃涓叉寜鍗曞€煎鐞?
    if isinstance(value, str):
        result = {value}
        logger.info("prefix store 瀛楃涓查泦鍚堝綊涓€鍖栧畬鎴? count: %s", len(result))
        return result

    # // 5.2 鍒楄〃鍜屽厓缁勮浆涓哄瓧绗︿覆闆嗗悎
    if isinstance(value, list | tuple):
        result = {str(item) for item in value}
        logger.info("prefix store 瀛楃涓查泦鍚堝綊涓€鍖栧畬鎴? count: %s", len(result))
        return result

    # // 5.3 闈炲簭鍒楀€艰繑鍥炵┖闆嗗悎
    logger.info("prefix store 瀛楃涓查泦鍚堝綊涓€鍖栧畬鎴? count: %s", 0)
    return set()


def _require_prefix_store_public_summary_no_identity_fields(
    summary: Mapping[str, Any],
    *,
    source: str,
) -> None:
    # /*
    #  * ========================================================================
    #  * 姝ラ6锛氭牎楠?prefix store 鍏紑鎽樿
    #  * ========================================================================
    #  * 鐩爣锛氬叕寮€ prefix/recovery 鎽樿鍙繚鐣?transfer evidence id
    #  * 鎿嶄綔锛?
    #  *   1) 鎷掔粷 RuntimeSession entrypoint 鍜?transfer record 鏄庣粏
    #  *   2) 鎷掔粷 receipt/ticket/decision/topology 鏍囪瘑
    #  */
    logger.info("寮€濮嬫牎楠?prefix store 鍏紑鎽樿...")

    # // 6.1 鎷掔粷鍏紑鎽樿鎼哄甫杩愯鎬?identity
    forbidden = {
        "runtime_entrypoint",
        "transfer_evidence_record",
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
        "daemon_recovery",
    }
    leaked = sorted(key for key in forbidden if key in summary)
    if leaked:
        raise ValueError(
            f"{source} exposes RuntimeSession identity fields: "
            + ", ".join(leaked)
        )

    # // 6.2 鎷掔粷 route policy 鏆撮湶
    if bool(summary.get("route_policy_visible_to_transfer", True)):
        raise ValueError(f"{source} exposes route policy to transfer")
    logger.info("prefix store 鍏紑鎽樿鏍￠獙瀹屾垚")


_PREFIX_STORE = TurboBusPrefixStore()


def clear_saved_prefixes(
    session_id: str | None = None,
    job_id: str | None = None,
) -> None:
    # /*
    #  * ========================================================================
    #  * 姝ラ6锛氭嫆缁濆叕鍏?prefix 娓呯┖
    #  * ========================================================================
    #  * 鐩爣锛氶槻姝㈠閮ㄤ唬鐮佺粫杩?connector cleanup lifecycle
    #  * 鏁版嵁婧愶細鍏叡 connector API
    #  * 鎿嶄綔锛?
    #  *   1) 缁熶竴鎷掔粷鍏叡娓呯┖
    #  *   2) 鎸囧悜 TurboBusRuntimeSession connector lifecycle
    #  */
    logger.info("寮€濮嬫嫆缁濆叕鍏?prefix 娓呯┖...")

    # // 6.1 鎷掔粷鍏叡娓呯┖
    raise RuntimeError(
        "saved prefix cleanup must go through TurboBusRuntimeSession connector lifecycle"
    )


def _clear_saved_prefixes_for_connector(
    session_id: str | None = None,
    job_id: str | None = None,
) -> None:
    # /*
    #  * ========================================================================
    #  * 姝ラ7锛氭竻绌?connector 鍐呴儴 prefix 缂撳瓨
    #  * ========================================================================
    #  * 鐩爣锛氬彧渚?connector 瀹屾垚 lifecycle cleanup 鍚庡垹闄ゅ叏灞€缂撳瓨
    #  * 鏁版嵁婧愶細鍏ㄥ眬 prefix store
    #  * 鎿嶄綔锛?
    #  *   1) 鎸?session/job 娓呯┖鍐呴儴缂撳瓨
    #  *   2) 涓嶄綔涓哄叕鍏卞鍑烘毚闇?
    #  */
    logger.info("寮€濮嬫竻绌?connector 鍐呴儴 prefix 缂撳瓨...")

    # // 7.1 娓呯悊鍐呴儴缂撳瓨
    _PREFIX_STORE.clear(session_id, job_id=job_id)
    logger.info("connector 鍐呴儴 prefix 缂撳瓨娓呯┖瀹屾垚")


def get_saved_prefix(
    key: str,
    session_id: str = "default",
    job_id: str | None = None,
) -> dict[str, Any] | None:
    # /*
    #  * ========================================================================
    #  * 姝ラ8锛氳鍙?saved prefix 鍏紑蹇収
    #  * ========================================================================
    #  * 鐩爣锛氱姝㈠叕鍏卞叆鍙ｈ繑鍥炲彲鍙?prefix 瀵硅薄鍜屽畬鏁?lifecycle evidence
    #  * 鏁版嵁婧愶細鍏ㄥ眬 prefix store
    #  * 鎿嶄綔锛?
    #  *   1) 璇诲彇 connector 绉佹湁 prefix 璁板綍
    #  *   2) 杩斿洖 RuntimeSession transfer evidence 缁戝畾鍚庣殑鏍囬噺蹇収
    #  */
    logger.info("寮€濮嬭鍙?saved prefix 鍏紑蹇収...")

    # // 8.1 璇诲彇鍐呴儴 prefix 瀵硅薄
    prefix = _get_saved_prefix_for_connector(str(key), str(session_id), job_id=job_id)
    if prefix is None:
        logger.info("saved prefix 鍏紑蹇収璇诲彇瀹屾垚, found: %s", False)
        return None

    # // 8.2 鏋勯€犲叕寮€蹇収
    snapshot = saved_prefix_runtime_snapshot(prefix)
    logger.info("saved prefix 鍏紑蹇収璇诲彇瀹屾垚, found: %s", True)
    return snapshot


def _get_saved_prefix_for_connector(
    key: str,
    session_id: str = "default",
    job_id: str | None = None,
) -> TurboBusSavedPrefix | None:
    # /*
    #  * ========================================================================
    #  * 姝ラ9锛氳鍙?connector 鍐呴儴 prefix 瀵硅薄
    #  * ========================================================================
    #  * 鐩爣锛氬彧渚?vLLM connector 鍐呴儴缁х画鎵ц restore/save lifecycle
    #  * 鏁版嵁婧愶細鍏ㄥ眬 prefix store
    #  * 鎿嶄綔锛?
    #  *   1) 鎸?job/session/key 绮剧‘璇诲彇瀵硅薄
    #  *   2) 涓嶄綔涓哄叕鍏卞鍑烘毚闇?
    #  */
    logger.info("寮€濮嬭鍙?connector 鍐呴儴 prefix 瀵硅薄...")

    # // 9.1 璇诲彇鍐呴儴瀵硅薄
    prefix = _PREFIX_STORE.get(str(key), str(session_id), job_id=job_id)
    logger.info("connector 鍐呴儴 prefix 瀵硅薄璇诲彇瀹屾垚, found: %s", prefix is not None)
    return prefix


def _store_saved_prefix_for_connector(prefix: TurboBusSavedPrefix) -> list[TurboBusSavedPrefix]:
    # /*
    #  * ========================================================================
    #  * 姝ラ10锛氬啓鍏?connector 鍐呴儴 prefix 瀵硅薄
    #  * ========================================================================
    #  * 鐩爣锛氬彧鍏佽宸插甫 RuntimeSession lifecycle evidence 鐨?connector 瀵硅薄杩涘叆鍏ㄥ眬缂撳瓨
    #  * 鏁版嵁婧愶細TurboBusSavedPrefix.save_lifecycle_evidence
    #  * 鎿嶄綔锛?
    #  *   1) 鏍￠獙 save evidence 缁ф壙 RuntimeSession entrypoint
    #  *   2) 鍐欏叆鍏ㄥ眬 prefix store
    #  */
    logger.info("寮€濮嬪啓鍏?connector 鍐呴儴 prefix 瀵硅薄...")

    # // 10.1 鏍￠獙 save lifecycle evidence
    save_entrypoint = _runtime_entrypoint_for_prefix_store(
        prefix.save_lifecycle_evidence,
        source="save_lifecycle_evidence",
    )
    _receipt_contracts_for_prefix_store(
        prefix.save_lifecycle_evidence,
        runtime_entrypoint=save_entrypoint,
        source="save_lifecycle_evidence",
    )

    # // 10.2 鍐欏叆鍏ㄥ眬 store
    evicted = _PREFIX_STORE.put(prefix)
    logger.info("connector 鍐呴儴 prefix 瀵硅薄鍐欏叆瀹屾垚, evicted: %s", len(evicted))
    return evicted


def store_saved_prefix(prefix: TurboBusSavedPrefix) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 姝ラ11锛氭嫆缁濆叕鍏?prefix 鍐欏叆
    #  * ========================================================================
    #  * 鐩爣锛氶槻姝㈠閮ㄤ唬鐮佷吉閫?saved prefix 鍜?receipt evidence
    #  * 鏁版嵁婧愶細鍏叡 connector API
    #  * 鎿嶄綔锛?
    #  *   1) 缁熶竴鎷掔粷鍏叡鍐欏叆
    #  *   2) 鎸囧悜 TurboBusRuntimeSession connector lifecycle
    #  */
    logger.info("寮€濮嬫嫆缁濆叕鍏?prefix 鍐欏叆...")

    # // 11.1 鎷掔粷鍏叡鍐欏叆
    raise RuntimeError(
        "saved prefix writes must go through TurboBusRuntimeSession connector lifecycle"
    )


def saved_prefix_runtime_snapshot(prefix: TurboBusSavedPrefix) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 姝ラ12锛氭瀯閫?saved prefix RuntimeSession 蹇収
    #  * ========================================================================
    #  * 鐩爣锛氬叕寮€鍙惈鏍囬噺鎽樿鍜?transfer evidence record 鐨?prefix 瑙嗗浘
    #  * 鏁版嵁婧愶細TurboBusSavedPrefix save/store lifecycle evidence
    #  * 鎿嶄綔锛?
    #  *   1) 鏍￠獙 save/store evidence 缁ф壙 RuntimeSession entrypoint
    #  *   2) 澶嶅埗 transfer evidence record 骞跺垹闄ゅ畬鏁?runtime_entrypoint
    #  */
    logger.info("寮€濮嬫瀯閫?saved prefix RuntimeSession 蹇収...")

    # // 12.1 鏍￠獙 store lifecycle evidence
    store_entrypoint = _runtime_entrypoint_for_prefix_store(
        prefix.store_lifecycle_evidence,
        source="store_lifecycle_evidence",
    )
    store_receipt_contracts = _receipt_contracts_for_prefix_store(
        prefix.store_lifecycle_evidence,
        runtime_entrypoint=store_entrypoint,
        source="store_lifecycle_evidence",
    )

    # // 12.2 鏍￠獙 save lifecycle evidence
    save_entrypoint = _runtime_entrypoint_for_prefix_store(
        prefix.save_lifecycle_evidence,
        source="save_lifecycle_evidence",
    )
    _receipt_contracts_for_prefix_store(
        prefix.save_lifecycle_evidence,
        runtime_entrypoint=save_entrypoint,
        source="save_lifecycle_evidence",
    )

    # // 12.3 杩斿洖鍏紑鏍囬噺蹇収
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
        "receipt_count": int(
            prefix.save_lifecycle_evidence.get("receipt_count", 0) or 0
        ),
        "receipt_states": str(
            prefix.save_lifecycle_evidence.get("receipt_states", "")
        ),
        "completion_sources": str(
            prefix.save_lifecycle_evidence.get("completion_sources", "")
        ),
        "save_lifecycle_evidence_id": str(
            prefix.save_lifecycle_evidence.get("evidence_id", "")
        ),
        "store_mutation_id": str(
            prefix.store_lifecycle_evidence.get("mutation_id", "")
        ),
        "transfer_evidence_id": str(
            store_entrypoint["transfer_evidence_record"].get("evidence_id", "")
        ),
        "receipt_contract_count": len(store_receipt_contracts),
        "route_policy_visible_to_transfer": False,
    }
    logger.info("saved prefix RuntimeSession 蹇収鏋勯€犲畬鎴? key: %s", prefix.key)
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
    #  * 姝ラ15锛氳鍙栧彲閫?lifecycle 鍏紑鎽樿
    #  * ========================================================================
    #  * 鏁版嵁婧愶細TurboBusSavedPrefix lifecycle evidence
    #  * 鎿嶄綔锛?
    #  *   1) 绌?evidence 鐩存帴璺宠繃
    #  *   2) 鏍￠獙 RuntimeSession transfer evidence record
    #  *   3) 鍙繑鍥炴爣閲忔憳瑕佸拰 transfer evidence record
    #  */
    logger.info("寮€濮嬭鍙栧彲閫?lifecycle 鍏紑鎽樿...")

    # // 15.1 绌?evidence 涓嶈繘鍏ュ叕寮€鎽樿
    if not evidence:
        logger.info("鍙€?lifecycle 鍏紑鎽樿璇诲彇瀹屾垚, present: %s", False)
        return None

    # // 15.2 鏍￠獙 RuntimeSession entrypoint 鍜?receipt contracts
    entrypoint = _runtime_entrypoint_for_prefix_store(evidence, source=source)
    receipt_contracts = _receipt_contracts_for_prefix_store(
        evidence,
        runtime_entrypoint=entrypoint,
        source=source,
    )

    # // 15.3 杩斿洖鍏紑鏍囬噺鎽樿
    summary = {
        "evidence_id": str(evidence.get("evidence_id", "")),
        "operation": str(evidence.get("operation", "")),
        "receipt_count": int(evidence.get("receipt_count", 0) or 0),
        "daemon_recovery_count": int(
            evidence.get("daemon_recovery_count", 0) or 0
        ),
        "daemon_recovery_sources": str(
            evidence.get("daemon_recovery_sources", "")
        ),
        "transfer_evidence_id": str(
            entrypoint["transfer_evidence_record"].get("evidence_id", "")
        ),
        "receipt_contract_count": len(receipt_contracts),
        "route_policy_visible_to_transfer": False,
    }
    logger.info("鍙€?lifecycle 鍏紑鎽樿璇诲彇瀹屾垚, present: %s", True)
    return summary


def _optional_daemon_recovery_summary_for_prefix_store(
    evidence: Mapping[str, Any],
    *,
    source: str,
) -> dict[str, Any] | None:
    # /*
    #  * ========================================================================
    #  * 姝ラ16锛氳鍙栧彲閫?daemon recovery 鍏紑鎽樿
    #  * ========================================================================
    #  * 鏁版嵁婧愶細TurboBusSavedPrefix daemon recovery evidence
    #  * 鎿嶄綔锛?
    #  *   1) 绌?recovery evidence 鐩存帴璺宠繃
    #  *   2) 鏍￠獙 RuntimeSession transfer evidence record
    #  *   3) 鍙繑鍥?recovery 鏍囬噺鎽樿
    #  */
    logger.info("寮€濮嬭鍙栧彲閫?daemon recovery 鍏紑鎽樿...")

    # // 16.1 绌?evidence 涓嶈繘鍏ュ叕寮€鎽樿
    if not evidence:
        logger.info("鍙€?daemon recovery 鍏紑鎽樿璇诲彇瀹屾垚, present: %s", False)
        return None

    # // 16.2 鏍￠獙 RuntimeSession entrypoint 鍜?transfer evidence record
    _require_prefix_store_public_summary_no_identity_fields(evidence, source=source)
    transfer_evidence_id = evidence.get("transfer_evidence_id")
    if not isinstance(transfer_evidence_id, str) or not transfer_evidence_id:
        raise ValueError(f"{source} missing transfer_evidence_id")
    if not bool(evidence.get("runtime_entrypoint_recorded", False)):
        raise ValueError(f"{source} missing RuntimeSession recorded flag")
    if not bool(evidence.get("transfer_evidence_recorded", False)):
        raise ValueError(f"{source} missing transfer evidence recorded flag")
    if bool(evidence.get("route_policy_visible_to_transfer", True)):
        raise ValueError(f"{source} exposes route policy to transfer")

    # // 16.3 杩斿洖鍏紑鏍囬噺鎽樿
    summary = {
        "operation": str(evidence.get("operation", "")),
        "request_id": str(evidence.get("request_id", "")),
        "daemon_recovery_count": int(
            evidence.get("daemon_recovery_count", 0) or 0
        ),
        "daemon_recovery_sources": str(
            evidence.get("daemon_recovery_sources", "")
        ),
        "transfer_evidence_id": str(transfer_evidence_id),
        "runtime_entrypoint_recorded": True,
        "transfer_evidence_recorded": True,
        "daemon_recovery_recorded": bool(
            evidence.get("daemon_recovery_recorded", False)
        ),
        "route_policy_visible_to_transfer": False,
    }
    logger.info("鍙€?daemon recovery 鍏紑鎽樿璇诲彇瀹屾垚, present: %s", True)
    return summary


def _remove_saved_prefix_for_connector(
    key: str,
    session_id: str = "default",
    job_id: str | None = None,
) -> TurboBusSavedPrefix | None:
    # /*
    #  * ========================================================================
    #  * 姝ラ13锛氬垹闄?connector 鍐呴儴 prefix 瀵硅薄
    #  * ========================================================================
    #  * 鐩爣锛氬彧渚?connector 鍦?lifecycle cleanup 鍚庣Щ闄ゅ叏灞€瀵硅薄
    #  * 鏁版嵁婧愶細鍏ㄥ眬 prefix store
    #  * 鎿嶄綔锛?
    #  *   1) 鎸?job/session/key 鍒犻櫎瀵硅薄
    #  *   2) 涓嶄綔涓哄叕鍏卞鍑烘毚闇?
    #  */
    logger.info("寮€濮嬪垹闄?connector 鍐呴儴 prefix 瀵硅薄...")

    # // 13.1 鍒犻櫎鍐呴儴瀵硅薄
    prefix = _PREFIX_STORE.remove(key, session_id, job_id=job_id)
    logger.info("connector 鍐呴儴 prefix 瀵硅薄鍒犻櫎瀹屾垚, found: %s", prefix is not None)
    return prefix


def remove_saved_prefix(
    key: str,
    session_id: str = "default",
    job_id: str | None = None,
) -> None:
    # /*
    #  * ========================================================================
    #  * 姝ラ14锛氭嫆缁濆叕鍏?prefix 鍒犻櫎
    #  * ========================================================================
    #  * 鐩爣锛氶槻姝㈠閮ㄤ唬鐮佺粫杩?connector cleanup lifecycle
    #  * 鏁版嵁婧愶細鍏叡 connector API
    #  * 鎿嶄綔锛?
    #  *   1) 缁熶竴鎷掔粷鍏叡鍒犻櫎
    #  *   2) 鎸囧悜 TurboBusRuntimeSession connector lifecycle
    #  */
    logger.info("寮€濮嬫嫆缁濆叕鍏?prefix 鍒犻櫎...")

    # // 14.1 鎷掔粷鍏叡鍒犻櫎
    raise RuntimeError(
        "saved prefix removal must go through TurboBusRuntimeSession connector lifecycle"
    )


__all__ = [
    "TurboBusPrefixStore",
    "get_saved_prefix",
    "saved_prefix_runtime_snapshot",
]
