from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Any


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
            "created_at": time.time(),
        }
        prefix.cleanup_lifecycle_evidence = evidence
        return TurboBusPrefixStoreRemoval(
            prefix=prefix,
            reason=str(reason),
            cleanup_evidence=evidence,
        )


_PREFIX_STORE = TurboBusPrefixStore()


def clear_saved_prefixes(
    session_id: str | None = None,
    job_id: str | None = None,
) -> None:
    _PREFIX_STORE.clear(session_id, job_id=job_id)


def get_saved_prefix(
    key: str,
    session_id: str = "default",
    job_id: str | None = None,
) -> TurboBusSavedPrefix | None:
    return _PREFIX_STORE.get(str(key), str(session_id), job_id=job_id)


def store_saved_prefix(prefix: TurboBusSavedPrefix) -> list[TurboBusSavedPrefix]:
    return _PREFIX_STORE.put(prefix)


def remove_saved_prefix(
    key: str,
    session_id: str = "default",
    job_id: str | None = None,
) -> TurboBusSavedPrefix | None:
    return _PREFIX_STORE.remove(key, session_id, job_id=job_id)


__all__ = [
    "TurboBusPrefixStore",
    "TurboBusPrefixStoreDrain",
    "TurboBusPrefixStoreMutation",
    "TurboBusPrefixStoreRemoval",
    "TurboBusRequestMetadata",
    "TurboBusSavedPrefix",
    "clear_saved_prefixes",
    "get_saved_prefix",
    "remove_saved_prefix",
    "store_saved_prefix",
]
