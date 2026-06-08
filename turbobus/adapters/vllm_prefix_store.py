from __future__ import annotations

from dataclasses import dataclass, field
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


class TurboBusPrefixStore:
    def __init__(self, max_prefixes: int = 0) -> None:
        self._prefixes: dict[str, TurboBusSavedPrefix] = {}
        self.max_prefixes = max(0, int(max_prefixes))

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

    def __len__(self) -> int:
        return len(self._prefixes)

    @staticmethod
    def _store_key(
        key: str,
        session_id: str = "default",
        job_id: str = "default",
    ) -> str:
        return f"{str(job_id)}\0{str(session_id)}\0{str(key)}"


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
    "TurboBusRequestMetadata",
    "TurboBusSavedPrefix",
    "clear_saved_prefixes",
    "get_saved_prefix",
    "remove_saved_prefix",
    "store_saved_prefix",
]
