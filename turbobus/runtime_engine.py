from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from . import native_runtime
from .schema import TransferMode


@dataclass
class RuntimeOptions:
    chunk_bytes: int = 16 * 1024 * 1024
    staging_slots: int = 2
    enable_peer_access: bool = True
    profile_bytes: int = 256 * 1024 * 1024
    profile_on_first_transfer: bool = True
    profile_cache_enabled: bool = True
    transfer_mode: TransferMode | str = TransferMode.POOL
    min_chunks_for_relay: int = 2
    min_pool_bytes: int = 12 * 1024 * 1024
    relay_min_effective_bw_gbps: float = 0.0
    relay_min_direct_ratio: float = 0.0
    enable_dynamic_weights: bool = False
    dynamic_weight_alpha: float = 0.25
    daemon_socket_path: str | None = None
    daemon_max_inflight_chunks: int = 8
    daemon_profile_max_age_seconds: float = 3600.0

    @classmethod
    def from_tuning_json(cls, path: str | Path) -> "RuntimeOptions":
        data = _read_json(path)
        best = data.get("best")
        if not isinstance(best, dict):
            raise ValueError("tuning JSON does not contain a 'best' object")
        chunk_bytes = int(best["chunk_bytes"])
        staging_slots = int(best["staging_slots"])
        return cls(chunk_bytes=chunk_bytes, staging_slots=staging_slots)

    @classmethod
    def from_profile_json(cls, path: str | Path) -> "RuntimeOptions":
        data = _read_json(path)
        config = data.get("config", {})
        if not isinstance(config, dict):
            raise ValueError("profile JSON contains an invalid 'config' object")
        defaults = cls()
        return cls(
            chunk_bytes=int(config.get("chunk_bytes", defaults.chunk_bytes)),
            staging_slots=int(config.get("staging_slots", defaults.staging_slots)),
            profile_bytes=int(config.get("profile_bytes", defaults.profile_bytes)),
        )

    def to_native(self):
        native = native_runtime.native_module()
        options = native.RuntimeOptions()
        options.chunk_bytes = self.chunk_bytes
        options.staging_slots = self.staging_slots
        options.enable_peer_access = self.enable_peer_access
        options.profile_bytes = self.profile_bytes
        options.profile_on_first_transfer = self.profile_on_first_transfer
        options.profile_cache_enabled = self.profile_cache_enabled
        mode = TransferMode(self.transfer_mode)
        options.transfer_mode = (
            native.TransferMode.Pool
            if mode is TransferMode.AUTO
            else native_runtime.native_transfer_mode(mode)
        )
        options.min_chunks_for_relay = self.min_chunks_for_relay
        options.relay_min_effective_bw_gbps = self.relay_min_effective_bw_gbps
        options.relay_min_direct_ratio = self.relay_min_direct_ratio
        options.enable_dynamic_weights = self.enable_dynamic_weights
        options.dynamic_weight_alpha = self.dynamic_weight_alpha
        return options


def _read_json(path: str | Path) -> dict:
    with Path(path).open("r", encoding="utf-8") as file:
        data = json.load(file)
    if not isinstance(data, dict):
        raise ValueError("expected a JSON object")
    return data


class _TransferStatsWithDaemon:
    def __init__(self, stats, daemon_info: dict[str, object]) -> None:
        self._stats = stats
        self.daemon_reservation_info = dict(daemon_info)
        for key, value in self.daemon_reservation_info.items():
            setattr(self, key, value)

    def __getattr__(self, name: str):
        return getattr(self._stats, name)


def _attach_daemon_stats(stats, daemon_info: dict[str, object]):
    if isinstance(stats, dict):
        return {**stats, **daemon_info}
    return _TransferStatsWithDaemon(stats, daemon_info)


class TransferHandle:
    def __init__(
        self,
        runtime,
        native_handle,
        daemon_reservations: list[str] | None = None,
    ) -> None:
        self.runtime = runtime
        self.native = native_handle
        self._daemon_reservations = list(daemon_reservations or [])
        last_daemon_reservation = getattr(runtime, "last_daemon_reservation_dict", None)
        self.daemon_reservation_info = (
            last_daemon_reservation() if callable(last_daemon_reservation) else {}
        )
        self._status = "submitted"
        self._stats = None
        self.error = ""

    @property
    def id(self) -> int:
        return self.native.id

    @property
    def status(self) -> str:
        return self._status

    @property
    def done(self) -> bool:
        return self._status == "complete"

    @property
    def stats(self):
        return self._stats

    def wait(self) -> None:
        if self.done:
            return
        try:
            self.runtime.wait(self)
        except Exception as exc:  # pragma: no cover - error path depends on CUDA
            self._status = "failed"
            self.error = str(exc)
            raise
        else:
            self._status = "complete"

    def __repr__(self) -> str:
        return f"TransferHandle(id={self.id}, status={self.status})"


__all__ = ["RuntimeOptions", "TransferHandle"]
