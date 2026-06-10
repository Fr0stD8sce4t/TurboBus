from __future__ import annotations

import json
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any

from . import native_runtime


@dataclass
class RuntimeOptions:
    chunk_bytes: int = 16 * 1024 * 1024
    staging_slots: int = 2
    enable_peer_access: bool = True
    profile_bytes: int = 256 * 1024 * 1024
    profile_on_first_transfer: bool = True
    profile_cache_enabled: bool = True
    min_chunks_for_relay: int = 2
    min_pool_bytes: int = 12 * 1024 * 1024
    relay_min_effective_bw_gbps: float = 0.0
    relay_min_direct_ratio: float = 0.0
    enable_dynamic_weights: bool = False
    dynamic_weight_alpha: float = 0.25
    clear_relay_staging_on_chunk: bool = False
    daemon_socket_path: str | None = None
    worker_socket_path: str | None = None
    daemon_max_inflight_chunks: int = 8
    daemon_profile_max_age_seconds: float = 3600.0
    worker_runtime_cache_entries: int = 8
    worker_terminal_history_entries: int = 128
    admission_retry_timeout_seconds: float = 5.0
    admission_retry_interval_seconds: float = 0.05

    @classmethod
    def from_tuning_json(cls, path: str | Path) -> "RuntimeOptions":
        data = _read_json(path)
        best = data.get("best")
        if not isinstance(best, dict):
            raise ValueError("tuning JSON does not contain a 'best' object")
        return cls(**_runtime_option_values_from_mapping(best))

    @classmethod
    def from_profile_json(cls, path: str | Path) -> "RuntimeOptions":
        data = _read_json(path)
        config = data.get("config", {})
        if not isinstance(config, dict):
            raise ValueError("profile JSON contains an invalid 'config' object")
        return cls(**_runtime_option_values_from_mapping(config))

    def to_native(self):
        native = native_runtime.native_module()
        options = native.RuntimeOptions()
        options.chunk_bytes = self.chunk_bytes
        options.staging_slots = self.staging_slots
        options.enable_peer_access = self.enable_peer_access
        options.profile_bytes = self.profile_bytes
        options.profile_on_first_transfer = self.profile_on_first_transfer
        options.profile_cache_enabled = self.profile_cache_enabled
        options.transfer_mode = native.TransferMode.Pool
        options.min_chunks_for_relay = self.min_chunks_for_relay
        options.min_pool_bytes = self.min_pool_bytes
        options.relay_min_effective_bw_gbps = self.relay_min_effective_bw_gbps
        options.relay_min_direct_ratio = self.relay_min_direct_ratio
        options.enable_dynamic_weights = self.enable_dynamic_weights
        options.dynamic_weight_alpha = self.dynamic_weight_alpha
        options.clear_relay_staging_on_chunk = self.clear_relay_staging_on_chunk
        return options


def _read_json(path: str | Path) -> dict:
    with Path(path).open("r", encoding="utf-8") as file:
        data = json.load(file)
    if not isinstance(data, dict):
        raise ValueError("expected a JSON object")
    return data


def _runtime_option_values_from_mapping(data: dict[str, Any]) -> dict[str, Any]:
    defaults = RuntimeOptions()
    option_fields = {field.name: field for field in fields(RuntimeOptions)}
    aliases = {
        "runtime_cache_entries": "worker_runtime_cache_entries",
        "terminal_history_entries": "worker_terminal_history_entries",
    }
    values: dict[str, Any] = {}
    for field_name, field in option_fields.items():
        if field_name in data:
            values[field_name] = _coerce_runtime_option_value(
                data[field_name],
                getattr(defaults, field_name),
            )
    for alias, field_name in aliases.items():
        if alias in data and field_name not in values:
            values[field_name] = _coerce_runtime_option_value(
                data[alias],
                getattr(defaults, field_name),
            )
    return values


def _coerce_runtime_option_value(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(default, bool):
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "on"}:
                return True
            if normalized in {"0", "false", "no", "off"}:
                return False
            raise ValueError(f"invalid boolean runtime option value: {value}")
        return bool(value)
    if isinstance(default, int) and not isinstance(default, bool):
        return int(value)
    if isinstance(default, float):
        return float(value)
    if isinstance(default, str) or default is None:
        return None if value is None else str(value)
    return value


__all__ = ["RuntimeOptions"]
