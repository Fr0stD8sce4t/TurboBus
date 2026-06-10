from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class TurboBusConnectorConfig:
    job_id: str
    session_id: str
    cpu_buffer_id: str
    gpu_buffer_id: str
    chunk_bytes: int
    profile_bytes: int
    min_pool_bytes: int
    min_chunks_for_relay: int
    relay_min_effective_bw_gbps: float
    relay_min_direct_ratio: float
    enable_dynamic_weights: bool
    dynamic_weight_alpha: float
    runtime_cache_entries: int
    terminal_history_entries: int
    clear_relay_staging_on_chunk: bool
    daemon_socket_path: str
    worker_socket_path: str
    wait_timeout_seconds: float | None
    restore_block_limit: int
    restore_enabled: bool
    max_saved_prefixes: int

    @classmethod
    def from_vllm_config(cls, vllm_config) -> "TurboBusConnectorConfig":
        session_id = extra_config_str(
            vllm_config,
            "turbobus.session_id",
            kv_transfer_engine_id(vllm_config),
        )
        return cls(
            job_id=extra_config_str(
                vllm_config,
                "turbobus.job_id",
                os.environ.get("TURBOBUS_JOB_ID", session_id),
            ),
            session_id=session_id,
            cpu_buffer_id=extra_config_str(
                vllm_config,
                "turbobus.cpu_buffer_id",
                os.environ.get("TURBOBUS_CPU_BUFFER_ID", "vllm-kv-cpu-buffer"),
            ),
            gpu_buffer_id=extra_config_str(
                vllm_config,
                "turbobus.gpu_buffer_id",
                os.environ.get("TURBOBUS_GPU_BUFFER_ID", "vllm-kv-gpu-buffer"),
            ),
            chunk_bytes=extra_config_int(
                vllm_config,
                "turbobus.chunk_bytes",
                int(os.environ.get("TURBOBUS_CHUNK_BYTES", 4 * 1024 * 1024)),
            ),
            profile_bytes=extra_config_int(
                vllm_config,
                "turbobus.profile_bytes",
                int(os.environ.get("TURBOBUS_PROFILE_BYTES", 256 * 1024 * 1024)),
            ),
            min_pool_bytes=extra_config_int(
                vllm_config,
                "turbobus.min_pool_bytes",
                int(os.environ.get("TURBOBUS_MIN_POOL_BYTES", 12 * 1024 * 1024)),
            ),
            min_chunks_for_relay=extra_config_int(
                vllm_config,
                "turbobus.min_chunks_for_relay",
                int(os.environ.get("TURBOBUS_MIN_CHUNKS_FOR_RELAY", "2") or 2),
            ),
            relay_min_effective_bw_gbps=extra_config_float(
                vllm_config,
                "turbobus.relay_min_effective_bw_gbps",
                float(
                    os.environ.get(
                        "TURBOBUS_RELAY_MIN_EFFECTIVE_BW_GBPS",
                        "0.0",
                    )
                    or 0.0
                ),
            ),
            relay_min_direct_ratio=extra_config_float(
                vllm_config,
                "turbobus.relay_min_direct_ratio",
                float(
                    os.environ.get("TURBOBUS_RELAY_MIN_DIRECT_RATIO", "0.0")
                    or 0.0
                ),
            ),
            enable_dynamic_weights=extra_config_bool(
                vllm_config,
                "turbobus.enable_dynamic_weights",
                os.environ.get("TURBOBUS_ENABLE_DYNAMIC_WEIGHTS", "0") == "1",
            ),
            dynamic_weight_alpha=extra_config_float(
                vllm_config,
                "turbobus.dynamic_weight_alpha",
                float(os.environ.get("TURBOBUS_DYNAMIC_WEIGHT_ALPHA", "0.25") or 0.25),
            ),
            runtime_cache_entries=extra_config_int(
                vllm_config,
                "turbobus.runtime_cache_entries",
                int(os.environ.get("TURBOBUS_RUNTIME_CACHE_ENTRIES", "8") or 8),
            ),
            terminal_history_entries=extra_config_int(
                vllm_config,
                "turbobus.terminal_history_entries",
                int(os.environ.get("TURBOBUS_TERMINAL_HISTORY_ENTRIES", "128") or 128),
            ),
            clear_relay_staging_on_chunk=extra_config_bool(
                vllm_config,
                "turbobus.clear_relay_staging_on_chunk",
                os.environ.get("TURBOBUS_CLEAR_RELAY_STAGING_ON_CHUNK", "0") == "1",
            ),
            daemon_socket_path=extra_config_str(
                vllm_config,
                "turbobus.daemon_socket_path",
                os.environ.get("TURBOBUS_DAEMON_SOCKET_PATH", ""),
            ),
            worker_socket_path=extra_config_str(
                vllm_config,
                "turbobus.worker_socket_path",
                os.environ.get("TURBOBUS_WORKER_SOCKET_PATH", ""),
            ),
            wait_timeout_seconds=extra_config_optional_float(
                vllm_config,
                "turbobus.wait_timeout_seconds",
                os.environ.get("TURBOBUS_WAIT_TIMEOUT_SECONDS", ""),
            ),
            restore_block_limit=extra_config_int(
                vllm_config,
                "turbobus.restore_block_limit",
                int(os.environ.get("TURBOBUS_RESTORE_BLOCK_LIMIT", "0") or 0),
            ),
            restore_enabled=extra_config_bool(
                vllm_config,
                "turbobus.restore_enabled",
                os.environ.get("TURBOBUS_RESTORE_ENABLED", "0") == "1",
            ),
            max_saved_prefixes=extra_config_int(
                vllm_config,
                "turbobus.max_saved_prefixes",
                int(os.environ.get("TURBOBUS_MAX_SAVED_PREFIXES", "0") or 0),
            ),
        )


def extra_config_int(vllm_config, key: str, default: int) -> int:
    config = getattr(vllm_config, "kv_transfer_config", None)
    getter = getattr(config, "get_from_extra_config", None)
    if getter is None:
        return default
    value = getter(key, default)
    return int(value)


def extra_config_float(vllm_config, key: str, default: float) -> float:
    config = getattr(vllm_config, "kv_transfer_config", None)
    getter = getattr(config, "get_from_extra_config", None)
    if getter is None:
        return default
    value = getter(key, default)
    return float(value)


def extra_config_optional_float(vllm_config, key: str, default) -> float | None:
    value = extra_config_value(vllm_config, key, default)
    if value is None or value == "":
        return None
    return float(value)


def extra_config_bool(vllm_config, key: str, default: bool) -> bool:
    config = getattr(vllm_config, "kv_transfer_config", None)
    getter = getattr(config, "get_from_extra_config", None)
    if getter is None:
        return default
    value = getter(key, default)
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"1", "true", "yes", "on"}


def extra_config_str(vllm_config, key: str, default: str) -> str:
    value = str(extra_config_value(vllm_config, key, default))
    if not value.strip():
        raise ValueError(f"{key} must be non-empty")
    return value


def extra_config_optional_str(vllm_config, key: str, default) -> str | None:
    value = extra_config_value(vllm_config, key, default)
    if value is None:
        return None
    text = str(value)
    if not text:
        return None
    return text


def extra_config_value(vllm_config, key: str, default):
    config = getattr(vllm_config, "kv_transfer_config", None)
    getter = getattr(config, "get_from_extra_config", None)
    if getter is None:
        return default
    return getter(key, default)


def kv_transfer_engine_id(vllm_config) -> str:
    config = getattr(vllm_config, "kv_transfer_config", None)
    engine_id = getattr(config, "engine_id", None)
    if engine_id:
        return str(engine_id)
    return "default"


__all__ = [
    "TurboBusConnectorConfig",
    "extra_config_bool",
    "extra_config_float",
    "extra_config_int",
    "extra_config_optional_str",
    "extra_config_optional_float",
    "extra_config_str",
    "extra_config_value",
    "kv_transfer_engine_id",
]
