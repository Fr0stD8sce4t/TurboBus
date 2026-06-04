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
    daemon_socket_path: str
    worker_socket_path: str | None
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
            daemon_socket_path=extra_config_str(
                vllm_config,
                "turbobus.daemon_socket_path",
                os.environ.get("TURBOBUS_DAEMON_SOCKET_PATH", ""),
            ),
            worker_socket_path=extra_config_optional_str(
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
    return str(extra_config_value(vllm_config, key, default))


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
