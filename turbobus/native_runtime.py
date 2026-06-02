from __future__ import annotations

from typing import Any

from .schema import TransferMode

try:
    from . import _turbobus
except ImportError as exc:  # pragma: no cover - depends on local build
    _turbobus = None
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None


def bind_native_runtime(native_module: Any) -> None:
    global _turbobus, _IMPORT_ERROR
    _turbobus = native_module
    _IMPORT_ERROR = None


def native_module() -> Any:
    require_extension()
    return _turbobus


def loaded_native_module() -> Any | None:
    return _turbobus


def require_extension() -> None:
    if _turbobus is None:
        raise RuntimeError(
            "turbobus native extension is not available. Build cpp/_turbobus "
            "before using the runtime."
        ) from _IMPORT_ERROR


def native_transfer_mode(mode: TransferMode | str) -> Any:
    native = native_module()
    if not isinstance(mode, TransferMode):
        mode = TransferMode(mode)
    if mode is TransferMode.POOL:
        return native.TransferMode.Pool
    if mode is TransferMode.DIRECT:
        return native.TransferMode.DirectOnly
    if mode is TransferMode.RELAY:
        return native.TransferMode.RelayOnly
    raise ValueError(f"unsupported transfer mode: {mode}")


def runtime_transfer_mode_value(mode: TransferMode | str) -> Any:
    if _turbobus is None:
        return TransferMode(mode)
    return native_transfer_mode(mode)
