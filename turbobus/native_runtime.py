from __future__ import annotations

from typing import Any

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
