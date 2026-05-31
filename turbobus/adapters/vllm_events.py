from __future__ import annotations

from typing import Any


_CONNECTOR_EVENTS: list[dict[str, Any]] = []


def clear_connector_events() -> None:
    _CONNECTOR_EVENTS.clear()


def get_connector_events() -> list[dict[str, Any]]:
    return list(_CONNECTOR_EVENTS)


def emit_event(event: str, **fields) -> None:
    _CONNECTOR_EVENTS.append({"event": event, **fields})
    parts = ["turbobus_kv_connector_event", f"event={event}"]
    for key, value in fields.items():
        parts.append(f"{key}={value}")
    print(" ".join(parts), flush=True)


__all__ = [
    "clear_connector_events",
    "emit_event",
    "get_connector_events",
]
