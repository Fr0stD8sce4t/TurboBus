from __future__ import annotations

from collections.abc import Mapping

from ..schema import TransferIntent


_PHYSICAL_ROUTE_METADATA_KEYS = {
    "mode",
    "path",
    "paths",
    "physical_path",
    "physical_paths",
    "physical_route",
    "physical_routes",
    "route",
    "routes",
    "relay",
    "relays",
    "relay_gpu",
    "relay_gpus",
    "target_device",
    "target_gpu",
    "transfer_mode",
}


def require_intent_control_plane_safe(intent: TransferIntent) -> None:
    policy_violations = physical_route_keys(intent.policy_hints)
    if policy_violations:
        raise ValueError(
            "intent policy_hints must not choose physical paths: "
            + ", ".join(policy_violations)
        )
    metadata_violations = physical_route_keys(intent.metadata)
    if metadata_violations:
        raise ValueError(
            "intent metadata must not choose physical paths: "
            + ", ".join(metadata_violations)
        )


def runtime_policy_hints_without_physical_routes(
    value: Mapping[str, object] | None,
) -> dict[str, object]:
    policy_hints = {} if value is None else dict(value)
    violations = physical_route_keys(policy_hints)
    if violations:
        raise ValueError(
            "policy_hints must not choose physical paths: "
            + ", ".join(violations)
        )
    return policy_hints


def runtime_metadata_without_physical_routes(
    value: Mapping[str, object] | None,
) -> dict[str, object]:
    metadata = {} if value is None else dict(value)
    violations = physical_route_keys(metadata)
    if violations:
        raise ValueError(
            "metadata must not choose physical paths: "
            + ", ".join(violations)
        )
    return metadata


def physical_route_keys(value: Mapping[str, object]) -> list[str]:
    if not isinstance(value, Mapping):
        raise TypeError("runtime intent control metadata must be a mapping")
    invalid: list[str] = []
    for key, item in value.items():
        key_text = str(key)
        normalized = _physical_route_key_name(key_text)
        if normalized in _PHYSICAL_ROUTE_METADATA_KEYS:
            invalid.append(key_text)
        if isinstance(item, Mapping):
            invalid.extend(
                f"{key_text}.{child}"
                for child in physical_route_keys(item)
            )
    return sorted(invalid)


def _physical_route_key_name(key: str) -> str:
    normalized = str(key).lower()
    for prefix in ("turbobus.", "scheduler.", "runtime."):
        if normalized.startswith(prefix):
            return normalized.removeprefix(prefix)
    return normalized


__all__ = [
    "physical_route_keys",
    "require_intent_control_plane_safe",
    "runtime_metadata_without_physical_routes",
    "runtime_policy_hints_without_physical_routes",
]
