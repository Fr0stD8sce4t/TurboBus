from __future__ import annotations

from collections.abc import Iterable, Mapping, MutableMapping


def normalize_relays(relay_gpus: Iterable[int]) -> list[int]:
    return sorted({int(gpu) for gpu in relay_gpus})


def profile_key(target_gpu: int, relay_gpus: Iterable[int]) -> str:
    relays = ",".join(str(gpu) for gpu in normalize_relays(relay_gpus))
    return f"target={int(target_gpu)};relays={relays}"


def normalize_profile(profile: dict, target_gpu: int) -> dict:
    if not isinstance(profile, dict):
        raise ValueError("profile must be a dict")
    target = int(target_gpu)
    profile_target = int(profile.get("target_device", target))
    if profile_target != target:
        raise ValueError("profile target_device must match target_gpu")
    direct_h2d = float(profile.get("direct_h2d_bw_gbps", 0.0) or 0.0)
    direct_d2h = float(profile.get("direct_d2h_bw_gbps", 0.0) or 0.0)
    if direct_h2d <= 0.0:
        raise ValueError("profile direct_h2d_bw_gbps must be positive")
    if direct_d2h <= 0.0:
        raise ValueError("profile direct_d2h_bw_gbps must be positive")
    relays = []
    for relay in profile.get("relays", []) or []:
        if not isinstance(relay, dict):
            raise ValueError("profile relays must be dicts")
        relay_target = int(relay.get("target_device", target))
        if relay_target != target:
            raise ValueError("profile relay target_device must match target_gpu")
        relay_record = {
            "relay_device": int(relay["relay_device"]),
            "target_device": relay_target,
            "h2d_bw_gbps": float(relay.get("h2d_bw_gbps", 0.0) or 0.0),
            "d2h_bw_gbps": float(relay.get("d2h_bw_gbps", 0.0) or 0.0),
            "p2p_bw_gbps": float(relay.get("p2p_bw_gbps", 0.0) or 0.0),
            "effective_bw_gbps": float(relay.get("effective_bw_gbps", 0.0) or 0.0),
            "effective_d2h_bw_gbps": float(
                relay.get("effective_d2h_bw_gbps", 0.0) or 0.0
            ),
            "p2p_enabled": bool(relay.get("p2p_enabled", False)),
        }
        _validate_relay_profile_measurement(relay_record)
        relays.append(relay_record)
    return {
        "target_device": target,
        "direct_h2d_bw_gbps": direct_h2d,
        "direct_d2h_bw_gbps": direct_d2h,
        "relays": relays,
    }


def profile_entry(
    *,
    target_gpu: int,
    relay_gpus: Iterable[int],
    profile: dict,
    profile_bytes: int,
    updated_at: float,
    topology_binding: Mapping[str, object] | None = None,
) -> dict:
    target = int(target_gpu)
    relays = normalize_relays(relay_gpus)
    normalized_profile = normalize_profile(profile, target)
    profile_relays = [int(relay["relay_device"]) for relay in normalized_profile["relays"]]
    if len(profile_relays) != len(set(profile_relays)):
        raise ValueError("profile relay devices must be unique")
    profile_relays = sorted(profile_relays)
    unexpected_relays = sorted(set(profile_relays) - set(relays))
    if unexpected_relays:
        raise ValueError("profile relay devices must be listed in relay_gpus")
    missing_relays = sorted(set(relays) - set(profile_relays))
    if missing_relays:
        raise ValueError("profile must include every daemon-discovered relay")
    entry = {
        "target_gpu": target,
        "relay_gpus": relays,
        "profile_bytes": int(profile_bytes),
        "updated_at": float(updated_at),
        "profile": normalized_profile,
    }
    if isinstance(topology_binding, Mapping):
        entry["topology_binding"] = dict(topology_binding)
    return entry


def _validate_relay_profile_measurement(relay: Mapping[str, object]) -> None:
    relay_device = int(relay["relay_device"])
    if not bool(relay.get("p2p_enabled", False)):
        raise ValueError(f"profile relay {relay_device} must have p2p_enabled")
    for field_name in (
        "h2d_bw_gbps",
        "d2h_bw_gbps",
        "p2p_bw_gbps",
        "effective_bw_gbps",
        "effective_d2h_bw_gbps",
    ):
        if float(relay.get(field_name, 0.0) or 0.0) <= 0.0:
            raise ValueError(
                f"profile relay {relay_device} {field_name} must be positive"
            )


def cached_profile(
    profile_cache: MutableMapping[str, dict],
    key: str,
) -> dict | None:
    entry = profile_cache.get(str(key))
    return dict(entry) if entry else None


def put_cached_profile(
    profile_cache: MutableMapping[str, dict],
    key: str,
    entry: dict,
) -> dict:
    profile_cache[str(key)] = entry
    return dict(entry)


def invalidate_cached_profile(
    profile_cache: MutableMapping[str, dict],
    key: str,
) -> bool:
    return profile_cache.pop(str(key), None) is not None


def purge_stale_profiles(
    profile_cache: MutableMapping[str, dict],
    *,
    max_age_seconds: float,
    now: float,
) -> list[str]:
    if max_age_seconds <= 0.0:
        return []
    expired = [
        key
        for key, entry in profile_cache.items()
        if float(now) - float(entry.get("updated_at", 0.0) or 0.0) > max_age_seconds
    ]
    for key in expired:
        profile_cache.pop(key, None)
    return expired


__all__ = [
    "cached_profile",
    "invalidate_cached_profile",
    "normalize_profile",
    "normalize_relays",
    "profile_entry",
    "profile_key",
    "purge_stale_profiles",
    "put_cached_profile",
]
