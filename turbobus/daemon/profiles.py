from __future__ import annotations

from collections.abc import Iterable, MutableMapping


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
    relays = []
    for relay in profile.get("relays", []) or []:
        if not isinstance(relay, dict):
            raise ValueError("profile relays must be dicts")
        relay_target = int(relay.get("target_device", target))
        if relay_target != target:
            raise ValueError("profile relay target_device must match target_gpu")
        relays.append(
            {
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
        )
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
) -> dict:
    target = int(target_gpu)
    relays = normalize_relays(relay_gpus)
    normalized_profile = normalize_profile(profile, target)
    profile_relays = [int(relay["relay_device"]) for relay in normalized_profile["relays"]]
    if len(profile_relays) != len(set(profile_relays)):
        raise ValueError("profile relay devices must be unique")
    profile_relays = sorted(profile_relays)
    if profile_relays != relays:
        raise ValueError("profile relay devices must match relay_gpus")
    return {
        "target_gpu": target,
        "relay_gpus": relays,
        "profile_bytes": int(profile_bytes),
        "updated_at": float(updated_at),
        "profile": normalized_profile,
    }


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
