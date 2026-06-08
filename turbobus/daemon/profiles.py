from __future__ import annotations

from collections.abc import Iterable, Mapping, MutableMapping

from ..profiling.daemon_format import validate_daemon_profile_dict


def normalize_relays(relay_gpus: Iterable[int]) -> list[int]:
    return sorted({int(gpu) for gpu in relay_gpus})


def profile_key(target_gpu: int, relay_gpus: Iterable[int]) -> str:
    relays = ",".join(str(gpu) for gpu in normalize_relays(relay_gpus))
    return f"target={int(target_gpu)};relays={relays}"


def normalize_profile(profile: dict, target_gpu: int) -> dict:
    if not isinstance(profile, Mapping):
        raise ValueError("profile must be a dict")
    relay_gpus = [
        int(relay["relay_device"])
        for relay in profile.get("relays", ()) or ()
        if isinstance(relay, Mapping)
    ]
    return validate_daemon_profile_dict(
        profile,
        target_gpu=int(target_gpu),
        relay_gpus=relay_gpus,
    )


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
    normalized_profile = validate_daemon_profile_dict(
        profile,
        target_gpu=target,
        relay_gpus=relays,
    )
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
        "profile_import": _profile_import_record(
            normalized_profile,
            profile_bytes=int(profile_bytes),
            updated_at=float(updated_at),
        ),
        "profile": normalized_profile,
    }
    if isinstance(topology_binding, Mapping):
        entry["topology_binding"] = dict(topology_binding)
    return entry


def _profile_import_record(
    profile: Mapping[str, object],
    *,
    profile_bytes: int,
    updated_at: float,
) -> dict[str, object]:
    records = tuple(
        item
        for item in profile.get("measurement_records", ()) or ()
        if isinstance(item, Mapping)
    )
    record_types = sorted({str(item.get("record_type", "")) for item in records})
    relay_devices = sorted(
        {
            int(item["relay_device"])
            for item in records
            if item.get("relay_device") is not None
        }
    )
    return {
        "source": "daemon_profile_import",
        "measurement_source": "cuda_profile",
        "profile_bytes": int(profile_bytes),
        "updated_at": float(updated_at),
        "record_count": len(records),
        "record_types": record_types,
        "relay_devices": relay_devices,
        "production_evidence": True,
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
