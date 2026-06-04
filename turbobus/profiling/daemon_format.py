from __future__ import annotations

import time
from collections.abc import Iterable, Mapping

from .. import native_runtime
from .models import SimpleProfileRelay, SimpleProfileResult


def profile_to_daemon_dict(profile) -> dict[str, object]:
    return {
        "target_device": int(getattr(profile, "target_device", 0)),
        "direct_h2d_bw_gbps": float(
            getattr(profile, "direct_h2d_bw_gbps", 0.0) or 0.0
        ),
        "direct_d2h_bw_gbps": float(
            getattr(profile, "direct_d2h_bw_gbps", 0.0) or 0.0
        ),
        "relays": [
            {
                "relay_device": int(getattr(relay, "relay_device")),
                "target_device": int(getattr(relay, "target_device", 0)),
                "h2d_bw_gbps": float(getattr(relay, "h2d_bw_gbps", 0.0) or 0.0),
                "d2h_bw_gbps": float(getattr(relay, "d2h_bw_gbps", 0.0) or 0.0),
                "p2p_bw_gbps": float(getattr(relay, "p2p_bw_gbps", 0.0) or 0.0),
                "effective_bw_gbps": float(
                    getattr(relay, "effective_bw_gbps", 0.0) or 0.0
                ),
                "effective_d2h_bw_gbps": float(
                    getattr(relay, "effective_d2h_bw_gbps", 0.0) or 0.0
                ),
                "p2p_enabled": bool(getattr(relay, "p2p_enabled", False)),
            }
            for relay in getattr(profile, "relays", []) or []
        ],
    }


def validate_daemon_profile_dict(
    profile: Mapping[str, object],
    *,
    target_gpu: int,
    relay_gpus: Iterable[int],
) -> dict[str, object]:
    if not isinstance(profile, Mapping):
        raise TypeError("profile must be a mapping")
    normalized = dict(profile)
    target = int(target_gpu)
    profile_target = int(normalized.get("target_device", target))
    if profile_target != target:
        raise ValueError("profile target_device must match runtime target_gpu")
    normalized["target_device"] = target
    expected_relays = sorted({int(gpu) for gpu in relay_gpus})
    relays = []
    for relay in normalized.get("relays", []) or []:
        if not isinstance(relay, Mapping):
            raise ValueError("profile relays must be mappings")
        relay_record = dict(relay)
        relay_target = int(relay_record.get("target_device", target))
        if relay_target != target:
            raise ValueError("profile relay target_device must match runtime target_gpu")
        relay_record["relay_device"] = int(relay_record["relay_device"])
        relay_record["target_device"] = target
        relays.append(relay_record)
    profile_relays = [int(relay["relay_device"]) for relay in relays]
    if len(profile_relays) != len(set(profile_relays)):
        raise ValueError("profile relay devices must be unique")
    profile_relays = sorted(profile_relays)
    if profile_relays != expected_relays:
        raise ValueError("profile relay devices must match daemon-discovered relays")
    normalized["relays"] = relays
    return normalized


def profile_from_daemon_entry(entry: Mapping, target_gpu: int):
    profile = entry.get("profile")
    if not isinstance(profile, Mapping):
        raise ValueError("daemon profile entry has no profile object")
    if int(profile.get("target_device", target_gpu)) != int(target_gpu):
        raise ValueError("daemon profile target_device does not match target_gpu")
    direct_h2d = float(profile.get("direct_h2d_bw_gbps", 0.0) or 0.0)
    if direct_h2d <= 0.0:
        raise ValueError("daemon profile direct_h2d_bw_gbps must be positive")
    native = native_runtime.loaded_native_module()
    use_native_profile = native is not None and hasattr(native, "ProfileResult")
    if use_native_profile:
        profile_obj = native.ProfileResult()
        profile_obj.target_device = int(profile.get("target_device", target_gpu))
        profile_obj.direct_h2d_bw_gbps = direct_h2d
        profile_obj.direct_d2h_bw_gbps = float(
            profile.get("direct_d2h_bw_gbps", 0.0) or 0.0
        )
        profile_relays = []
    else:
        profile_relays = []
    for relay in profile.get("relays", []) or []:
        if not isinstance(relay, Mapping):
            raise ValueError("daemon profile relay must be an object")
        relay_obj = {
            "relay_device": int(relay["relay_device"]),
            "target_device": int(relay.get("target_device", target_gpu)),
            "h2d_bw_gbps": float(relay.get("h2d_bw_gbps", 0.0) or 0.0),
            "d2h_bw_gbps": float(relay.get("d2h_bw_gbps", 0.0) or 0.0),
            "p2p_bw_gbps": float(relay.get("p2p_bw_gbps", 0.0) or 0.0),
            "effective_bw_gbps": float(
                relay.get("effective_bw_gbps", 0.0) or 0.0
            ),
            "effective_d2h_bw_gbps": float(
                relay.get("effective_d2h_bw_gbps", 0.0) or 0.0
            ),
            "p2p_enabled": bool(relay.get("p2p_enabled", False)),
        }
        if use_native_profile:
            native_relay = native.RelayProfile()
            native_relay.relay_device = relay_obj["relay_device"]
            native_relay.target_device = relay_obj["target_device"]
            native_relay.h2d_bw_gbps = relay_obj["h2d_bw_gbps"]
            native_relay.d2h_bw_gbps = relay_obj["d2h_bw_gbps"]
            native_relay.p2p_bw_gbps = relay_obj["p2p_bw_gbps"]
            native_relay.effective_bw_gbps = relay_obj["effective_bw_gbps"]
            native_relay.effective_d2h_bw_gbps = relay_obj["effective_d2h_bw_gbps"]
            native_relay.p2p_enabled = relay_obj["p2p_enabled"]
            profile_relays.append(native_relay)
        else:
            profile_relays.append(SimpleProfileRelay(**relay_obj))
    if use_native_profile:
        profile_obj.relays = profile_relays
        return profile_obj
    return SimpleProfileResult(
        target_device=int(profile.get("target_device", target_gpu)),
        direct_h2d_bw_gbps=direct_h2d,
        direct_d2h_bw_gbps=float(profile.get("direct_d2h_bw_gbps", 0.0) or 0.0),
        relays=profile_relays,
    )


def daemon_profile_is_fresh(entry: Mapping, max_age_seconds: float) -> bool:
    if max_age_seconds <= 0:
        return True
    updated_at = float(entry.get("updated_at", 0.0) or 0.0)
    if updated_at <= 0.0:
        return False
    return (time.time() - updated_at) <= float(max_age_seconds)


__all__ = [
    "daemon_profile_is_fresh",
    "profile_from_daemon_entry",
    "profile_to_daemon_dict",
    "validate_daemon_profile_dict",
]
