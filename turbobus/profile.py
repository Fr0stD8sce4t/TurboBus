from __future__ import annotations

import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from . import native_runtime
from .runtime_engine import RuntimeOptions


@dataclass(frozen=True)
class SimpleProfileRelay:
    relay_device: int
    target_device: int
    h2d_bw_gbps: float
    d2h_bw_gbps: float
    p2p_bw_gbps: float
    effective_bw_gbps: float
    effective_d2h_bw_gbps: float
    p2p_enabled: bool


@dataclass(frozen=True)
class SimpleProfileResult:
    target_device: int
    direct_h2d_bw_gbps: float
    direct_d2h_bw_gbps: float
    relays: list[SimpleProfileRelay]


def profile_to_daemon_dict(profile) -> dict[str, object]:
    return {
        "target_device": int(getattr(profile, "target_device", 0)),
        "direct_h2d_bw_gbps": float(getattr(profile, "direct_h2d_bw_gbps", 0.0) or 0.0),
        "direct_d2h_bw_gbps": float(getattr(profile, "direct_d2h_bw_gbps", 0.0) or 0.0),
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
        profile_obj.direct_d2h_bw_gbps = float(profile.get("direct_d2h_bw_gbps", 0.0) or 0.0)
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
            "effective_bw_gbps": float(relay.get("effective_bw_gbps", 0.0) or 0.0),
            "effective_d2h_bw_gbps": float(relay.get("effective_d2h_bw_gbps", 0.0) or 0.0),
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
            profile_relays.append(
                SimpleProfileRelay(
                    relay_device=relay_obj["relay_device"],
                    target_device=relay_obj["target_device"],
                    h2d_bw_gbps=relay_obj["h2d_bw_gbps"],
                    d2h_bw_gbps=relay_obj["d2h_bw_gbps"],
                    p2p_bw_gbps=relay_obj["p2p_bw_gbps"],
                    effective_bw_gbps=relay_obj["effective_bw_gbps"],
                    effective_d2h_bw_gbps=relay_obj["effective_d2h_bw_gbps"],
                    p2p_enabled=relay_obj["p2p_enabled"],
                )
            )
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


def collect_cuda_profile(
    backend,
    options: RuntimeOptions,
    target_gpu: int,
    relay_gpus: Iterable[int],
    *,
    profile_bytes: int | None = None,
    force: bool = False,
):
    runtime = backend.create_runtime(options)
    backend.initialize_runtime(
        runtime,
        int(target_gpu),
        [int(gpu) for gpu in relay_gpus],
    )
    return backend.profile(
        runtime,
        int(options.profile_bytes if profile_bytes is None else profile_bytes),
        force=force,
    )


def install_daemon_profile(
    daemon_client,
    *,
    target_gpu: int,
    relay_gpus: Iterable[int],
    profile,
    profile_bytes: int,
):
    writer = getattr(daemon_client, "put_profile", None)
    if not callable(writer):
        raise TypeError("daemon client must support put_profile")
    response = writer(
        int(target_gpu),
        [int(gpu) for gpu in relay_gpus],
        validate_daemon_profile_dict(
            profile_to_daemon_dict(profile),
            target_gpu=int(target_gpu),
            relay_gpus=relay_gpus,
        ),
        profile_bytes=int(profile_bytes),
    )
    if not getattr(response, "ok", False):
        error = getattr(response, "error", None)
        raise RuntimeError(error or "daemon profile bootstrap failed")
    return response


def bootstrap_daemon_profile(
    daemon_client,
    backend,
    options: RuntimeOptions,
    *,
    target_gpu: int,
    relay_gpus: Iterable[int],
    force: bool = False,
):
    relays = tuple(int(gpu) for gpu in relay_gpus)
    if not force and bool(options.profile_cache_enabled):
        reader = getattr(daemon_client, "get_profile", None)
        if callable(reader):
            cached = reader(int(target_gpu), list(relays))
            if getattr(cached, "ok", False):
                entry = cached.payload.get("profile")
                if isinstance(entry, Mapping) and daemon_profile_is_fresh(
                    entry,
                    float(options.daemon_profile_max_age_seconds),
                ):
                    return profile_from_daemon_entry(entry, int(target_gpu)), cached
    profile_bytes = int(options.profile_bytes)
    profile = collect_cuda_profile(
        backend,
        options,
        int(target_gpu),
        relays,
        profile_bytes=profile_bytes,
        force=force,
    )
    response = install_daemon_profile(
        daemon_client,
        target_gpu=int(target_gpu),
        relay_gpus=relays,
        profile=profile,
        profile_bytes=profile_bytes,
    )
    return profile, response
