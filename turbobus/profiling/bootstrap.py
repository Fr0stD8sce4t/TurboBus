from __future__ import annotations

from collections.abc import Iterable, Mapping

from ..runtime_options import RuntimeOptions
from .daemon_format import (
    daemon_profile_is_fresh,
    profile_from_daemon_entry,
    profile_to_daemon_dict,
    validate_daemon_profile_dict,
)


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
    cache_lookup: dict[str, object] = {
        "enabled": bool(options.profile_cache_enabled),
        "attempted": False,
        "hit": False,
        "fresh": False,
    }
    if not force and bool(options.profile_cache_enabled):
        reader = getattr(daemon_client, "get_profile", None)
        if callable(reader):
            cache_lookup["attempted"] = True
            cached = reader(int(target_gpu), list(relays))
            if getattr(cached, "ok", False):
                entry = cached.payload.get("profile")
                cache_lookup["hit"] = isinstance(entry, Mapping)
                if isinstance(entry, Mapping) and daemon_profile_is_fresh(
                    entry,
                    float(options.daemon_profile_max_age_seconds),
                ):
                    cache_lookup["fresh"] = True
                    profile = profile_from_daemon_entry(entry, int(target_gpu))
                    evidence = _profile_bootstrap_evidence(
                        source="daemon_cache",
                        target_gpu=int(target_gpu),
                        relay_gpus=relays,
                        profile_bytes=int(entry.get("profile_bytes", 0) or 0),
                        force=force,
                        cache_lookup=cache_lookup,
                        daemon_cache_verified=True,
                        daemon_cache_entry=entry,
                    )
                    return profile, _response_with_profile_bootstrap(cached, evidence)
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
    verification = _verify_daemon_profile_cache(
        daemon_client,
        target_gpu=int(target_gpu),
        relay_gpus=relays,
    )
    evidence = _profile_bootstrap_evidence(
        source="cuda_profile",
        target_gpu=int(target_gpu),
        relay_gpus=relays,
        profile_bytes=profile_bytes,
        force=force,
        cache_lookup=cache_lookup,
        daemon_cache_verified=bool(verification.get("verified", False)),
        daemon_cache_entry=verification.get("profile"),
    )
    if "error" in verification:
        evidence["daemon_cache_verification_error"] = verification["error"]
    if "response_ok" in verification:
        evidence["daemon_cache_verification_response_ok"] = verification["response_ok"]
    response = _response_with_profile_bootstrap(response, evidence)
    return profile, response


def _verify_daemon_profile_cache(
    daemon_client,
    *,
    target_gpu: int,
    relay_gpus: tuple[int, ...],
) -> dict[str, object]:
    reader = getattr(daemon_client, "get_profile", None)
    if not callable(reader):
        return {
            "verified": False,
            "error": "daemon client does not support get_profile",
        }
    try:
        response = reader(int(target_gpu), list(relay_gpus))
    except Exception as exc:
        return {
            "verified": False,
            "error": str(exc) or exc.__class__.__name__,
        }
    entry = response.payload.get("profile") if getattr(response, "ok", False) else None
    return {
        "verified": isinstance(entry, Mapping),
        "response_ok": bool(getattr(response, "ok", False)),
        "profile": entry if isinstance(entry, Mapping) else None,
    }


def _response_with_profile_bootstrap(response, evidence: Mapping[str, object]):
    payload = dict(getattr(response, "payload", {}) or {})
    payload["profile_bootstrap"] = dict(evidence)
    return response.__class__(
        ok=bool(getattr(response, "ok", False)),
        payload=payload,
        error=getattr(response, "error", None),
    )


def _profile_bootstrap_evidence(
    *,
    source: str,
    target_gpu: int,
    relay_gpus: tuple[int, ...],
    profile_bytes: int,
    force: bool,
    cache_lookup: Mapping[str, object],
    daemon_cache_verified: bool,
    daemon_cache_entry: object | None,
) -> dict[str, object]:
    summary = _profile_entry_summary(daemon_cache_entry)
    return {
        "source": str(source),
        "target_gpu": int(target_gpu),
        "relay_gpus": [int(gpu) for gpu in relay_gpus],
        "profile_bytes": int(profile_bytes),
        "force": bool(force),
        "cache_lookup": dict(cache_lookup),
        "daemon_cache_verified": bool(daemon_cache_verified),
        "profile_summary": summary,
    }


def _profile_entry_summary(entry: object | None) -> dict[str, object]:
    if not isinstance(entry, Mapping):
        return {"available": False}
    profile = entry.get("profile")
    if not isinstance(profile, Mapping):
        return {"available": False}
    relays = profile.get("relays", []) or []
    relay_records = tuple(item for item in relays if isinstance(item, Mapping))
    return {
        "available": True,
        "target_device": int(profile.get("target_device", entry.get("target_gpu", 0))),
        "direct_h2d_bw_gbps": float(profile.get("direct_h2d_bw_gbps", 0.0) or 0.0),
        "direct_d2h_bw_gbps": float(profile.get("direct_d2h_bw_gbps", 0.0) or 0.0),
        "relay_count": len(relay_records),
        "profile_bytes": int(entry.get("profile_bytes", 0) or 0),
        "updated_at": float(entry.get("updated_at", 0.0) or 0.0),
    }


__all__ = [
    "bootstrap_daemon_profile",
    "collect_cuda_profile",
    "install_daemon_profile",
]
