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


__all__ = [
    "bootstrap_daemon_profile",
    "collect_cuda_profile",
    "install_daemon_profile",
]
