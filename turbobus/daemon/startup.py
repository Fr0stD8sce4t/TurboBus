from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Iterable

from .server import TurboBusDaemon
from ..socket_security import UnixSocketSecurityPolicy
from ..topology import DaemonResourceInventory, TopologyProvider
from ..topology.cuda_nvml import CudaNvmlTopologyProvider, TopologyDiscoveryError


class DaemonStartupError(RuntimeError):
    pass


@dataclass(frozen=True)
class DaemonStartupConfig:
    topology_provider: str = "cuda-nvml"
    target_gpu: int | None = None
    min_relay_count: int = 1
    require_fabric: bool = True
    require_pcie: bool = True
    require_peer_credentials: bool = True
    max_sessions_per_relay: int = 1
    max_inflight_chunks_per_relay: int = 8
    min_pool_bytes: int = 12 * 1024 * 1024
    min_chunks_for_relay: int = 2
    relay_min_effective_bw_gbps: float = 0.0
    relay_min_direct_ratio: float = 0.0
    session_timeout_seconds: float = 0.0
    profile_max_age_seconds: float = 0.0
    require_root: bool = False
    socket_group: str | None = None
    socket_mode: int | str = 0o600
    max_sessions_per_uid: int = 16
    max_jobs_per_uid: int = 64
    max_buffers_per_uid: int = 4096
    max_buffer_bytes_per_uid: int = 0

    def __post_init__(self) -> None:
        min_relay_count = int(self.min_relay_count)
        if min_relay_count < 0:
            raise ValueError("min_relay_count must be non-negative")
        if int(self.min_pool_bytes) < 0:
            raise ValueError("min_pool_bytes must be non-negative")
        if int(self.min_chunks_for_relay) < 0:
            raise ValueError("min_chunks_for_relay must be non-negative")
        if self.target_gpu is not None and int(self.target_gpu) < 0:
            raise ValueError("target_gpu must be non-negative")
        socket_mode = _parse_socket_mode(self.socket_mode)
        if not bool(self.require_root) and os.name == "nt":
            pass
        object.__setattr__(
            self,
            "topology_provider",
            str(self.topology_provider).strip().lower().replace("_", "-"),
        )
        object.__setattr__(self, "min_relay_count", min_relay_count)
        object.__setattr__(self, "min_pool_bytes", int(self.min_pool_bytes))
        object.__setattr__(self, "min_chunks_for_relay", int(self.min_chunks_for_relay))
        object.__setattr__(
            self,
            "relay_min_effective_bw_gbps",
            float(self.relay_min_effective_bw_gbps),
        )
        object.__setattr__(
            self,
            "relay_min_direct_ratio",
            float(self.relay_min_direct_ratio),
        )
        if self.target_gpu is not None:
            object.__setattr__(self, "target_gpu", int(self.target_gpu))
        object.__setattr__(self, "require_root", bool(self.require_root))
        object.__setattr__(self, "socket_mode", socket_mode)
        if self.socket_group is not None:
            group = str(self.socket_group).strip()
            if not group:
                raise ValueError("socket_group must be non-empty")
            object.__setattr__(self, "socket_group", group)
        for field_name in (
            "max_sessions_per_uid",
            "max_jobs_per_uid",
            "max_buffers_per_uid",
            "max_buffer_bytes_per_uid",
        ):
            value = int(getattr(self, field_name))
            if value < 0:
                raise ValueError(f"{field_name} must be non-negative")
            object.__setattr__(self, field_name, value)


def build_topology_provider(name: str) -> TopologyProvider:
    normalized = str(name).strip().lower().replace("_", "-")
    if normalized in {"cuda-nvml", "nvidia-smi"}:
        return CudaNvmlTopologyProvider()
    raise DaemonStartupError(f"unsupported topology provider: {name}")


def create_production_daemon(
    config: DaemonStartupConfig,
    *,
    topology_provider: TopologyProvider | None = None,
) -> TurboBusDaemon:
    _validate_root_policy(config)
    provider = topology_provider or build_topology_provider(config.topology_provider)
    inventory = _snapshot_or_startup_error(provider)
    relays = relay_candidates_for_policy(inventory, config)
    return TurboBusDaemon(
        relays,
        max_sessions_per_relay=config.max_sessions_per_relay,
        max_inflight_chunks_per_relay=config.max_inflight_chunks_per_relay,
        session_timeout_seconds=config.session_timeout_seconds,
        profile_max_age_seconds=config.profile_max_age_seconds,
        min_pool_bytes=config.min_pool_bytes,
        min_chunks_for_relay=config.min_chunks_for_relay,
        relay_min_effective_bw_gbps=config.relay_min_effective_bw_gbps,
        relay_min_direct_ratio=config.relay_min_direct_ratio,
        topology_provider=provider,
        require_authenticated_peers=config.require_peer_credentials,
        socket_security_policy=UnixSocketSecurityPolicy(
            mode=config.socket_mode,
            group=config.socket_group,
        ),
        max_sessions_per_uid=config.max_sessions_per_uid,
        max_jobs_per_uid=config.max_jobs_per_uid,
        max_buffers_per_uid=config.max_buffers_per_uid,
        max_buffer_bytes_per_uid=config.max_buffer_bytes_per_uid,
    )


def relay_candidates_for_policy(
    inventory: DaemonResourceInventory,
    config: DaemonStartupConfig,
) -> tuple[int, ...]:
    _reject_fixture_inventory(inventory)
    if not inventory.gpus:
        raise DaemonStartupError("topology discovery found no GPUs")
    if config.require_pcie and not inventory.pcie_paths:
        raise DaemonStartupError("topology discovery did not report PCIe paths")
    if config.require_fabric and not inventory.fabric_links:
        raise DaemonStartupError("topology discovery did not report GPU fabric links")

    visible_gpus = tuple(sorted(gpu.device_id for gpu in inventory.gpus if gpu.visible))
    if config.target_gpu is None:
        if len(visible_gpus) < config.min_relay_count:
            raise DaemonStartupError(
                "topology discovery found fewer visible GPUs than min_relay_count"
            )
        return visible_gpus

    target = int(config.target_gpu)
    if target not in {gpu.device_id for gpu in inventory.gpus}:
        raise DaemonStartupError(f"target GPU {target} was not discovered")
    eligibility = inventory.relay_eligibility(
        target_device=target,
        requested_relays=visible_gpus,
    )
    relays = tuple(item["relay_gpu"] for item in eligibility["eligible_relays"])
    if len(relays) < config.min_relay_count:
        filtered = ", ".join(
            f"{item['relay_gpu']}:{item['reason']}"
            for item in eligibility["filtered_relays"]
        )
        raise DaemonStartupError(
            "topology discovery could not satisfy relay policy: "
            f"target_gpu={target} min_relay_count={config.min_relay_count} "
            f"eligible_relays={list(relays)} filtered_relays=[{filtered}]"
        )
    return relays


def _snapshot_or_startup_error(provider: TopologyProvider) -> DaemonResourceInventory:
    try:
        return provider.snapshot()
    except TopologyDiscoveryError as exc:
        raise DaemonStartupError(str(exc)) from exc


def _reject_fixture_inventory(inventory: DaemonResourceInventory) -> None:
    synthetic_markers = ("test_fixture", "test fixture", "fixture", "synthetic", "fake")
    source = str(inventory.source).lower()
    discovery = str(inventory.metadata.get("discovery", "")).lower()
    provider = str(inventory.metadata.get("provider", "")).lower()
    if (
        any(marker in source for marker in synthetic_markers)
        or any(marker in discovery for marker in synthetic_markers)
        or any(marker in provider for marker in synthetic_markers)
    ):
        raise DaemonStartupError(
            "production daemon startup cannot use synthetic topology fixtures"
        )


def _parse_socket_mode(value: int | str) -> int:
    if isinstance(value, str):
        raw = value.strip().lower()
        if not raw:
            raise ValueError("socket_mode must be non-empty")
        base = 8 if raw.startswith("0o") or raw.startswith("0") else 8
        return int(raw, base)
    return int(value)


def _validate_root_policy(config: DaemonStartupConfig) -> None:
    if not bool(config.require_root):
        return
    if os.name == "nt" or not hasattr(os, "geteuid"):
        raise DaemonStartupError("require_root is only supported on POSIX platforms")
    if os.geteuid() != 0:
        raise DaemonStartupError("daemon startup requires root privileges")


__all__ = [
    "DaemonStartupConfig",
    "DaemonStartupError",
    "build_topology_provider",
    "create_production_daemon",
    "relay_candidates_for_policy",
]
