from __future__ import annotations

from dataclasses import dataclass


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


__all__ = ["SimpleProfileRelay", "SimpleProfileResult"]
