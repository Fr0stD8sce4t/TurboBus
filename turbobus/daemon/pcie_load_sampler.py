from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import asdict, dataclass
import subprocess
import time

from ..topology.bandwidth_model import PcieEdgeLoad


@dataclass(frozen=True)
class HardwarePcieCounter:
    device_id: int
    rx_mib_s: float
    tx_mib_s: float
    sampled_at: float
    source: str = "nvidia_smi_dmon"
    known: bool = True
    error: str | None = None

    @property
    def sample_age_ms(self) -> float:
        return max(0.0, (time.time() - float(self.sampled_at)) * 1000.0)

    @property
    def h2d_used_gbps(self) -> float:
        return _mib_s_to_gbps(self.tx_mib_s)

    @property
    def d2h_used_gbps(self) -> float:
        return _mib_s_to_gbps(self.rx_mib_s)

    def as_dict(self) -> dict[str, object]:
        record = asdict(self)
        record["sample_age_ms"] = self.sample_age_ms
        record["h2d_used_gbps"] = self.h2d_used_gbps
        record["d2h_used_gbps"] = self.d2h_used_gbps
        return record


@dataclass(frozen=True)
class HardwarePcieSample:
    counters: tuple[HardwarePcieCounter, ...] = ()
    sampled_at: float = 0.0
    source: str = "nvidia_smi_dmon"
    known: bool = False
    error: str | None = None

    @property
    def sample_age_ms(self) -> float:
        if self.sampled_at <= 0.0:
            return 0.0
        return max(0.0, (time.time() - float(self.sampled_at)) * 1000.0)

    def by_device(self) -> dict[int, HardwarePcieCounter]:
        return {int(counter.device_id): counter for counter in self.counters}

    def as_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "known": bool(self.known),
            "sampled_at": float(self.sampled_at),
            "sample_age_ms": self.sample_age_ms,
            "error": self.error,
            "counters": [counter.as_dict() for counter in self.counters],
        }


@dataclass(frozen=True)
class HardwarePcieSamplerConfig:
    executable: str = "nvidia-smi"
    timeout_seconds: float = 1.0


@dataclass(frozen=True)
class NvidiaSmiPcieLoadSampler:
    config: HardwarePcieSamplerConfig = HardwarePcieSamplerConfig()

    def sample(self) -> HardwarePcieSample:
        sampled_at = time.time()
        try:
            completed = subprocess.run(
                [
                    self.config.executable,
                    "dmon",
                    "-s",
                    "t",
                    "-c",
                    "2",
                ],
                capture_output=True,
                check=False,
                text=True,
                timeout=float(self.config.timeout_seconds),
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return HardwarePcieSample(
                sampled_at=sampled_at,
                known=False,
                error=str(exc) or exc.__class__.__name__,
            )
        if completed.returncode != 0:
            return HardwarePcieSample(
                sampled_at=sampled_at,
                known=False,
                error=(completed.stderr or completed.stdout or "").strip()
                or f"nvidia-smi exited with {completed.returncode}",
            )
        try:
            counters = _parse_nvidia_smi_dmon(completed.stdout, sampled_at=sampled_at)
        except ValueError as exc:
            return HardwarePcieSample(
                sampled_at=sampled_at,
                known=False,
                error=str(exc),
            )
        return HardwarePcieSample(
            counters=tuple(counters),
            sampled_at=sampled_at,
            known=bool(counters),
            error=None if counters else "nvidia-smi dmon returned no PCIe samples",
        )


def pcie_load_from_active_paths(
    *,
    active_paths: object,
    path_edge_map: Mapping[int, tuple[str, ...]],
    capacity_by_edge: Mapping[str, float],
) -> dict[str, dict[str, object]]:
    bytes_by_edge: dict[str, dict[str, int]] = defaultdict(
        lambda: {"h2d": 0, "d2h": 0}
    )
    for record in _mapping_records(active_paths):
        direction = str(record.get("direction", "h2d")).lower()
        if direction not in {"h2d", "d2h"}:
            direction = "h2d"
        device = _path_pcie_device(record)
        if device is None:
            continue
        for edge_id in path_edge_map.get(int(device), ()):
            bytes_by_edge[str(edge_id)][direction] += int(
                record.get("bytes_total", 0) or 0
            )
    loads: dict[str, dict[str, object]] = {}
    max_bytes = max(
        [sum(direction_bytes.values()) for direction_bytes in bytes_by_edge.values()]
        or [0],
    )
    for edge_id, direction_bytes in bytes_by_edge.items():
        capacity = max(0.0, float(capacity_by_edge.get(edge_id, 0.0) or 0.0))
        if capacity <= 0.0 or max_bytes <= 0:
            load = PcieEdgeLoad(edge_id=edge_id, source="daemon_active_paths", known=False)
        else:
            scale = min(1.0, sum(direction_bytes.values()) / max(max_bytes, 1))
            h2d_share = direction_bytes["h2d"] / max(sum(direction_bytes.values()), 1)
            d2h_share = direction_bytes["d2h"] / max(sum(direction_bytes.values()), 1)
            load = PcieEdgeLoad(
                edge_id=edge_id,
                h2d_used_gbps=capacity * scale * h2d_share,
                d2h_used_gbps=capacity * scale * d2h_share,
                source="daemon_active_paths",
                known=True,
            )
        loads[edge_id] = load.as_dict()
    return loads


def _path_pcie_device(record: Mapping[str, object]) -> int | None:
    kind = str(record.get("kind", "")).lower()
    if kind == "relay" and record.get("relay_device") is not None:
        return int(record["relay_device"])
    if record.get("target_device") is not None:
        return int(record["target_device"])
    return None


def _mapping_records(value: object) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list | tuple):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _parse_nvidia_smi_dmon(
    output: str,
    *,
    sampled_at: float,
) -> tuple[HardwarePcieCounter, ...]:
    header: list[str] | None = None
    samples: list[list[str]] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            columns = line.lstrip("#").split()
            if "gpu" in columns and "rxpci" in columns and "txpci" in columns:
                header = columns
            continue
        if header is not None:
            values = line.split()
            if len(values) >= len(header):
                samples.append(values)
    if header is None:
        raise ValueError("nvidia-smi dmon output missing gpu/rxpci/txpci columns")
    if not samples:
        raise ValueError("nvidia-smi dmon output missing sample rows")
    selected_by_gpu: dict[int, list[str]] = {}
    seen_count: dict[int, int] = defaultdict(int)
    gpu_index = header.index("gpu")
    rx_index = header.index("rxpci")
    tx_index = header.index("txpci")
    for values in samples:
        try:
            gpu = int(values[gpu_index])
        except (TypeError, ValueError):
            continue
        seen_count[gpu] += 1
        if seen_count[gpu] >= 2 or gpu not in selected_by_gpu:
            selected_by_gpu[gpu] = values
    counters = []
    for gpu, values in sorted(selected_by_gpu.items()):
        try:
            counters.append(
                HardwarePcieCounter(
                    device_id=gpu,
                    rx_mib_s=float(values[rx_index]),
                    tx_mib_s=float(values[tx_index]),
                    sampled_at=sampled_at,
                )
            )
        except (TypeError, ValueError):
            continue
    if not counters:
        raise ValueError("nvidia-smi dmon output did not contain numeric PCIe samples")
    return tuple(counters)


def _mib_s_to_gbps(value: float) -> float:
    return max(0.0, float(value)) * 8.0 / 1024.0


__all__ = [
    "HardwarePcieCounter",
    "HardwarePcieSample",
    "HardwarePcieSamplerConfig",
    "NvidiaSmiPcieLoadSampler",
    "pcie_load_from_active_paths",
]
