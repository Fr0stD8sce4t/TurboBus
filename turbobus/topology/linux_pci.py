from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from . import PciePathRecord


_PCIE_EFFECTIVE_GBPS_PER_LANE = {
    1: 0.25,
    2: 0.5,
    3: 0.985,
    4: 1.969,
    5: 3.938,
    6: 7.877,
}


@dataclass(frozen=True)
class LinuxPciDiscovery:
    sysfs_root: Path = Path("/sys/bus/pci/devices")

    def pcie_paths_for_gpu_buses(
        self,
        pci_bus_ids: dict[int, str],
    ) -> tuple[PciePathRecord, ...]:
        records = []
        for device_id, bus_id in sorted(pci_bus_ids.items()):
            path = self.pcie_path_for_bus(device_id=int(device_id), pci_bus_id=bus_id)
            if path is not None:
                records.append(path)
        return tuple(records)

    def pcie_path_for_bus(
        self,
        *,
        device_id: int,
        pci_bus_id: str,
    ) -> PciePathRecord | None:
        normalized_bus = _normalize_bus_id(pci_bus_id)
        device_path = self.sysfs_root / normalized_bus
        if not device_path.exists():
            return None
        speed_gtps = _read_float_from_speed(device_path / "current_link_speed")
        link_generation = _pcie_generation_from_speed(speed_gtps)
        link_width = _read_int(device_path / "current_link_width")
        return PciePathRecord(
            device_id=int(device_id),
            numa_node=_read_int(device_path / "numa_node"),
            root_complex=_root_complex_from_bus(normalized_bus),
            link_generation=link_generation,
            link_width=link_width,
            bandwidth_gbps=_estimate_pcie_bandwidth_gbps(
                link_generation,
                link_width,
            ),
            negotiated_speed_gtps=speed_gtps,
            switch_hierarchy=_switch_hierarchy_from_sysfs(device_path, self.sysfs_root),
            bandwidth_source=(
                None
                if link_generation is None or link_width is None
                else "estimated_from_linux_sysfs_link_speed_width"
            ),
        )


def _normalize_bus_id(value: str) -> str:
    text = str(value).strip()
    if text.count(":") == 1:
        text = f"0000:{text}"
    return text


def _root_complex_from_bus(bus_id: str) -> str:
    parts = str(bus_id).split(":")
    if len(parts) >= 2:
        return ":".join(parts[:2])
    return str(bus_id)


def _read_int(path: Path) -> int | None:
    try:
        text = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def _read_float_from_speed(path: Path) -> float | None:
    try:
        text = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    token = text.split()[0] if text.split() else ""
    try:
        return float(token)
    except ValueError:
        return None


def _pcie_generation_from_speed(speed_gtps: float | None) -> int | None:
    if speed_gtps is None:
        return None
    candidates = {
        2.5: 1,
        5.0: 2,
        8.0: 3,
        16.0: 4,
        32.0: 5,
        64.0: 6,
    }
    closest = min(candidates, key=lambda value: abs(value - float(speed_gtps)))
    if abs(closest - float(speed_gtps)) > 0.5:
        return None
    return candidates[closest]


def _estimate_pcie_bandwidth_gbps(
    link_generation: int | None,
    link_width: int | None,
) -> float:
    if link_generation is None or link_width is None:
        return 0.0
    per_lane = _PCIE_EFFECTIVE_GBPS_PER_LANE.get(int(link_generation))
    if per_lane is None or int(link_width) <= 0:
        return 0.0
    return round(per_lane * int(link_width), 3)


def _switch_hierarchy_from_sysfs(device_path: Path, sysfs_root: Path) -> tuple[str, ...]:
    try:
        resolved = device_path.resolve()
    except OSError:
        return ()
    hierarchy = []
    for parent in resolved.parents:
        if parent == sysfs_root:
            break
        name = parent.name
        if ":" in name and "." in name:
            hierarchy.append(name)
    return tuple(reversed(hierarchy))


__all__ = ["LinuxPciDiscovery"]
