from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field

from . import DaemonResourceInventory, PciePathRecord


@dataclass(frozen=True)
class PcieFabricEdge:
    edge_id: str
    device_ids: tuple[int, ...]
    root_complex: str | None
    switch_hierarchy: tuple[str, ...]
    capacity_gbps: float
    capacity_source: str | None = None

    def __post_init__(self) -> None:
        devices = tuple(sorted({int(device) for device in self.device_ids}))
        if not devices:
            raise ValueError("PCIe fabric edge requires at least one device")
        capacity = float(self.capacity_gbps)
        if capacity < 0.0:
            raise ValueError("capacity_gbps must be non-negative")
        object.__setattr__(self, "edge_id", str(self.edge_id))
        object.__setattr__(self, "device_ids", devices)
        if self.root_complex is not None:
            object.__setattr__(self, "root_complex", str(self.root_complex))
        object.__setattr__(
            self,
            "switch_hierarchy",
            tuple(str(item) for item in self.switch_hierarchy if str(item).strip()),
        )
        object.__setattr__(self, "capacity_gbps", capacity)
        if self.capacity_source is not None:
            source = str(self.capacity_source).strip()
            if not source:
                raise ValueError("capacity_source must be non-empty")
            object.__setattr__(self, "capacity_source", source)

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class PcieFabricPath:
    device_id: int
    edge_ids: tuple[str, ...]
    root_complex: str | None
    capacity_gbps: float
    capacity_source: str | None = None

    def __post_init__(self) -> None:
        device = int(self.device_id)
        if device < 0:
            raise ValueError("device_id must be non-negative")
        capacity = float(self.capacity_gbps)
        if capacity < 0.0:
            raise ValueError("capacity_gbps must be non-negative")
        object.__setattr__(self, "device_id", device)
        object.__setattr__(
            self,
            "edge_ids",
            tuple(str(edge_id) for edge_id in self.edge_ids if str(edge_id).strip()),
        )
        if self.root_complex is not None:
            object.__setattr__(self, "root_complex", str(self.root_complex))
        object.__setattr__(self, "capacity_gbps", capacity)
        if self.capacity_source is not None:
            source = str(self.capacity_source).strip()
            if not source:
                raise ValueError("capacity_source must be non-empty")
            object.__setattr__(self, "capacity_source", source)

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class PcieFabricSnapshot:
    snapshot_id: str
    source: str
    version: int
    paths: tuple[PcieFabricPath, ...] = field(default_factory=tuple)
    edges: tuple[PcieFabricEdge, ...] = field(default_factory=tuple)
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        version = int(self.version)
        if version < 0:
            raise ValueError("version must be non-negative")
        object.__setattr__(self, "snapshot_id", str(self.snapshot_id))
        object.__setattr__(self, "source", str(self.source))
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "paths", tuple(self.paths))
        object.__setattr__(self, "edges", tuple(self.edges))
        object.__setattr__(self, "metadata", dict(self.metadata))

    def as_dict(self) -> dict[str, object]:
        return {
            "snapshot_id": self.snapshot_id,
            "source": self.source,
            "version": self.version,
            "paths": tuple(path.as_dict() for path in self.paths),
            "edges": tuple(edge.as_dict() for edge in self.edges),
            "metadata": dict(self.metadata),
        }


def pcie_fabric_snapshot_from_inventory(
    inventory: DaemonResourceInventory,
) -> PcieFabricSnapshot:
    paths = tuple(
        _fabric_path_from_pcie_record(record)
        for record in inventory.pcie_paths
    )
    edge_groups: dict[tuple[object, ...], list[PciePathRecord]] = defaultdict(list)
    for record in inventory.pcie_paths:
        edge_groups[_shared_edge_key(record)].append(record)
    edges = tuple(
        _fabric_edge_from_group(key, records)
        for key, records in sorted(edge_groups.items(), key=lambda item: str(item[0]))
    )
    return PcieFabricSnapshot(
        snapshot_id=inventory.topology_snapshot_id(),
        source=inventory.source,
        version=inventory.version,
        paths=paths,
        edges=edges,
        metadata={
            "inventory_source": inventory.source,
            "inventory_snapshot_id": inventory.topology_snapshot_id(),
            "path_count": len(paths),
            "edge_count": len(edges),
        },
    )


def pcie_fabric_from_mapping(value: Mapping[str, object]) -> PcieFabricSnapshot:
    paths = tuple(
        _fabric_path_from_mapping(item)
        for item in value.get("paths", ()) or ()
        if isinstance(item, Mapping)
    )
    edges = tuple(
        _fabric_edge_from_mapping(item)
        for item in value.get("edges", ()) or ()
        if isinstance(item, Mapping)
    )
    return PcieFabricSnapshot(
        snapshot_id=str(value.get("snapshot_id", "unknown")),
        source=str(value.get("source", "unknown")),
        version=int(value.get("version", 0) or 0),
        paths=paths,
        edges=edges,
        metadata=dict(value.get("metadata", {}) or {}),
    )


def path_edge_ids_for_device(
    fabric: PcieFabricSnapshot | Mapping[str, object],
    device_id: int,
) -> tuple[str, ...]:
    snapshot = (
        fabric
        if isinstance(fabric, PcieFabricSnapshot)
        else pcie_fabric_from_mapping(fabric)
    )
    for path in snapshot.paths:
        if int(path.device_id) == int(device_id):
            return path.edge_ids
    return ()


def _fabric_path_from_pcie_record(record: PciePathRecord) -> PcieFabricPath:
    return PcieFabricPath(
        device_id=record.device_id,
        edge_ids=(_shared_edge_id(record),),
        root_complex=record.root_complex,
        capacity_gbps=record.bandwidth_gbps,
        capacity_source=record.bandwidth_source,
    )


def _fabric_edge_from_group(
    key: tuple[object, ...],
    records: Iterable[PciePathRecord],
) -> PcieFabricEdge:
    records_tuple = tuple(records)
    capacity_values = [
        float(record.bandwidth_gbps)
        for record in records_tuple
        if float(record.bandwidth_gbps) > 0.0
    ]
    capacity = min(capacity_values) if capacity_values else 0.0
    sources = sorted(
        {
            str(record.bandwidth_source)
            for record in records_tuple
            if record.bandwidth_source is not None
        }
    )
    first = records_tuple[0]
    return PcieFabricEdge(
        edge_id=_shared_edge_id(first),
        device_ids=tuple(record.device_id for record in records_tuple),
        root_complex=first.root_complex,
        switch_hierarchy=first.switch_hierarchy,
        capacity_gbps=capacity,
        capacity_source=",".join(sources) if sources else None,
    )


def _shared_edge_key(record: PciePathRecord) -> tuple[object, ...]:
    hierarchy = tuple(record.switch_hierarchy)
    if hierarchy:
        return ("switch", record.root_complex, hierarchy)
    return ("root", record.root_complex or f"device-{record.device_id}")


def _shared_edge_id(record: PciePathRecord) -> str:
    key = _shared_edge_key(record)
    if key[0] == "switch":
        hierarchy = "-".join(str(item).replace(":", "_") for item in key[2])
        root = "unknown" if key[1] is None else str(key[1]).replace(":", "_")
        return f"pcie-root-{root}-switch-{hierarchy}"
    root = str(key[1]).replace(":", "_")
    return f"pcie-root-{root}"


def _fabric_path_from_mapping(value: Mapping[str, object]) -> PcieFabricPath:
    return PcieFabricPath(
        device_id=int(value.get("device_id", 0) or 0),
        edge_ids=tuple(str(item) for item in value.get("edge_ids", ()) or ()),
        root_complex=(
            None
            if value.get("root_complex") is None
            else str(value.get("root_complex"))
        ),
        capacity_gbps=float(value.get("capacity_gbps", 0.0) or 0.0),
        capacity_source=(
            None
            if value.get("capacity_source") is None
            else str(value.get("capacity_source"))
        ),
    )


def _fabric_edge_from_mapping(value: Mapping[str, object]) -> PcieFabricEdge:
    return PcieFabricEdge(
        edge_id=str(value.get("edge_id", "")),
        device_ids=tuple(int(item) for item in value.get("device_ids", ()) or ()),
        root_complex=(
            None
            if value.get("root_complex") is None
            else str(value.get("root_complex"))
        ),
        switch_hierarchy=tuple(
            str(item) for item in value.get("switch_hierarchy", ()) or ()
        ),
        capacity_gbps=float(value.get("capacity_gbps", 0.0) or 0.0),
        capacity_source=(
            None
            if value.get("capacity_source") is None
            else str(value.get("capacity_source"))
        ),
    )


__all__ = [
    "PcieFabricEdge",
    "PcieFabricPath",
    "PcieFabricSnapshot",
    "path_edge_ids_for_device",
    "pcie_fabric_from_mapping",
    "pcie_fabric_snapshot_from_inventory",
]
