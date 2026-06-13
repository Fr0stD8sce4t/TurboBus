from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence

from . import native_runtime
from .plan_trace import transfer_plan_to_dict


def native_ranges(
    ranges: Iterable,
    source_bytes: int,
    destination_bytes: int,
) -> list:
    native = native_runtime.native_module()
    converted = []
    for item in ranges:
        src_offset, dst_offset, bytes_ = range_fields(item)
        if src_offset < 0 or dst_offset < 0 or bytes_ <= 0:
            raise ValueError("range offsets must be non-negative and bytes must be positive")
        if src_offset + bytes_ > source_bytes:
            raise ValueError("range source extends past source tensor")
        if dst_offset + bytes_ > destination_bytes:
            raise ValueError("range destination extends past destination tensor")
        transfer_range = native.TransferRange()
        transfer_range.src_offset = int(src_offset)
        transfer_range.dst_offset = int(dst_offset)
        transfer_range.bytes = int(bytes_)
        converted.append(transfer_range)
    if not converted:
        raise ValueError("at least one non-empty range is required")
    return converted


def native_transfer_plan(plan):
    native = native_runtime.native_module()
    payload = dict(plan) if isinstance(plan, Mapping) else transfer_plan_to_dict(plan)
    native_plan = native.TransferPlan()
    native_plan.total_bytes = int(payload.get("total_bytes", 0))
    native_plan.chunk_bytes = int(payload.get("chunk_bytes", default_chunk_bytes()))
    if native_plan.total_bytes < 0:
        raise ValueError("transfer plan total_bytes must be non-negative")
    if native_plan.chunk_bytes <= 0:
        raise ValueError("transfer plan chunk_bytes must be positive")

    assignments = []
    chunk_total_bytes = 0
    plan_direction: str | None = None
    for assignment_payload in payload.get("assignments", []) or []:
        if not isinstance(assignment_payload, Mapping):
            raise ValueError("transfer plan assignment must be an object")
        path_payload = assignment_payload.get("path")
        if not isinstance(path_payload, Mapping):
            raise ValueError("transfer plan assignment path must be an object")
        if not bool(path_payload.get("enabled", True)):
            raise ValueError("transfer plan contains a disabled path")

        native_assignment = native.PathAssignment()
        native_path = native.Path()
        direction = str(path_payload.get("direction", "h2d")).lower()
        kind = str(path_payload.get("kind", "")).lower()
        plan_direction = _validate_assignment_path(
            kind=kind,
            direction=direction,
            relay_device=int(path_payload.get("relay_device", -1)),
            plan_direction=plan_direction,
        )
        _set_native_field(native_path, "kind", _native_path_kind(native, kind, direction))
        _set_native_field(
            native_path,
            "direction",
            _native_transfer_direction(native, direction),
        )
        native_path.target_device = int(path_payload.get("target_device", 0))
        native_path.relay_device = int(path_payload.get("relay_device", -1))
        native_path.h2d_bw_gbps = float(path_payload.get("h2d_bw_gbps", 0.0) or 0.0)
        native_path.d2h_bw_gbps = float(path_payload.get("d2h_bw_gbps", 0.0) or 0.0)
        native_path.p2p_bw_gbps = float(path_payload.get("p2p_bw_gbps", 0.0) or 0.0)
        native_path.effective_bw_gbps = float(
            path_payload.get("effective_bw_gbps", 0.0) or 0.0
        )
        native_path.enabled = True

        chunks = []
        assignment_chunk_bytes = 0
        for chunk_payload in assignment_payload.get("chunks", []) or []:
            if not isinstance(chunk_payload, Mapping):
                raise ValueError("transfer plan chunk must be an object")
            src_offset = int(chunk_payload.get("src_offset", 0))
            dst_offset = int(chunk_payload.get("dst_offset", 0))
            bytes_ = int(chunk_payload.get("bytes", 0))
            if src_offset < 0 or dst_offset < 0 or bytes_ <= 0:
                raise ValueError(
                    "transfer plan chunk offsets must be non-negative and bytes positive"
                )
            native_chunk = native.Chunk()
            native_chunk.src_offset = src_offset
            native_chunk.dst_offset = dst_offset
            native_chunk.bytes = bytes_
            chunks.append(native_chunk)
            assignment_chunk_bytes += bytes_
            chunk_total_bytes += bytes_
        if not chunks:
            continue
        _validate_assignment_totals(
            assignment_payload,
            chunk_bytes=assignment_chunk_bytes,
            chunk_count=len(chunks),
        )
        native_assignment.path = native_path
        native_assignment.chunks = chunks
        assignments.append(native_assignment)

    if native_plan.total_bytes > 0 and not assignments:
        raise ValueError("transfer plan has no chunk assignments")
    if assignments and chunk_total_bytes != native_plan.total_bytes:
        raise ValueError(
            "transfer plan total_bytes must match assigned chunk bytes"
        )
    native_plan.assignments = assignments
    return native_plan


def default_chunk_bytes() -> int:
    return 16 * 1024 * 1024


def range_fields(item) -> tuple[int, int, int]:
    if isinstance(item, Mapping):
        return int(item["src_offset"]), int(item["dst_offset"]), int(item["bytes"])
    if isinstance(item, Sequence) and not isinstance(item, (str, bytes, bytearray)):
        if len(item) != 3:
            raise ValueError("range tuples must be (src_offset, dst_offset, bytes)")
        return int(item[0]), int(item[1]), int(item[2])
    src_offset = getattr(item, "src_offset")
    dst_offset = getattr(item, "dst_offset")
    bytes_ = getattr(item, "bytes")
    return int(src_offset), int(dst_offset), int(bytes_)


def _native_path_kind(native, kind: str, direction: str):
    path_kind = getattr(native, "PathKind", None)
    if path_kind is None:
        raise RuntimeError("native extension does not expose PathKind")
    if kind == "direct" and direction == "h2d":
        return path_kind.DirectH2D
    if kind == "relay" and direction == "h2d":
        return path_kind.RelayH2DThenP2P
    if kind == "direct" and direction == "d2h":
        return path_kind.DirectD2H
    if kind == "relay" and direction == "d2h":
        return path_kind.RelayP2PThenD2H
    raise ValueError(f"unsupported transfer plan path: {kind}/{direction}")


def _native_transfer_direction(native, direction: str):
    transfer_direction = getattr(native, "TransferDirection", None)
    if transfer_direction is None:
        raise RuntimeError("native extension does not expose TransferDirection")
    if direction == "h2d":
        return transfer_direction.H2D
    if direction == "d2h":
        return transfer_direction.D2H
    raise ValueError(f"unsupported transfer plan direction: {direction}")


def _validate_assignment_path(
    *,
    kind: str,
    direction: str,
    relay_device: int,
    plan_direction: str | None,
) -> str:
    if direction not in {"h2d", "d2h"}:
        raise ValueError(f"unsupported transfer plan direction: {direction}")
    if plan_direction is not None and direction != plan_direction:
        raise ValueError("transfer plan assignments must use one direction")
    if kind == "direct":
        if relay_device >= 0:
            raise ValueError("direct transfer plan path must not set relay_device")
    elif kind == "relay":
        if relay_device < 0:
            raise ValueError("relay transfer plan path requires relay_device")
    else:
        raise ValueError(f"unsupported transfer plan path kind: {kind}")
    return direction


def _validate_assignment_totals(
    assignment_payload: Mapping[str, object],
    *,
    chunk_bytes: int,
    chunk_count: int,
) -> None:
    declared_bytes = assignment_payload.get("bytes")
    if declared_bytes is not None and int(declared_bytes) != int(chunk_bytes):
        raise ValueError("transfer plan assignment bytes must match chunk bytes")
    declared_chunks = assignment_payload.get("chunk_count")
    if declared_chunks is not None and int(declared_chunks) != int(chunk_count):
        raise ValueError("transfer plan assignment chunk_count must match chunks")


def _set_native_field(obj, field_name: str, value) -> None:
    value_field = f"{field_name}_value"
    if hasattr(obj, value_field):
        setattr(obj, value_field, value)
        return
    setattr(obj, field_name, value)
