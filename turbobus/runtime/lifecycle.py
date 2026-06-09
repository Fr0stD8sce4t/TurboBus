from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import fields

from ..buffer_registration import ExecutableBuffer
from ..client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer
from ..schema import DaemonResponse, TransferReceipt


def receipt_from_daemon_response(
    response: DaemonResponse,
    *,
    expected_intent_id: str,
) -> TransferReceipt:
    if not isinstance(response, DaemonResponse):
        raise TypeError("daemon response must be a DaemonResponse")
    if not response.ok:
        raise RuntimeError(response.error or "daemon receipt wait failed")
    receipt_payload = response.payload.get("receipt")
    if not isinstance(receipt_payload, Mapping):
        raise ValueError("daemon response missing receipt")
    receipt = transfer_receipt_from_payload(receipt_payload)
    if receipt.intent_id != str(expected_intent_id):
        raise ValueError("daemon receipt intent_id does not match request")
    return receipt


def transfer_receipt_from_payload(payload: Mapping[str, object]) -> TransferReceipt:
    names = {field.name for field in fields(TransferReceipt)}
    unknown = sorted(key for key in payload if key not in names)
    if unknown:
        raise ValueError("daemon receipt contains unknown fields: " + ", ".join(unknown))
    return TransferReceipt(**dict(payload))


def runtime_buffer_retention_evidence(
    *,
    buffer_id: str,
    buffer: ExecutableBuffer | None,
    reason: str,
    runtime_owned: bool,
    local_cpu_cleanup: Mapping[str, object] | None,
    lifecycle_record: Mapping[str, object] | None,
) -> dict[str, object]:
    retention = {
        "buffer_id": str(buffer_id),
        "reason": str(reason),
        "runtime_owned": bool(runtime_owned),
        "runtime_lifecycle_pool": True,
    }
    if lifecycle_record is not None:
        retention["runtime_buffer_lifecycle"] = copy_buffer_lifecycle_record(
            lifecycle_record
        )
    if isinstance(buffer, SharedPinnedCpuBuffer):
        retention["runtime_buffer_kind"] = "shared_pinned_cpu"
        if local_cpu_cleanup is not None:
            retention["local_cpu_buffer_cleanup"] = dict(local_cpu_cleanup)
            if bool(local_cpu_cleanup.get("runtime_owned", False)):
                retention["owned_cpu_buffer_release"] = dict(local_cpu_cleanup)
    elif isinstance(buffer, CudaIpcDeviceBuffer):
        retention["runtime_buffer_kind"] = "cuda_ipc_device"
    else:
        retention["runtime_buffer_kind"] = "unknown"
    return retention


def owned_cpu_release_records(
    cleanup_records: Iterable[Mapping[str, object]],
) -> list[dict[str, object]]:
    return [
        dict(record)
        for record in cleanup_records
        if isinstance(record, Mapping) and bool(record.get("runtime_owned", False))
    ]


def copy_buffer_lifecycle_record(record: Mapping[str, object]) -> dict[str, object]:
    copied: dict[str, object] = {}
    for key, value in record.items():
        if isinstance(value, Mapping):
            copied[str(key)] = copy_lifecycle_mapping(value)
        elif isinstance(value, (tuple, list)):
            copied[str(key)] = tuple(copy_lifecycle_sequence(value))
        else:
            copied[str(key)] = value
    return copied


def copy_lifecycle_mapping(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        return {}
    copied: dict[str, object] = {}
    for key, item in value.items():
        if isinstance(item, Mapping):
            copied[str(key)] = copy_lifecycle_mapping(item)
        elif isinstance(item, (tuple, list)):
            copied[str(key)] = tuple(copy_lifecycle_sequence(item))
        else:
            copied[str(key)] = item
    return copied


def copy_lifecycle_sequence(value: object) -> list[object]:
    if not isinstance(value, (tuple, list)):
        return []
    copied: list[object] = []
    for item in value:
        if isinstance(item, Mapping):
            copied.append(copy_lifecycle_mapping(item))
        elif isinstance(item, (tuple, list)):
            copied.append(tuple(copy_lifecycle_sequence(item)))
        else:
            copied.append(item)
    return copied


__all__ = [
    "copy_buffer_lifecycle_record",
    "copy_lifecycle_mapping",
    "copy_lifecycle_sequence",
    "owned_cpu_release_records",
    "receipt_from_daemon_response",
    "runtime_buffer_retention_evidence",
    "transfer_receipt_from_payload",
]
