from __future__ import annotations

import time
from collections.abc import Mapping

from ..buffer_registration import ExecutableBuffer
from ..schema import DaemonResponse, TransferIntent, TransferReceipt
from .buffers import (
    runtime_buffer_lifecycle_intent_use,
    runtime_buffer_lifecycle_registration,
)
from .lifecycle import copy_lifecycle_mapping, copy_lifecycle_sequence
from .validation import validate_runtime_receipt


def record_buffer_lifecycle_registration(
    records: dict[str, dict[str, object]],
    buffer: ExecutableBuffer,
    *,
    session_id: str,
    runtime_owned: bool,
    registered_at: float | None = None,
) -> None:
    normalized_id = str(buffer.buffer_id)
    existing = records.get(normalized_id, {})
    registration_count = int(existing.get("registration_count", 0) or 0) + 1
    record = runtime_buffer_lifecycle_registration(
        buffer,
        session_id=session_id,
        runtime_owned=runtime_owned,
        registered_at=time.time() if registered_at is None else float(registered_at),
        registration_count=registration_count,
    )
    active_uses = copy_lifecycle_mapping(existing.get("active_uses"))
    terminal_uses = tuple(copy_lifecycle_sequence(existing.get("terminal_uses")))
    cleanup_history = tuple(copy_lifecycle_sequence(existing.get("cleanup_history")))
    if active_uses:
        record["active_uses"] = active_uses
    if terminal_uses:
        record["terminal_uses"] = terminal_uses
    if cleanup_history:
        record["cleanup_history"] = cleanup_history
    records[normalized_id] = record


def record_buffer_lifecycle_intent_use(
    records: dict[str, dict[str, object]],
    intent: TransferIntent,
    *,
    now: float | None = None,
) -> None:
    timestamp = time.time() if now is None else float(now)
    roles = (
        ("source", intent.source_buffer_id),
        ("destination", intent.destination_buffer_id),
    )
    for role, buffer_id in roles:
        normalized_id = str(buffer_id)
        record = records.setdefault(
            normalized_id,
            {
                "buffer_id": normalized_id,
                "job_id": str(intent.job_id),
                "session_id": str(intent.session_id),
                "state": "referenced",
            },
        )
        active_uses = copy_lifecycle_mapping(record.get("active_uses"))
        active_uses[str(intent.intent_id)] = runtime_buffer_lifecycle_intent_use(
            intent,
            buffer_id=normalized_id,
            role=role,
        )
        record["active_uses"] = active_uses
        record["active_use_count"] = len(active_uses)
        record["last_used_at"] = timestamp
        if str(record.get("state", "")) != "cleaned":
            record["state"] = "active"


def record_buffer_lifecycle_receipt(
    records: dict[str, dict[str, object]],
    submitted_intent_buffers: Mapping[str, tuple[str, str]],
    receipt: TransferReceipt,
    *,
    intent_id: str,
) -> None:
    buffer_ids = submitted_intent_buffers.get(str(intent_id))
    if buffer_ids is None:
        return
    state_text = str(getattr(receipt.state, "value", receipt.state))
    terminal_states = {"complete", "failed", "canceled"}
    terminal = state_text.lower() in terminal_states
    for buffer_id in buffer_ids:
        record = records.get(str(buffer_id))
        if record is None:
            continue
        active_uses = copy_lifecycle_mapping(record.get("active_uses"))
        use_record = active_uses.pop(str(intent_id), None)
        if use_record is None:
            use_record = {
                "intent_id": str(intent_id),
                "buffer_id": str(buffer_id),
                "state": "unknown",
            }
        use_record["state"] = state_text
        use_record["bytes_completed"] = int(receipt.bytes_completed)
        use_record["bytes_total"] = int(receipt.bytes_total)
        if receipt.completed_at is not None:
            use_record["completed_at"] = float(receipt.completed_at)
        if receipt.error is not None:
            use_record["error"] = str(receipt.error)
        terminal_uses = list(copy_lifecycle_sequence(record.get("terminal_uses")))
        if terminal:
            terminal_uses.append(use_record)
        else:
            active_uses[str(intent_id)] = use_record
        record["active_uses"] = active_uses
        record["terminal_uses"] = tuple(terminal_uses)
        record["active_use_count"] = len(active_uses)
        record["terminal_use_count"] = len(terminal_uses)
        if active_uses:
            record["state"] = "active"
        elif str(record.get("state", "")) != "cleaned":
            record["state"] = "registered"


def record_buffer_lifecycle_cleanup(
    records: dict[str, dict[str, object]],
    buffer_id: str,
    *,
    reason: str,
    ok: bool,
    daemon_response: DaemonResponse,
    local_cpu_cleanup: Mapping[str, object] | None,
    retention_evidence: Mapping[str, object] | None,
    error: Exception | None = None,
) -> None:
    normalized_id = str(buffer_id)
    record = records.setdefault(
        normalized_id,
        {"buffer_id": normalized_id},
    )
    cleanup_record: dict[str, object] = {
        "buffer_id": normalized_id,
        "reason": str(reason),
        "ok": bool(ok),
        "cleaned_at": time.time(),
    }
    if daemon_response.error is not None:
        cleanup_record["daemon_error"] = daemon_response.error
    if isinstance(daemon_response.payload, Mapping):
        cleanup_record["daemon_payload"] = dict(daemon_response.payload)
    if local_cpu_cleanup is not None:
        cleanup_record["local_cpu_buffer_cleanup"] = dict(local_cpu_cleanup)
    if retention_evidence is not None:
        cleanup_record["runtime_buffer_retention"] = dict(retention_evidence)
    if error is not None:
        cleanup_record["error"] = str(error) or error.__class__.__name__
    cleanup_history = list(copy_lifecycle_sequence(record.get("cleanup_history")))
    cleanup_history.append(cleanup_record)
    record["cleanup_history"] = tuple(cleanup_history)
    record["last_cleanup"] = dict(cleanup_record)
    record["state"] = "cleaned" if ok else "cleanup_failed"


def finalize_runtime_receipt(
    receipt: TransferReceipt,
    *,
    intent_id: str,
    job_id: str,
    session_id: str,
    submitted_intent_buffers: Mapping[str, tuple[str, str]],
    active_intent_ids: set[str],
    record_buffer_lifecycle_receipt_fn,
) -> TransferReceipt:
    normalized_intent_id = str(intent_id)
    buffer_ids = submitted_intent_buffers.get(normalized_intent_id)
    validate_runtime_receipt(
        receipt,
        intent_id=normalized_intent_id,
        job_id=job_id,
        session_id=session_id,
        source_buffer_id=(None if buffer_ids is None else buffer_ids[0]),
        destination_buffer_id=(None if buffer_ids is None else buffer_ids[1]),
    )
    terminal_states = {"complete", "failed", "canceled"}
    state_text = str(getattr(receipt.state, "value", receipt.state)).lower()
    if state_text in terminal_states:
        active_intent_ids.discard(normalized_intent_id)
    record_buffer_lifecycle_receipt_fn(receipt, intent_id=normalized_intent_id)
    return receipt


def close_active_intent_receipts(
    active_intent_ids: set[str],
    wait_transfer_receipt_fn,
) -> list[dict[str, object]]:
    evidence: list[dict[str, object]] = []
    for intent_id in tuple(active_intent_ids):
        try:
            receipt = wait_transfer_receipt_fn(intent_id, timeout_seconds=0.0)
            evidence.append(
                {
                    "intent_id": str(intent_id),
                    "ok": True,
                    "state": str(getattr(receipt.state, "value", receipt.state)),
                    "bytes_completed": int(receipt.bytes_completed),
                }
            )
        except Exception as exc:
            evidence.append(
                {
                    "intent_id": str(intent_id),
                    "ok": False,
                    "error": str(exc) or exc.__class__.__name__,
                }
            )
    return evidence


def recover_active_intent_receipts(
    active_intent_ids: set[str],
    recover_transfer_state_fn,
) -> list[dict[str, object]]:
    evidence: list[dict[str, object]] = []
    for intent_id in tuple(active_intent_ids):
        try:
            recovery = recover_transfer_state_fn(intent_id=str(intent_id))
            receipt = recovery.get("receipt") if isinstance(recovery, Mapping) else None
            record = {
                "intent_id": str(intent_id),
                "ok": True,
                "source": "daemon_authoritative_transfer_recovery",
            }
            if isinstance(receipt, Mapping):
                record["state"] = str(receipt.get("state"))
                record["bytes_completed"] = int(receipt.get("bytes_completed", 0) or 0)
                record["bytes_total"] = int(receipt.get("bytes_total", 0) or 0)
            if isinstance(recovery, Mapping):
                transfer_id = recovery.get("transfer_id")
                if transfer_id is not None:
                    record["transfer_id"] = str(transfer_id)
                record["archived"] = bool(recovery.get("archived", False))
            evidence.append(record)
        except Exception as exc:
            evidence.append(
                {
                    "intent_id": str(intent_id),
                    "ok": False,
                    "source": "daemon_authoritative_transfer_recovery",
                    "error": str(exc) or exc.__class__.__name__,
                }
            )
    return evidence


__all__ = [
    "close_active_intent_receipts",
    "finalize_runtime_receipt",
    "record_buffer_lifecycle_cleanup",
    "record_buffer_lifecycle_intent_use",
    "record_buffer_lifecycle_receipt",
    "record_buffer_lifecycle_registration",
    "recover_active_intent_receipts",
]
