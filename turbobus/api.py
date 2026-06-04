from __future__ import annotations

from dataclasses import fields
from typing import Mapping

from .daemon.client import TurboBusDaemonClient
from .intent_execution_support import require_ok
from .runtime.validation import validate_runtime_receipt
from .schema import DaemonResponse, TransferIntent, TransferReceipt


class TurboBusClient:
    def __init__(
        self,
        daemon: object | None = None,
        *,
        socket_path: str | None = None,
        daemon_socket_path: str | None = None,
    ) -> None:
        if daemon is None:
            resolved_socket_path = socket_path
            if resolved_socket_path is None:
                resolved_socket_path = daemon_socket_path
            if resolved_socket_path is None:
                raise ValueError(
                    "TurboBusClient requires a daemon object or socket_path"
                )
            daemon = TurboBusDaemonClient(str(resolved_socket_path))
        self.daemon = daemon

    def submit(self, intent: TransferIntent) -> TransferReceipt:
        return self.submit_transfer_intent(intent)

    def submit_transfer_intent(self, intent: TransferIntent) -> TransferReceipt:
        if not isinstance(intent, TransferIntent):
            raise TypeError("intent must be a TransferIntent")
        submitter = getattr(self.daemon, "submit_transfer_intent", None)
        if not callable(submitter):
            raise TypeError("daemon must support submit_transfer_intent")
        response = submitter(intent)
        receipt = _receipt_from_daemon_response(response, expected_intent_id=intent.intent_id)
        validate_runtime_receipt(
            receipt,
            intent_id=intent.intent_id,
            job_id=intent.job_id,
            session_id=intent.session_id,
        )
        return receipt

    def wait_transfer_receipt(
        self,
        intent_id: str,
        timeout_seconds: float | None = None,
    ) -> TransferReceipt:
        waiter = getattr(self.daemon, "wait_transfer_receipt", None)
        if not callable(waiter):
            raise TypeError("daemon must support wait_transfer_receipt")
        response = waiter(
            str(intent_id),
            timeout_seconds=timeout_seconds,
        )
        return _receipt_from_daemon_response(
            response,
            expected_intent_id=str(intent_id),
        )


def _receipt_from_daemon_response(
    response: DaemonResponse,
    *,
    expected_intent_id: str,
) -> TransferReceipt:
    if not isinstance(response, DaemonResponse):
        raise TypeError("daemon response must be a DaemonResponse")
    require_ok(response, "daemon request failed")
    receipt_payload = response.payload.get("receipt")
    if not isinstance(receipt_payload, Mapping):
        raise ValueError("daemon response missing receipt")
    names = {field.name for field in fields(TransferReceipt)}
    unknown = sorted(key for key in receipt_payload if key not in names)
    if unknown:
        raise ValueError("daemon receipt contains unknown fields: " + ", ".join(unknown))
    receipt = TransferReceipt(**dict(receipt_payload))
    if receipt.intent_id != str(expected_intent_id):
        raise ValueError("daemon receipt intent_id does not match request")
    return receipt


__all__ = ["TurboBusClient"]
