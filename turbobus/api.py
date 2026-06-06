from __future__ import annotations

from dataclasses import fields
from typing import Mapping

from .daemon.client import TurboBusDaemonClient
from .intent_execution_support import require_ok
from .schema import DaemonResponse, TransferIntent, TransferReceipt, TransferStatusState


class TurboBusClient:
    """Compatibility receipt client.

    Production `TransferIntent` execution must go through
    `TurboBusRuntimeSession`, which owns daemon-issued execution and receipt
    validation. This client remains only as a terminal-receipt consumption
    boundary for older non-production-facing surfaces.
    """

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
        raise RuntimeError(
            "TurboBusClient is not a production transfer submission API; use "
            "TurboBusRuntimeSession to submit daemon-issued TransferIntent execution"
        )

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
        receipt = _receipt_from_daemon_response(
            response,
            expected_intent_id=str(intent_id),
        )
        _require_terminal_receipt(
            receipt,
            operation="wait_transfer_receipt",
        )
        return receipt


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


def _require_terminal_receipt(
    receipt: TransferReceipt,
    *,
    operation: str,
) -> None:
    state = TransferStatusState(receipt.state)
    if state in {
        TransferStatusState.COMPLETE,
        TransferStatusState.FAILED,
        TransferStatusState.CANCELED,
    }:
        return
    raise RuntimeError(
        "TurboBusClient cannot complete production transfer execution from a "
        f"non-terminal daemon receipt during {operation}; use "
        "TurboBusRuntimeSession for daemon-issued execution"
    )


__all__ = ["TurboBusClient"]
