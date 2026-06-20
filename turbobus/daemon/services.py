from __future__ import annotations

import logging

from ..schema import DaemonRequest, DaemonResponse, PeerIdentity, TransferIntent

logger = logging.getLogger(__name__)


class DaemonRequestRouter:
    def __init__(self, daemon) -> None:
        self._daemon = daemon

    def handle_request(
        self,
        request: DaemonRequest,
        *,
        connection_id: str | None = None,
    ) -> DaemonResponse:
        logger.info("开始路由 daemon 请求")

        if self._daemon._requires_authenticated_peer_for_request(request):
            logger.info("daemon 请求在分发前被拒绝")
            from . import peer_auth

            return peer_auth.authenticated_peer_required_response(request.peer_identity)

        try:
            response = self._daemon._handle_request_impl(
                request,
                connection_id=connection_id,
            )
        except (KeyError, TypeError, ValueError) as exc:
            response = DaemonResponse(ok=False, error=f"invalid request: {exc}")

        logger.info("daemon 请求路由完成: ok=%s", response.ok)
        return response


class DaemonTransferLifecycleService:
    def __init__(self, daemon) -> None:
        self._daemon = daemon

    def submit_transfer_intent(
        self,
        intent: TransferIntent,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        logger.info("开始提交 daemon transfer intent")

        response = self._daemon._submit_transfer_intent_impl(
            intent,
            peer_identity=peer_identity,
        )
        logger.info("daemon transfer intent 提交完成: ok=%s", response.ok)
        return response

    def wait_transfer_receipt(
        self,
        intent_id: str,
        *,
        timeout_seconds: float | None = None,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        logger.info("开始等待 daemon transfer receipt")

        response = self._daemon._wait_transfer_receipt_impl(
            intent_id,
            timeout_seconds=timeout_seconds,
            peer_identity=peer_identity,
        )
        logger.info("daemon transfer receipt 等待完成: ok=%s", response.ok)
        return response

    def recover_transfer_state(
        self,
        *,
        intent_id: str | None = None,
        transfer_id: str | None = None,
        peer_identity: PeerIdentity | None = None,
    ) -> DaemonResponse:
        logger.info("开始恢复 daemon transfer state")

        response = self._daemon._recover_transfer_state_impl(
            intent_id=intent_id,
            transfer_id=transfer_id,
            peer_identity=peer_identity,
        )
        logger.info("daemon transfer state 恢复完成: ok=%s", response.ok)
        return response


__all__ = [
    "DaemonRequestRouter",
    "DaemonTransferLifecycleService",
]
