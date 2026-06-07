from __future__ import annotations

import json
import os
import socket
import threading
from dataclasses import asdict

from ..schema import (
    DaemonRequest,
    DaemonResponse,
    RequestType,
    TransferIntent,
    WorkerTransferAuthorizationRequest,
)


class _DaemonSocketClientBase:
    def __init__(self, socket_path: str) -> None:
        self.socket_path = str(socket_path)

    def send(self, request: DaemonRequest) -> DaemonResponse:
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            timeout = _daemon_socket_timeout()
            if timeout is not None:
                client.settimeout(timeout)
            client.connect(self.socket_path)
            client.sendall((json.dumps(asdict(request)) + "\n").encode("utf-8"))
            data = b""
            while True:
                chunk = client.recv(65536)
                if not chunk:
                    break
                data += chunk
                if b"\n" in data:
                    break
        finally:
            client.close()

        response_data = json.loads(data.decode("utf-8"))
        return DaemonResponse(
            ok=bool(response_data["ok"]),
            payload=response_data.get("payload", {}),
            error=response_data.get("error"),
        )


class TurboBusDaemonClient(_DaemonSocketClientBase):
    def submit_transfer_intent(self, intent: TransferIntent) -> DaemonResponse:
        if not isinstance(intent, TransferIntent):
            raise TypeError("intent must be a TransferIntent")
        return self.send(
            DaemonRequest(
                request_type=RequestType.SUBMIT_TRANSFER_INTENT,
                session_id=intent.session_id,
                payload={"intent": asdict(intent)},
            )
        )

    def wait_transfer_receipt(
        self,
        intent_id: str,
        timeout_seconds: float | None = None,
    ) -> DaemonResponse:
        payload: dict[str, object] = {"intent_id": str(intent_id)}
        if timeout_seconds is not None:
            payload["timeout_seconds"] = float(timeout_seconds)
        return self.send(
            DaemonRequest(
                request_type=RequestType.WAIT_TRANSFER_RECEIPT,
                payload=payload,
            )
        )


class TurboBusDaemonRuntimeClient(_DaemonSocketClientBase):
    def register_session(
        self,
        target_gpu: int,
        max_inflight_chunks: int = 8,
        connection_scoped: bool = False,
        worker_relay_capable: bool = False,
    ) -> DaemonResponse:
        payload = {
            "target_gpu": int(target_gpu),
            "max_inflight_chunks": int(max_inflight_chunks),
            "worker_relay_capable": bool(worker_relay_capable),
        }
        if connection_scoped:
            payload["connection_scoped"] = True
        return self.send(
            DaemonRequest(
                request_type=RequestType.REGISTER_SESSION,
                payload=payload,
            )
        )

    def close_session(self, session_id: str) -> DaemonResponse:
        return self.send(
            DaemonRequest(
                request_type=RequestType.CLOSE_SESSION,
                session_id=str(session_id),
            )
        )

    def register_job(
        self,
        job_id: str,
        user_id: str | None = None,
        session_id: str | None = None,
        container_id: str | None = None,
        process_id: int | None = None,
        weight: float = 1.0,
    ) -> DaemonResponse:
        payload: dict[str, object] = {"job_id": str(job_id)}
        if user_id is not None:
            payload["user_id"] = str(user_id)
        if session_id is not None:
            payload["session_id"] = str(session_id)
        if container_id is not None:
            payload["container_id"] = str(container_id)
        if process_id is not None:
            payload["process_id"] = int(process_id)
        payload["weight"] = float(weight)
        return self.send(
            DaemonRequest(
                request_type=RequestType.REGISTER_JOB,
                payload=payload,
            )
        )

    def register_buffer(
        self,
        buffer_id: str,
        job_id: str,
        kind: str,
        size_bytes: int,
        device_index: int | None = None,
        address: int | None = None,
        pinned: bool = False,
        handle_type: str = "registered_buffer",
        metadata: dict[str, object] | None = None,
    ) -> DaemonResponse:
        payload: dict[str, object] = {
            "buffer_id": str(buffer_id),
            "job_id": str(job_id),
            "kind": str(kind),
            "size_bytes": int(size_bytes),
            "pinned": bool(pinned),
            "handle_type": str(handle_type),
        }
        if device_index is not None:
            payload["device_index"] = int(device_index)
        if address is not None:
            payload["address"] = int(address)
        if metadata is not None:
            payload["metadata"] = dict(metadata)
        return self.send(
            DaemonRequest(
                request_type=RequestType.REGISTER_BUFFER,
                payload=payload,
            )
        )


class TurboBusPersistentDaemonRuntimeClient(TurboBusDaemonRuntimeClient):
    persistent_connection = True

    def __init__(self, socket_path: str) -> None:
        super().__init__(socket_path)
        self._lock = threading.Lock()
        self._client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        timeout = _daemon_socket_timeout()
        if timeout is not None:
            self._client.settimeout(timeout)
        self._client.connect(self.socket_path)
        self._recv_buffer = b""
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._client.close()
            self._recv_buffer = b""

    def send(self, request: DaemonRequest) -> DaemonResponse:
        with self._lock:
            if self._closed:
                raise RuntimeError("persistent daemon runtime client is closed")
            self._client.sendall((json.dumps(asdict(request)) + "\n").encode("utf-8"))
            while b"\n" not in self._recv_buffer:
                chunk = self._client.recv(65536)
                if not chunk:
                    self._closed = True
                    raise RuntimeError(
                        "persistent daemon runtime connection closed before response"
                    )
                self._recv_buffer += chunk
            line, _, self._recv_buffer = self._recv_buffer.partition(b"\n")
        response_data = json.loads(line.decode("utf-8"))
        return DaemonResponse(
            ok=bool(response_data["ok"]),
            payload=response_data.get("payload", {}),
            error=response_data.get("error"),
        )


def _daemon_socket_timeout() -> float | None:
    raw = os.environ.get("TURBOBUS_DAEMON_SOCKET_TIMEOUT_SECONDS")
    if raw is None or not raw.strip():
        return None
    timeout = float(raw)
    if timeout <= 0:
        raise ValueError("TURBOBUS_DAEMON_SOCKET_TIMEOUT_SECONDS must be positive")
    return timeout


class TurboBusDaemonAdminClient(_DaemonSocketClientBase):
    def describe(self) -> DaemonResponse:
        return self.send(DaemonRequest(request_type=RequestType.PROFILE))

    def get_inventory(self) -> DaemonResponse:
        return self.send(
            DaemonRequest(
                request_type=RequestType.GET_INVENTORY,
            )
        )

    def invalidate_topology(self) -> DaemonResponse:
        return self.send(
            DaemonRequest(
                request_type=RequestType.INVALIDATE_TOPOLOGY,
            )
        )

    def reap_expired_leases(self, now: float | None = None) -> DaemonResponse:
        payload: dict[str, object] = {}
        if now is not None:
            payload["now"] = float(now)
        return self.send(
            DaemonRequest(
                request_type=RequestType.REAP_EXPIRED_LEASES,
                payload=payload,
            )
        )


class TurboBusDaemonProfileClient(_DaemonSocketClientBase):
    def discover_relays(
        self,
        target_gpu: int | None = None,
    ) -> DaemonResponse:
        payload: dict[str, object] = {}
        if target_gpu is not None:
            payload["target_gpu"] = int(target_gpu)
        return self.send(
            DaemonRequest(
                request_type=RequestType.DISCOVER_RELAYS,
                payload=payload,
            )
        )

    def get_profile(self, target_gpu: int, relay_gpus: list[int]) -> DaemonResponse:
        return self.send(
            DaemonRequest(
                request_type=RequestType.GET_PROFILE,
                payload={
                    "target_gpu": int(target_gpu),
                    "relay_gpus": [int(gpu) for gpu in relay_gpus],
                },
            )
        )

    def put_profile(
        self,
        target_gpu: int,
        relay_gpus: list[int],
        profile: dict,
        profile_bytes: int = 0,
    ) -> DaemonResponse:
        return self.send(
            DaemonRequest(
                request_type=RequestType.PUT_PROFILE,
                payload={
                    "target_gpu": int(target_gpu),
                    "relay_gpus": [int(gpu) for gpu in relay_gpus],
                    "profile": profile,
                    "profile_bytes": int(profile_bytes),
                },
            )
        )

    def invalidate_profile(self, target_gpu: int, relay_gpus: list[int]) -> DaemonResponse:
        return self.send(
            DaemonRequest(
                request_type=RequestType.INVALIDATE_PROFILE,
                payload={
                    "target_gpu": int(target_gpu),
                    "relay_gpus": [int(gpu) for gpu in relay_gpus],
                },
            )
        )


class TurboBusDaemonExecutionClient(_DaemonSocketClientBase):
    def cleanup(
        self,
        target_kind: str,
        target_id: str,
        reason: str = "manual",
        force: bool = False,
        owner_binding: dict[str, object] | None = None,
        retention_evidence: dict[str, object] | None = None,
    ) -> DaemonResponse:
        payload = {
            "target_kind": str(target_kind),
            "target_id": str(target_id),
            "reason": str(reason),
            "force": bool(force),
        }
        if owner_binding is not None:
            payload["owner_binding"] = dict(owner_binding)
        if retention_evidence is not None:
            payload["retention_evidence"] = dict(retention_evidence)
        return self.send(
            DaemonRequest(
                request_type=RequestType.CLEANUP,
                payload=payload,
            )
        )

    def transfer_status(
        self,
        transfer_id: str,
        state: str | None = None,
        bytes_completed: int | None = None,
        error: str | None = None,
        completion_source: str | None = None,
        completion_evidence: dict[str, object] | None = None,
    ) -> DaemonResponse:
        payload: dict[str, object] = {"transfer_id": str(transfer_id)}
        if state is not None:
            payload["state"] = str(state)
        if bytes_completed is not None:
            payload["bytes_completed"] = int(bytes_completed)
        if error is not None:
            payload["error"] = str(error)
        if completion_source is not None:
            payload["completion_source"] = str(completion_source)
        if completion_evidence is not None:
            payload["completion_evidence"] = dict(completion_evidence)
        return self.send(
            DaemonRequest(
                request_type=RequestType.TRANSFER_STATUS,
                payload=payload,
            )
        )

    def validate_lease(
        self,
        lease_id: str,
        token: str,
        session_id: str | None = None,
        relay_gpu: int | None = None,
        job_id: str | None = None,
        buffer_ids: list[str] | None = None,
    ) -> DaemonResponse:
        payload: dict[str, object] = {
            "lease_id": str(lease_id),
            "token": str(token),
        }
        if session_id is not None:
            payload["session_id"] = str(session_id)
        if relay_gpu is not None:
            payload["relay_gpu"] = int(relay_gpu)
        if job_id is not None:
            payload["job_id"] = str(job_id)
        if buffer_ids is not None:
            payload["buffer_ids"] = [str(buffer_id) for buffer_id in buffer_ids]
        return self.send(
            DaemonRequest(
                request_type=RequestType.VALIDATE_LEASE,
                payload=payload,
            )
        )

    def authorize_worker_transfer(
        self,
        request: WorkerTransferAuthorizationRequest,
    ) -> DaemonResponse:
        return self.send(
            DaemonRequest(
                request_type=RequestType.AUTHORIZE_WORKER_TRANSFER,
                payload=asdict(request),
            )
        )
