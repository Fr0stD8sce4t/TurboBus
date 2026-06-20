from __future__ import annotations

import socket
import struct

from ..schema import DaemonResponse, PeerIdentity


def validate_unix_socket_support(*, require_authenticated_peers: bool) -> None:
    if not hasattr(socket, "AF_UNIX"):
        raise RuntimeError("Unix domain sockets are unavailable on this platform")
    if require_authenticated_peers and not hasattr(socket, "SO_PEERCRED"):
        raise RuntimeError(
            "authenticated Unix peer credentials are required but SO_PEERCRED "
            "is unavailable on this platform"
        )


def authenticated_peer_required_response(
    peer_identity: PeerIdentity | None,
) -> DaemonResponse:
    reason = "peer identity is unavailable"
    if peer_identity is not None:
        reason = peer_identity.unsupported_reason or "peer identity is unauthenticated"
    return DaemonResponse(
        ok=False,
        error=f"authenticated peer credentials are required: {reason}",
    )


def peer_uid(peer_identity: PeerIdentity | None) -> str | None:
    if peer_identity is None or not peer_identity.authenticated:
        return None
    if peer_identity.user_id is None:
        return None
    return str(peer_identity.user_id)


def peer_gid(peer_identity: PeerIdentity | None) -> int | None:
    if peer_identity is None or not peer_identity.authenticated:
        return None
    return None if peer_identity.group_id is None else int(peer_identity.group_id)


def require_authenticated_peer(peer_identity: PeerIdentity | None) -> PeerIdentity:
    if peer_identity is None or not peer_identity.authenticated:
        reason = "peer identity is unavailable"
        if peer_identity is not None:
            reason = peer_identity.unsupported_reason or "peer identity is unauthenticated"
        raise ValueError(f"authenticated peer credentials are required: {reason}")
    return peer_identity


def bind_job_identity_to_peer(
    *,
    user_id: str | None,
    process_id: int | None,
    container_id: str | None,
    peer_identity: PeerIdentity | None,
) -> tuple[str | None, int | None, str | None]:
    if peer_identity is None or not peer_identity.authenticated:
        return user_id, process_id, container_id
    if user_id is not None and str(user_id) != str(peer_identity.user_id):
        raise ValueError("job user_id does not match authenticated peer")
    if (
        process_id is not None
        and peer_identity.process_id is not None
        and int(process_id) != int(peer_identity.process_id)
    ):
        raise ValueError("job process_id does not match authenticated peer")
    if (
        container_id is not None
        and peer_identity.container_id is not None
        and str(container_id) != str(peer_identity.container_id)
    ):
        raise ValueError("job container_id does not match authenticated peer")
    return (
        str(peer_identity.user_id),
        peer_identity.process_id if process_id is None else int(process_id),
        peer_identity.container_id if container_id is None else str(container_id),
    )


def validate_peer_owner_match(
    *,
    expected: PeerIdentity | None,
    actual: PeerIdentity | None,
    owner_name: str,
) -> None:
    if expected is None or actual is None:
        return
    if not expected.authenticated or not actual.authenticated:
        return
    if str(expected.user_id) != str(actual.user_id):
        raise ValueError(f"{owner_name} owner does not match authenticated peer")


def peer_identity_same_connection(
    expected: PeerIdentity | None,
    actual: PeerIdentity | None,
) -> bool:
    if expected is None or actual is None:
        return False
    if expected.authenticated and actual.authenticated:
        return (
            str(expected.user_id) == str(actual.user_id)
            and expected.process_id == actual.process_id
            and expected.group_id == actual.group_id
        )
    return (
        expected.authenticated == actual.authenticated
        and expected.source == actual.source
        and expected.unsupported_reason == actual.unsupported_reason
    )


def peer_identity_from_socket(conn: socket.socket) -> PeerIdentity:
    if hasattr(socket, "SO_PEERCRED"):
        try:
            credentials = conn.getsockopt(
                socket.SOL_SOCKET,
                socket.SO_PEERCRED,
                struct.calcsize("3i"),
            )
            pid, uid, gid = struct.unpack("3i", credentials)
            return PeerIdentity(
                authenticated=True,
                source="unix_socket_peercred",
                user_id=str(uid),
                process_id=pid,
                group_id=gid,
            )
        except OSError as exc:
            return PeerIdentity(
                authenticated=False,
                source="unix_socket_peercred",
                unsupported_reason=str(exc),
            )
    return PeerIdentity(
        authenticated=False,
        source="unix_socket",
        unsupported_reason="SO_PEERCRED is unavailable on this platform",
    )


__all__ = [
    "authenticated_peer_required_response",
    "bind_job_identity_to_peer",
    "peer_gid",
    "peer_identity_from_socket",
    "peer_identity_same_connection",
    "peer_uid",
    "require_authenticated_peer",
    "validate_peer_owner_match",
    "validate_unix_socket_support",
]
