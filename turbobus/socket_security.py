from __future__ import annotations

import os
import stat

DEFAULT_SOCKET_MODE = 0o600


class SocketSecurityError(RuntimeError):
    pass


def unlink_stale_socket(path: str) -> None:
    socket_path = str(path)
    if not os.path.exists(socket_path):
        return
    if os.name == "posix":
        try:
            mode = os.stat(socket_path).st_mode
        except OSError as exc:
            raise SocketSecurityError(
                f"failed to inspect existing socket path: {socket_path}"
            ) from exc
        if not stat.S_ISSOCK(mode):
            raise SocketSecurityError(
                f"refusing to unlink non-socket path: {socket_path}"
            )
    os.unlink(socket_path)


def secure_unix_socket(path: str, mode: int = DEFAULT_SOCKET_MODE) -> None:
    if os.name != "posix":
        return
    socket_path = str(path)
    try:
        os.chmod(socket_path, int(mode))
    except OSError as exc:
        raise SocketSecurityError(
            f"failed to secure Unix socket permissions: {socket_path}"
        ) from exc
