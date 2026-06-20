from __future__ import annotations

from dataclasses import asdict, dataclass
import os
import shutil
import stat

DEFAULT_SOCKET_MODE = 0o600


class SocketSecurityError(RuntimeError):
    pass


@dataclass(frozen=True)
class UnixSocketSecurityPolicy:
    mode: int = DEFAULT_SOCKET_MODE
    group: str | None = None
    allow_world_access: bool = False

    def __post_init__(self) -> None:
        mode = int(self.mode)
        if mode < 0:
            raise ValueError("socket mode must be non-negative")
        object.__setattr__(self, "mode", mode)
        if self.group is not None:
            group = str(self.group).strip()
            if not group:
                raise ValueError("socket group must be non-empty")
            object.__setattr__(self, "group", group)

    def as_dict(self) -> dict[str, object]:
        record = asdict(self)
        record["mode_octal"] = oct(int(self.mode))
        return record


@dataclass(frozen=True)
class UnixSocketSecurityRecord:
    path: str
    mode: int
    uid: int | None
    gid: int | None
    group: str | None
    source: str = "unix_socket_security"

    def as_dict(self) -> dict[str, object]:
        record = asdict(self)
        record["mode_octal"] = oct(int(self.mode))
        return record


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


def secure_unix_socket(
    path: str,
    policy: UnixSocketSecurityPolicy | int | None = None,
    *,
    mode: int = DEFAULT_SOCKET_MODE,
) -> UnixSocketSecurityRecord:
    resolved_policy = _resolve_socket_security_policy(policy, mode=mode)
    if os.name != "posix":
        return UnixSocketSecurityRecord(
            path=str(path),
            mode=int(resolved_policy.mode),
            uid=None,
            gid=None,
            group=resolved_policy.group,
            source="unix_socket_security_unsupported_platform",
        )
    socket_path = str(path)
    _validate_socket_path(socket_path)
    _validate_socket_mode(resolved_policy)
    try:
        if resolved_policy.group is not None:
            shutil.chown(socket_path, group=resolved_policy.group)
        os.chmod(socket_path, int(resolved_policy.mode))
    except OSError as exc:
        raise SocketSecurityError(
            f"failed to secure Unix socket permissions: {socket_path}"
        ) from exc
    stat_result = os.stat(socket_path)
    return UnixSocketSecurityRecord(
        path=socket_path,
        mode=stat.S_IMODE(stat_result.st_mode),
        uid=int(stat_result.st_uid),
        gid=int(stat_result.st_gid),
        group=resolved_policy.group,
    )


def _resolve_socket_security_policy(
    policy: UnixSocketSecurityPolicy | int | None,
    *,
    mode: int,
) -> UnixSocketSecurityPolicy:
    if isinstance(policy, UnixSocketSecurityPolicy):
        return policy
    if policy is None:
        return UnixSocketSecurityPolicy(mode=mode)
    return UnixSocketSecurityPolicy(mode=int(policy))


def _validate_socket_path(socket_path: str) -> None:
    try:
        mode = os.stat(socket_path).st_mode
    except OSError as exc:
        raise SocketSecurityError(
            f"failed to inspect socket path: {socket_path}"
        ) from exc
    if not stat.S_ISSOCK(mode):
        raise SocketSecurityError(f"refusing to secure non-socket path: {socket_path}")


def _validate_socket_mode(policy: UnixSocketSecurityPolicy) -> None:
    mode = int(policy.mode)
    if mode & 0o007:
        raise SocketSecurityError("refusing world-accessible TurboBus socket mode")
