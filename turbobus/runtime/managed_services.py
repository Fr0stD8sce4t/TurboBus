from __future__ import annotations

import os
import socket
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import fields

from ..daemon.client import TurboBusDaemonProfileClient
from ..intent_execution_support import require_ok
from ..runtime_options import RuntimeOptions
from ..worker.process import run_worker_service_process


class ManagedProductionStartupError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        evidence: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.evidence = None if evidence is None else dict(evidence)


def _managed_service_startup_context() -> tuple[
    dict[str, dict[str, object]],
    threading.Lock,
]:
    return {}, threading.Lock()


def run_managed_daemon_service(
    *,
    daemon,
    socket_path: str,
    stop_event: threading.Event,
    startup_records: dict[str, dict[str, object]],
    startup_lock: threading.Lock,
) -> None:
    try:
        daemon.serve_forever(
            socket_path=str(socket_path),
            stop_event=stop_event,
        )
    except Exception as exc:
        update_managed_service_startup_record(
            startup_records,
            startup_lock,
            "daemon",
            state="failed",
            error=str(exc) or exc.__class__.__name__,
            error_type=exc.__class__.__name__,
        )
        raise
    if not stop_event.is_set():
        update_managed_service_startup_record(
            startup_records,
            startup_lock,
            "daemon",
            state="stopped",
            error="managed daemon service exited before runtime shutdown",
            error_type="UnexpectedExit",
        )


def run_managed_worker_service(
    *,
    daemon_socket_path: str,
    worker_socket_path: str,
    stop_event: threading.Event,
    backend,
    runtime_options: RuntimeOptions,
    startup_records: dict[str, dict[str, object]],
    startup_lock: threading.Lock,
) -> None:
    def report_startup(record: dict[str, object]) -> None:
        payload = dict(record)
        payload.pop("service", None)
        update_managed_service_startup_record(
            startup_records,
            startup_lock,
            "worker",
            **payload,
        )

    run_worker_service_process(
        daemon_socket_path,
        worker_socket_path,
        stop_event=stop_event,
        backend=backend,
        runtime_options=runtime_options,
        startup_reporter=report_startup,
    )
    if not stop_event.is_set():
        update_managed_service_startup_record(
            startup_records,
            startup_lock,
            "worker",
            state="stopped",
            error="managed worker service exited before runtime shutdown",
            error_type="UnexpectedExit",
        )


def update_managed_service_startup_record(
    startup_records: dict[str, dict[str, object]],
    startup_lock: threading.Lock,
    service: str,
    **updates,
) -> None:
    with startup_lock:
        existing = dict(startup_records.get(service, {}))
        startup_evidence = updates.get("startup_evidence")
        if isinstance(existing.get("startup_evidence"), Mapping) and not isinstance(
            startup_evidence,
            Mapping,
        ):
            updates["startup_evidence"] = dict(existing["startup_evidence"])
        record = {
            **existing,
            **updates,
            "service": str(service),
        }
        startup_records[str(service)] = record


def managed_service_startup_snapshot(
    startup_records: dict[str, dict[str, object]],
    startup_lock: threading.Lock,
) -> dict[str, object]:
    with startup_lock:
        services = {
            name: dict(record)
            for name, record in startup_records.items()
        }
    return {
        "services": services,
    }


def managed_service_failure_record(
    startup_records: dict[str, dict[str, object]],
    startup_lock: threading.Lock,
    service: str,
) -> dict[str, object] | None:
    with startup_lock:
        record = startup_records.get(str(service))
        if not isinstance(record, Mapping):
            return None
        state = str(record.get("state", "")).lower()
        if state not in {"failed", "stopped"}:
            return None
        return dict(record)


def shutdown_managed_service_threads(
    *,
    daemon_stop_event: threading.Event | None,
    daemon_thread: threading.Thread | None,
    daemon_socket_path: str | None,
    worker_stop_event: threading.Event | None,
    worker_thread: threading.Thread | None,
    worker_socket_path: str | None,
) -> list[dict[str, object]]:
    evidence: list[dict[str, object]] = []
    if worker_stop_event is not None:
        worker_stop_event.set()
    if daemon_stop_event is not None:
        daemon_stop_event.set()
    if worker_thread is not None:
        worker_thread.join(timeout=1.0)
        evidence.append(
            {
                "service": "worker",
                "socket_path": worker_socket_path,
                "alive_after_join": worker_thread.is_alive(),
            }
        )
    if daemon_thread is not None:
        daemon_thread.join(timeout=1.0)
        evidence.append(
            {
                "service": "daemon",
                "socket_path": daemon_socket_path,
                "alive_after_join": daemon_thread.is_alive(),
            }
        )
    return evidence


def managed_startup_error(
    exc: Exception,
    *,
    startup_evidence: Mapping[str, object],
    shutdown_evidence: Sequence[Mapping[str, object]],
) -> ManagedProductionStartupError:
    message = str(exc) or exc.__class__.__name__
    existing_evidence = (
        dict(exc.evidence)
        if isinstance(exc, ManagedProductionStartupError)
        and isinstance(exc.evidence, Mapping)
        else {}
    )
    startup_payload = dict(existing_evidence.get("startup", {}))
    startup_payload.update(dict(startup_evidence))
    shutdown_payload = [
        dict(item)
        for item in existing_evidence.get("shutdown", ())
        if isinstance(item, Mapping)
    ]
    shutdown_payload.extend(dict(item) for item in shutdown_evidence)
    return ManagedProductionStartupError(
        message,
        evidence={
            "startup": startup_payload,
            "shutdown": shutdown_payload,
        },
    )


def _managed_startup_failure(
    exc: Exception,
    *,
    startup_records: dict[str, dict[str, object]],
    startup_lock: threading.Lock,
    daemon_stop_event: threading.Event | None,
    daemon_thread: threading.Thread | None,
    daemon_socket_path: str | None,
    worker_stop_event: threading.Event | None,
    worker_thread: threading.Thread | None,
    worker_socket_path: str | None,
) -> ManagedProductionStartupError:
    shutdown_evidence = shutdown_managed_service_threads(
        daemon_stop_event=daemon_stop_event,
        daemon_thread=daemon_thread,
        daemon_socket_path=daemon_socket_path,
        worker_stop_event=worker_stop_event,
        worker_thread=worker_thread,
        worker_socket_path=worker_socket_path,
    )
    startup_snapshot = managed_service_startup_snapshot(
        startup_records,
        startup_lock,
    )
    return managed_startup_error(
        exc,
        startup_evidence=startup_snapshot,
        shutdown_evidence=shutdown_evidence,
    )


def attach_runtime_managed_service_state(
    session,
    *,
    startup_records: dict[str, dict[str, object]],
    startup_lock: threading.Lock,
    startup_evidence: Mapping[str, object] | None,
    daemon_socket_path: str | None,
    daemon_stop_event: threading.Event | None,
    daemon_thread: threading.Thread | None,
    worker_socket_path: str | None,
    worker_stop_event: threading.Event | None,
    worker_thread: threading.Thread | None,
    runtime_control_owned: bool,
) -> None:
    session._managed_service_startup_evidence = (
        None if startup_evidence is None else dict(startup_evidence)
    )
    session._managed_service_records = startup_records
    session._managed_service_lock = startup_lock
    session._owned_daemon_socket_path = daemon_socket_path
    session._owned_daemon_stop_event = daemon_stop_event
    session._owned_daemon_thread = daemon_thread
    session._owned_worker_socket_path = worker_socket_path
    session._owned_worker_stop_event = worker_stop_event
    session._owned_worker_thread = worker_thread
    session._runtime_control_connection_owned = bool(runtime_control_owned)


def wait_for_managed_services_ready(
    *,
    daemon_socket_path: str,
    worker_socket_path: str,
    startup_records: dict[str, dict[str, object]] | None = None,
    startup_lock: threading.Lock | None = None,
    timeout_seconds: float = 5.0,
    poll_interval_seconds: float = 0.05,
) -> dict[str, object]:
    wait_for_daemon_socket_ready(
        daemon_socket_path=daemon_socket_path,
        startup_records=startup_records,
        startup_lock=startup_lock,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )
    wait_for_worker_socket_ready(
        worker_socket_path=worker_socket_path,
        startup_records=startup_records,
        startup_lock=startup_lock,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )
    if startup_records is None or startup_lock is None:
        return {}
    return managed_service_startup_snapshot(startup_records, startup_lock)


def managed_service_failure_record_or_none(
    startup_records: dict[str, dict[str, object]] | None,
    startup_lock: threading.Lock | None,
    service: str,
) -> dict[str, object] | None:
    if startup_records is None or startup_lock is None:
        return None
    return managed_service_failure_record(startup_records, startup_lock, service)


def update_managed_service_startup_record_if_available(
    startup_records: dict[str, dict[str, object]] | None,
    startup_lock: threading.Lock | None,
    service: str,
    **updates,
) -> None:
    if startup_records is None or startup_lock is None:
        return
    update_managed_service_startup_record(
        startup_records,
        startup_lock,
        service,
        **updates,
    )


def managed_service_runtime_snapshot(
    *,
    startup_records: dict[str, dict[str, object]] | None,
    startup_lock: threading.Lock | None,
    daemon_thread: threading.Thread | None,
    daemon_stop_event: threading.Event | None,
    daemon_socket_path: str | None,
    worker_thread: threading.Thread | None,
    worker_stop_event: threading.Event | None,
    worker_socket_path: str | None,
    runtime_control_owned: bool,
    runtime_client: object | None,
) -> dict[str, object] | None:
    if (
        startup_records is None
        and startup_lock is None
        and daemon_thread is None
        and daemon_stop_event is None
        and worker_thread is None
        and worker_stop_event is None
        and daemon_socket_path is None
        and worker_socket_path is None
        and not runtime_control_owned
        and runtime_client is None
    ):
        return None
    snapshot = (
        {"services": {}}
        if startup_records is None or startup_lock is None
        else managed_service_startup_snapshot(startup_records, startup_lock)
    )
    services = (
        dict(snapshot.get("services", {}))
        if isinstance(snapshot.get("services"), Mapping)
        else {}
    )
    for service_name, thread, stop_event, socket_path in (
        ("daemon", daemon_thread, daemon_stop_event, daemon_socket_path),
        ("worker", worker_thread, worker_stop_event, worker_socket_path),
    ):
        record = (
            dict(services.get(service_name, {}))
            if isinstance(services.get(service_name), Mapping)
            else {}
        )
        present = (
            thread is not None
            or stop_event is not None
            or socket_path is not None
            or bool(record)
        )
        if not present:
            continue
        owned = (
            bool(record.get("owned", False))
            or thread is not None
            or stop_event is not None
        )
        record["service"] = str(service_name)
        record["owned"] = owned
        if thread is not None:
            record["thread_alive"] = bool(thread.is_alive())
        elif owned and "thread_alive" not in record:
            record["thread_alive"] = False
        if stop_event is not None:
            record["stop_requested"] = bool(stop_event.is_set())
        else:
            record["stop_requested"] = bool(record.get("stop_requested", False))
        if socket_path is not None:
            record["socket_path"] = str(socket_path)
            record["socket_exists"] = os.path.exists(str(socket_path))
        elif "socket_path" in record:
            record["socket_exists"] = os.path.exists(str(record["socket_path"]))
        services[service_name] = record
    snapshot["services"] = services
    snapshot["runtime_control"] = {
        "owned": bool(runtime_control_owned),
        "client_type": (
            None if runtime_client is None else runtime_client.__class__.__name__
        ),
        "closed": bool(getattr(runtime_client, "closed", False)),
    }
    return snapshot


def runtime_options_with_socket_paths(
    options: RuntimeOptions,
    *,
    daemon_socket_path: str,
    worker_socket_path: str,
) -> RuntimeOptions:
    values = {
        field.name: getattr(options, field.name)
        for field in fields(RuntimeOptions)
    }
    values["daemon_socket_path"] = str(daemon_socket_path)
    values["worker_socket_path"] = str(worker_socket_path)
    return RuntimeOptions(**values)


def runtime_options_with_optional_socket_paths(
    options: RuntimeOptions,
    *,
    daemon_socket_path: str,
    worker_socket_path: str | None,
) -> RuntimeOptions:
    values = {
        field.name: getattr(options, field.name)
        for field in fields(RuntimeOptions)
    }
    values["daemon_socket_path"] = str(daemon_socket_path)
    values["worker_socket_path"] = (
        None if worker_socket_path is None else str(worker_socket_path)
    )
    return RuntimeOptions(**values)


def bootstrap_attached_runtime_services(
    *,
    daemon_socket_path: str,
    worker_socket_path: str,
    backend,
    runtime_options: RuntimeOptions,
) -> tuple[
    dict[str, dict[str, object]],
    threading.Lock,
    dict[str, object],
    threading.Event | None,
    threading.Thread | None,
]:
    startup_records, startup_lock = _managed_service_startup_context()
    daemon_path = str(daemon_socket_path)
    worker_path = str(worker_socket_path)
    update_managed_service_startup_record(
        startup_records,
        startup_lock,
        "daemon",
        state="attaching",
        owned=False,
        socket_path=daemon_path,
    )
    worker_stop_event: threading.Event | None = None
    worker_thread: threading.Thread | None = None
    try:
        wait_for_daemon_socket_ready(
            daemon_socket_path=daemon_path,
            startup_records=startup_records,
            startup_lock=startup_lock,
        )
        worker_stop_event, worker_thread = _attach_or_start_worker_service(
            daemon_socket_path=daemon_path,
            worker_socket_path=worker_path,
            backend=backend,
            runtime_options=runtime_options,
            startup_records=startup_records,
            startup_lock=startup_lock,
        )
        startup_evidence = managed_service_startup_snapshot(
            startup_records,
            startup_lock,
        )
    except Exception as exc:
        raise _managed_startup_failure(
            exc,
            startup_records=startup_records,
            startup_lock=startup_lock,
            daemon_stop_event=None,
            daemon_thread=None,
            daemon_socket_path=daemon_path,
            worker_stop_event=worker_stop_event,
            worker_thread=worker_thread,
            worker_socket_path=worker_path,
        ) from exc
    return (
        startup_records,
        startup_lock,
        startup_evidence,
        worker_stop_event,
        worker_thread,
    )


def _attach_or_start_worker_service(
    *,
    daemon_socket_path: str,
    worker_socket_path: str,
    backend,
    runtime_options: RuntimeOptions,
    startup_records: dict[str, dict[str, object]],
    startup_lock: threading.Lock,
) -> tuple[threading.Event | None, threading.Thread | None]:
    update_managed_service_startup_record(
        startup_records,
        startup_lock,
        "worker",
        state="probing",
        owned=False,
        daemon_socket_path=daemon_socket_path,
        socket_path=worker_socket_path,
    )
    try:
        wait_for_worker_socket_ready(
            worker_socket_path=worker_socket_path,
            startup_records=startup_records,
            startup_lock=startup_lock,
            timeout_seconds=0.1,
            poll_interval_seconds=0.01,
        )
        return None, None
    except Exception:
        pass
    worker_stop_event, worker_thread = _start_managed_worker_service(
        daemon_socket_path=daemon_socket_path,
        worker_socket_path=worker_socket_path,
        backend=backend,
        runtime_options=runtime_options,
        startup_records=startup_records,
        startup_lock=startup_lock,
        owned=True,
    )
    wait_for_worker_socket_ready(
        worker_socket_path=worker_socket_path,
        startup_records=startup_records,
        startup_lock=startup_lock,
    )
    return worker_stop_event, worker_thread


def bootstrap_owned_runtime_services(
    *,
    daemon,
    daemon_socket_path: str,
    worker_socket_path: str,
    backend,
    runtime_options: RuntimeOptions,
) -> tuple[
    dict[str, dict[str, object]],
    threading.Lock,
    dict[str, object],
    threading.Event,
    threading.Thread,
    threading.Event,
    threading.Thread,
]:
    startup_records, startup_lock = _managed_service_startup_context()
    daemon_path = str(daemon_socket_path)
    worker_path = str(worker_socket_path)
    update_managed_service_startup_record(
        startup_records,
        startup_lock,
        "daemon",
        state="starting",
        owned=True,
        socket_path=daemon_path,
        require_authenticated_peers=bool(
            getattr(daemon, "_require_authenticated_peers", False)
        ),
    )
    daemon_stop_event, daemon_thread = _start_managed_daemon_service(
        daemon=daemon,
        daemon_socket_path=daemon_path,
        startup_records=startup_records,
        startup_lock=startup_lock,
    )
    worker_stop_event, worker_thread = _start_managed_worker_service(
        daemon_socket_path=daemon_path,
        worker_socket_path=worker_path,
        backend=backend,
        runtime_options=runtime_options,
        startup_records=startup_records,
        startup_lock=startup_lock,
        owned=True,
    )
    try:
        startup_evidence = wait_for_managed_services_ready(
            daemon_socket_path=daemon_path,
            worker_socket_path=worker_path,
            startup_records=startup_records,
            startup_lock=startup_lock,
        )
    except Exception as exc:
        raise _managed_startup_failure(
            exc,
            startup_records=startup_records,
            startup_lock=startup_lock,
            daemon_stop_event=daemon_stop_event,
            daemon_thread=daemon_thread,
            daemon_socket_path=daemon_path,
            worker_stop_event=worker_stop_event,
            worker_thread=worker_thread,
            worker_socket_path=worker_path,
        ) from exc
    return (
        startup_records,
        startup_lock,
        startup_evidence,
        daemon_stop_event,
        daemon_thread,
        worker_stop_event,
        worker_thread,
    )


def _start_managed_daemon_service(
    *,
    daemon,
    daemon_socket_path: str,
    startup_records: dict[str, dict[str, object]],
    startup_lock: threading.Lock,
) -> tuple[threading.Event, threading.Thread]:
    daemon_stop_event = threading.Event()
    daemon_thread = threading.Thread(
        target=run_managed_daemon_service,
        kwargs={
            "daemon": daemon,
            "socket_path": daemon_socket_path,
            "stop_event": daemon_stop_event,
            "startup_records": startup_records,
            "startup_lock": startup_lock,
        },
        name="turbobus-daemon-service",
        daemon=True,
    )
    daemon_thread.start()
    return daemon_stop_event, daemon_thread


def _start_managed_worker_service(
    *,
    daemon_socket_path: str,
    worker_socket_path: str,
    backend,
    runtime_options: RuntimeOptions,
    startup_records: dict[str, dict[str, object]],
    startup_lock: threading.Lock,
    owned: bool,
) -> tuple[threading.Event, threading.Thread]:
    update_managed_service_startup_record(
        startup_records,
        startup_lock,
        "worker",
        state="starting",
        owned=bool(owned),
        daemon_socket_path=daemon_socket_path,
        socket_path=worker_socket_path,
    )
    worker_stop_event = threading.Event()
    worker_thread = threading.Thread(
        target=run_managed_worker_service,
        kwargs={
            "daemon_socket_path": daemon_socket_path,
            "worker_socket_path": worker_socket_path,
            "stop_event": worker_stop_event,
            "backend": backend,
            "runtime_options": runtime_options,
            "startup_records": startup_records,
            "startup_lock": startup_lock,
        },
        name="turbobus-worker-service",
        daemon=True,
    )
    worker_thread.start()
    return worker_stop_event, worker_thread


def wait_for_daemon_socket_ready(
    *,
    daemon_socket_path: str,
    startup_records: dict[str, dict[str, object]] | None = None,
    startup_lock: threading.Lock | None = None,
    timeout_seconds: float = 5.0,
    poll_interval_seconds: float = 0.05,
) -> None:
    deadline = time.time() + max(0.1, float(timeout_seconds))
    daemon_error: Exception | None = None
    while time.time() < deadline:
        daemon_failure = managed_service_failure_record_or_none(
            startup_records,
            startup_lock,
            "daemon",
        )
        if daemon_failure is not None:
            raise ManagedProductionStartupError(
                "managed daemon service failed before startup completed",
                evidence={"startup": {"services": {"daemon": daemon_failure}}},
            )
        try:
            daemon_ready = TurboBusDaemonProfileClient(str(daemon_socket_path))
            require_ok(daemon_ready.discover_relays(), "daemon startup probe failed")
            update_managed_service_startup_record_if_available(
                startup_records,
                startup_lock,
                "daemon",
                state="ready",
                ready_at=time.time(),
                ready_probe="discover_relays",
                socket_path=str(daemon_socket_path),
            )
            return
        except Exception as exc:
            daemon_error = exc
            time.sleep(max(0.001, float(poll_interval_seconds)))
    raise RuntimeError(
        f"managed daemon socket did not become ready: {daemon_error}"
    ) from daemon_error


def wait_for_worker_socket_ready(
    *,
    worker_socket_path: str,
    startup_records: dict[str, dict[str, object]] | None = None,
    startup_lock: threading.Lock | None = None,
    timeout_seconds: float = 5.0,
    poll_interval_seconds: float = 0.05,
) -> None:
    deadline = time.time() + max(0.1, float(timeout_seconds))
    worker_error: Exception | None = None
    while time.time() < deadline:
        worker_failure = managed_service_failure_record_or_none(
            startup_records,
            startup_lock,
            "worker",
        )
        if worker_failure is not None:
            raise ManagedProductionStartupError(
                "managed worker service failed before startup completed",
                evidence={"startup": {"services": {"worker": worker_failure}}},
            )
        try:
            _probe_worker_socket_ready(str(worker_socket_path))
            update_managed_service_startup_record_if_available(
                startup_records,
                startup_lock,
                "worker",
                state="ready",
                ready_at=time.time(),
                ready_probe="worker_socket_connect",
                socket_path=str(worker_socket_path),
            )
            return
        except Exception as exc:
            worker_error = exc
            time.sleep(max(0.001, float(poll_interval_seconds)))
    raise RuntimeError(
        f"managed worker socket did not become ready: {worker_error}"
    ) from worker_error


def _probe_worker_socket_ready(socket_path: str) -> None:
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        client.connect(str(socket_path))
    finally:
        client.close()


__all__ = [
    "ManagedProductionStartupError",
    "attach_runtime_managed_service_state",
    "bootstrap_attached_runtime_services",
    "bootstrap_owned_runtime_services",
    "managed_service_runtime_snapshot",
    "managed_service_startup_snapshot",
    "managed_startup_error",
    "run_managed_daemon_service",
    "run_managed_worker_service",
    "runtime_options_with_optional_socket_paths",
    "runtime_options_with_socket_paths",
    "shutdown_managed_service_threads",
    "update_managed_service_startup_record",
    "wait_for_daemon_socket_ready",
    "wait_for_managed_services_ready",
    "wait_for_worker_socket_ready",
]
