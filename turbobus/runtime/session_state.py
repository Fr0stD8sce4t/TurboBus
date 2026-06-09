from __future__ import annotations

from ..runtime_options import RuntimeOptions


def normalize_runtime_session_config(session) -> None:
    job_id = str(session.job_id)
    if not job_id.strip():
        raise ValueError("job_id must be non-empty")
    session.job_id = job_id
    if session.user_id is not None:
        session.user_id = str(session.user_id)
    session.max_inflight_chunks = int(session.max_inflight_chunks)
    if session.max_inflight_chunks <= 0:
        raise ValueError("max_inflight_chunks must be positive")
    if session.runtime_options is None:
        session.runtime_options = RuntimeOptions()
    if not isinstance(session.runtime_options, RuntimeOptions):
        raise TypeError("runtime_options must be a RuntimeOptions")


def resolve_runtime_role_clients(
    session,
    *,
    daemon_client_factory,
    runtime_client_factory,
    execution_client_factory,
    profile_client_factory,
    worker_client_factory,
) -> None:
    daemon_client = getattr(session, "daemon_client", None)
    socket_path = getattr(daemon_client, "socket_path", None)
    runtime_options = getattr(session, "runtime_options", None)
    if socket_path is None and runtime_options is not None:
        socket_path = getattr(runtime_options, "daemon_socket_path", None)
    if session.daemon_client is None:
        if socket_path is None:
            raise ValueError("daemon_client is required without daemon_socket_path")
        session.daemon_client = daemon_client_factory(str(socket_path))
        socket_path = getattr(session.daemon_client, "socket_path", socket_path)
    if session.runtime_daemon_client is None:
        if socket_path is None:
            raise ValueError("runtime_daemon_client is required without socket_path")
        session.runtime_daemon_client = runtime_client_factory(str(socket_path))
    if session.execution_daemon_client is None:
        if socket_path is None:
            raise ValueError("execution_daemon_client is required without socket_path")
        session.execution_daemon_client = execution_client_factory(str(socket_path))
    if session.profile_daemon_client is None:
        if socket_path is None:
            raise ValueError("profile_daemon_client is required without socket_path")
        session.profile_daemon_client = profile_client_factory(str(socket_path))
    worker_socket_path = (
        None
        if runtime_options is None
        else getattr(runtime_options, "worker_socket_path", None)
    )
    if session.worker_client is None and worker_socket_path is not None:
        session.worker_client = worker_client_factory(str(worker_socket_path))


def clear_runtime_session_state(session) -> None:
    session._session_id = None
    session._target_gpu = None
    session._relay_gpus = None
    session._client = None
    session._transfer_executor = None
    session._profile_bootstrapped = False
    profile_bootstrap_evidence = getattr(session, "_profile_bootstrap_evidence", None)
    if profile_bootstrap_evidence is not None:
        session._profile_bootstrap_evidence = None
    session._buffers.clear()
    session._registered_buffer_ids.clear()
    session._registered_buffer_fingerprints.clear()
    session._owned_cpu_buffer_ids.clear()
    session._submitted_intent_ids.clear()
    submitted_intent_buffers = getattr(session, "_submitted_intent_buffers", None)
    if submitted_intent_buffers is not None:
        submitted_intent_buffers.clear()
    active_intent_ids = getattr(session, "_active_intent_ids", None)
    if active_intent_ids is not None:
        active_intent_ids.clear()
    buffer_lifecycle_records = getattr(session, "_buffer_lifecycle_records", None)
    if buffer_lifecycle_records is not None:
        buffer_lifecycle_records.clear()


__all__ = [
    "clear_runtime_session_state",
    "normalize_runtime_session_config",
    "resolve_runtime_role_clients",
]
