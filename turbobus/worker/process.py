from __future__ import annotations

import argparse
from collections.abc import Callable
from threading import Event
from typing import Mapping, Sequence

from ..backends.cuda import default_cuda_backend
from ..daemon.client import TurboBusDaemonAdminClient, TurboBusDaemonExecutionClient
from ..intent_execution_support import require_ok
from ..runtime_options import RuntimeOptions
from .endpoint import WorkerServiceEndpoint
from .lifecycle import (
    WorkerAsyncExecutionPool,
    WorkerTransferClient,
    WorkerTransferService,
)
from .cuda_executor import CudaWorkerExecutor
from .resources import WorkerDataPlaneResourceBinder
from .transport import WorkerServiceUnixSocketTransport


class WorkerStartupError(RuntimeError):
    pass


WorkerStartupReporter = Callable[[dict[str, object]], None]


def build_worker_service_transport(
    daemon_socket_path: str,
    socket_path: str,
    *,
    backend=default_cuda_backend,
    runtime_options: RuntimeOptions | None = None,
) -> WorkerServiceUnixSocketTransport:
    transport, _startup_evidence = _build_worker_service_runtime(
        daemon_socket_path,
        socket_path,
        backend=backend,
        runtime_options=runtime_options,
    )
    return transport


def _build_worker_service_runtime(
    daemon_socket_path: str,
    socket_path: str,
    *,
    backend=default_cuda_backend,
    runtime_options: RuntimeOptions | None = None,
) -> tuple[WorkerServiceUnixSocketTransport, dict[str, object]]:
    options = runtime_options or RuntimeOptions()
    startup_evidence = worker_startup_evidence_from_daemon(daemon_socket_path)
    daemon_client = TurboBusDaemonExecutionClient(str(daemon_socket_path))
    executor = CudaWorkerExecutor(
        backend=backend,
        options=options,
    )
    resource_binder = WorkerDataPlaneResourceBinder(backend=backend)
    execution_pool = WorkerAsyncExecutionPool(
        executor,
        resource_binder=resource_binder,
        worker_startup_evidence=startup_evidence,
        max_workers=max(1, int(options.staging_slots)),
    )
    transfer_client = WorkerTransferClient(
        daemon_client,
        executor=executor,
        resource_binder=resource_binder,
        execution_pool=execution_pool,
        worker_startup_evidence=startup_evidence,
    )
    endpoint = WorkerServiceEndpoint(
        service=WorkerTransferService(
            daemon_client,
            transfer_client=transfer_client,
        ),
    )
    return (
        WorkerServiceUnixSocketTransport(
            endpoint=endpoint,
            socket_path=str(socket_path),
        ),
        startup_evidence,
    )


def worker_startup_evidence_from_daemon(
    daemon_socket_path: str,
) -> dict[str, object]:
    admin_client = TurboBusDaemonAdminClient(str(daemon_socket_path))
    try:
        inventory_response = admin_client.get_inventory()
        require_ok(inventory_response, "daemon inventory query failed")
        describe_response = admin_client.describe()
        require_ok(describe_response, "daemon describe query failed")
    except Exception as exc:
        raise WorkerStartupError(
            f"worker startup could not bind to production daemon topology: {exc}"
        ) from exc
    inventory_payload = inventory_response.payload
    if not isinstance(inventory_payload, Mapping):
        raise WorkerStartupError("daemon inventory response payload must be a mapping")
    inventory = inventory_payload.get("inventory")
    if not isinstance(inventory, Mapping):
        raise WorkerStartupError("daemon inventory response did not include inventory")
    _reject_synthetic_inventory(inventory)
    topology_snapshot = inventory_payload.get("topology_snapshot")
    if not isinstance(topology_snapshot, Mapping):
        raise WorkerStartupError(
            "daemon inventory response did not include topology snapshot"
        )
    describe_payload = (
        describe_response.payload
        if isinstance(describe_response.payload, Mapping)
        else {}
    )
    gpus = tuple(inventory.get("gpus", ()) or ())
    pcie_paths = tuple(inventory.get("pcie_paths", ()) or ())
    fabric_links = tuple(inventory.get("fabric_links", ()) or ())
    requester_peer_identity = describe_payload.get("requester_peer_identity")
    daemon_peer_identity = (
        dict(requester_peer_identity)
        if isinstance(requester_peer_identity, Mapping)
        else None
    )
    return {
        "startup_source": "worker_process_daemon_inventory",
        "daemon_socket_path": str(daemon_socket_path),
        "topology_snapshot_id": str(
            topology_snapshot.get(
                "snapshot_id",
                inventory.get("snapshot_id", "unknown"),
            )
        ),
        "inventory_source": str(inventory.get("source", "unknown")),
        "inventory_version": int(inventory.get("version", 0) or 0),
        "inventory_discovered_at": float(inventory.get("discovered_at", 0.0) or 0.0),
        "gpu_count": len(gpus),
        "pcie_path_count": len(pcie_paths),
        "fabric_link_count": len(fabric_links),
        "require_authenticated_peers": bool(
            describe_payload.get("require_authenticated_peers", False)
        ),
        "daemon_peer_identity": daemon_peer_identity,
        "daemon_peer_authenticated": bool(
            daemon_peer_identity is not None
            and daemon_peer_identity.get("authenticated", False)
        ),
    }


def _reject_synthetic_inventory(inventory: Mapping[str, object]) -> None:
    source = str(inventory.get("source", "")).strip().lower()
    metadata = inventory.get("metadata", {})
    discovery = ""
    provider = ""
    if isinstance(metadata, Mapping):
        discovery = str(metadata.get("discovery", "")).lower()
        provider = str(metadata.get("provider", "")).lower()
    synthetic_markers = ("test_fixture", "fixture", "synthetic", "fake")
    if any(marker in source for marker in synthetic_markers):
        raise WorkerStartupError(
            "worker production startup cannot use synthetic topology inventory"
        )
    if any(marker in discovery for marker in synthetic_markers):
        raise WorkerStartupError(
            "worker production startup cannot use synthetic topology inventory"
        )
    if any(marker in provider for marker in synthetic_markers):
        raise WorkerStartupError(
            "worker production startup cannot use synthetic topology inventory"
        )


def run_worker_service_process(
    daemon_socket_path: str,
    socket_path: str,
    stop_event: Event | None = None,
    *,
    backend=default_cuda_backend,
    runtime_options: RuntimeOptions | None = None,
    startup_reporter: WorkerStartupReporter | None = None,
) -> None:
    startup_evidence: dict[str, object] | None = None
    try:
        if backend is default_cuda_backend and runtime_options is None:
            transport, startup_evidence = _build_worker_service_runtime(
                daemon_socket_path,
                socket_path,
            )
        else:
            transport, startup_evidence = _build_worker_service_runtime(
                daemon_socket_path,
                socket_path,
                backend=backend,
                runtime_options=runtime_options,
            )
        _report_worker_service_startup(
            startup_reporter,
            {
                "service": "worker",
                "state": "initialized",
                "daemon_socket_path": str(daemon_socket_path),
                "socket_path": str(socket_path),
                "startup_evidence": dict(startup_evidence),
            },
        )
        transport.serve_forever(stop_event=stop_event)
    except Exception as exc:
        _report_worker_service_startup(
            startup_reporter,
            {
                "service": "worker",
                "state": "failed",
                "daemon_socket_path": str(daemon_socket_path),
                "socket_path": str(socket_path),
                "startup_evidence": (
                    None if startup_evidence is None else dict(startup_evidence)
                ),
                "error": str(exc) or exc.__class__.__name__,
                "error_type": exc.__class__.__name__,
            },
        )
        raise


def _report_worker_service_startup(
    startup_reporter: WorkerStartupReporter | None,
    record: dict[str, object],
) -> None:
    if startup_reporter is None:
        return
    startup_reporter(dict(record))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="TurboBus worker socket service",
    )
    parser.add_argument(
        "--daemon-socket-path",
        required=True,
        help="Unix socket path for the daemon control plane",
    )
    parser.add_argument(
        "--socket-path",
        required=True,
        help="Unix socket path for the worker socket service",
    )
    parser.add_argument("--chunk-bytes", type=int, default=None)
    parser.add_argument("--staging-slots", type=int, default=None)
    parser.add_argument("--profile-bytes", type=int, default=None)
    args = parser.parse_args(argv)
    runtime_options = _runtime_options_from_args(args)
    try:
        if runtime_options is None:
            run_worker_service_process(
                args.daemon_socket_path,
                args.socket_path,
            )
        else:
            run_worker_service_process(
                args.daemon_socket_path,
                args.socket_path,
                runtime_options=runtime_options,
            )
    except WorkerStartupError as exc:
        parser.exit(2, f"turbobus worker startup failed: {exc}\n")
    return 0


def _runtime_options_from_args(args) -> RuntimeOptions | None:
    if (
        args.chunk_bytes is None
        and args.staging_slots is None
        and args.profile_bytes is None
    ):
        return None
    defaults = RuntimeOptions()
    return RuntimeOptions(
        chunk_bytes=defaults.chunk_bytes if args.chunk_bytes is None else args.chunk_bytes,
        staging_slots=(
            defaults.staging_slots if args.staging_slots is None else args.staging_slots
        ),
        profile_bytes=(
            defaults.profile_bytes if args.profile_bytes is None else args.profile_bytes
        ),
    )


__all__ = [
    "build_worker_service_transport",
    "main",
    "run_worker_service_process",
    "worker_startup_evidence_from_daemon",
    "WorkerStartupError",
]
