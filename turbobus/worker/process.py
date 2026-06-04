from __future__ import annotations

import argparse
from threading import Event
from typing import Sequence

from ..backends.cuda import default_cuda_backend
from ..daemon.client import TurboBusDaemonExecutionClient
from ..runtime_options import RuntimeOptions
from .endpoint import WorkerServiceEndpoint
from .lifecycle import WorkerTransferClient, WorkerTransferService
from .cuda_executor import CudaWorkerExecutor
from .resources import WorkerDataPlaneResourceBinder
from .transport import WorkerServiceUnixSocketTransport


def build_worker_service_transport(
    daemon_socket_path: str,
    socket_path: str,
    *,
    backend=default_cuda_backend,
    runtime_options: RuntimeOptions | None = None,
) -> WorkerServiceUnixSocketTransport:
    options = runtime_options or RuntimeOptions()
    daemon_client = TurboBusDaemonExecutionClient(str(daemon_socket_path))
    transfer_client = WorkerTransferClient(
        daemon_client,
        executor=CudaWorkerExecutor(
            backend=backend,
            options=options,
        ),
        resource_binder=WorkerDataPlaneResourceBinder(backend=backend),
    )
    endpoint = WorkerServiceEndpoint(
        service=WorkerTransferService(
            daemon_client,
            transfer_client=transfer_client,
        ),
    )
    return WorkerServiceUnixSocketTransport(
        endpoint=endpoint,
        socket_path=str(socket_path),
    )


def run_worker_service_process(
    daemon_socket_path: str,
    socket_path: str,
    stop_event: Event | None = None,
    *,
    backend=default_cuda_backend,
    runtime_options: RuntimeOptions | None = None,
) -> None:
    if backend is default_cuda_backend and runtime_options is None:
        transport = build_worker_service_transport(daemon_socket_path, socket_path)
    else:
        transport = build_worker_service_transport(
            daemon_socket_path,
            socket_path,
            backend=backend,
            runtime_options=runtime_options,
        )
    transport.serve_forever(stop_event=stop_event)


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
]
