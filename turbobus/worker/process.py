from __future__ import annotations

import argparse
from threading import Event
from typing import Sequence

from ..daemon.client import TurboBusDaemonExecutionClient
from .endpoint import WorkerServiceEndpoint
from .lifecycle import WorkerTransferClient, WorkerTransferService
from .transport import WorkerServiceUnixSocketTransport


def build_worker_service_transport(
    daemon_socket_path: str,
    socket_path: str,
) -> WorkerServiceUnixSocketTransport:
    daemon_client = TurboBusDaemonExecutionClient(str(daemon_socket_path))
    transfer_client = WorkerTransferClient(daemon_client)
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
) -> None:
    transport = build_worker_service_transport(daemon_socket_path, socket_path)
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
    args = parser.parse_args(argv)
    run_worker_service_process(
        args.daemon_socket_path,
        args.socket_path,
    )
    return 0


__all__ = [
    "build_worker_service_transport",
    "main",
    "run_worker_service_process",
]
