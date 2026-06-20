from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from threading import Event
from unittest.mock import Mock, patch

from turbobus.worker import (
    CudaWorkerExecutor,
    WorkerServiceEndpoint,
    WorkerServiceUnixSocketTransport,
    decode_worker_response_envelope,
)
from turbobus.worker.process import (
    build_worker_service_transport,
    main,
    run_worker_service_process,
)


class WorkerProcessTest(unittest.TestCase):
    def test_build_worker_service_transport_wires_daemon_and_worker_sockets(self) -> None:
        startup_evidence = production_worker_startup_evidence()

        with patch(
            "turbobus.worker.process.worker_startup_evidence_from_daemon",
            return_value=startup_evidence,
        ) as startup:
            transport = build_worker_service_transport(
                "/tmp/turbobusd.sock",
                "/tmp/turbobus-worker.sock",
            )

        self.assertIsInstance(transport, WorkerServiceUnixSocketTransport)
        self.assertIsInstance(transport.endpoint, WorkerServiceEndpoint)
        self.assertEqual(transport.socket_path, "/tmp/turbobus-worker.sock")
        startup.assert_called_once_with("/tmp/turbobusd.sock")
        self.assertEqual(
            transport.endpoint.service.transfer_client._authorizer.daemon_client.socket_path,
            "/tmp/turbobusd.sock",
        )
        self.assertIsInstance(
            transport.endpoint.service.transfer_client.executor,
            CudaWorkerExecutor,
        )
        self.assertIsNotNone(
            transport.endpoint.service.transfer_client.resource_binder,
        )

    def test_run_worker_service_process_uses_the_transport(self) -> None:
        stop_event = Event()
        fake_transport = Mock()
        fake_transfer_client = Mock()
        fake_transfer_client.close_execution_pool.return_value = {
            "cancel_queued": True,
            "cancelled_transfer_ids": (),
        }
        startup_evidence = production_worker_startup_evidence()

        with patch(
            "turbobus.worker.process._build_worker_service_runtime",
            return_value=(fake_transport, fake_transfer_client, startup_evidence),
        ) as build_runtime:
            run_worker_service_process(
                "/tmp/turbobusd.sock",
                "/tmp/turbobus-worker.sock",
                stop_event=stop_event,
            )

        build_runtime.assert_called_once_with(
            "/tmp/turbobusd.sock",
            "/tmp/turbobus-worker.sock",
        )
        fake_transport.serve_forever.assert_called_once_with(
            stop_event=stop_event,
        )
        fake_transfer_client.close_execution_pool.assert_called_once_with(
            cancel_queued=True,
        )

    def test_main_parses_args_and_runs_service_process(self) -> None:
        with patch("turbobus.worker.process.run_worker_service_process") as run:
            exit_code = main(
                [
                    "--daemon-socket-path",
                    "/tmp/turbobusd.sock",
                    "--socket-path",
                    "/tmp/turbobus-worker.sock",
                ]
            )

        self.assertEqual(exit_code, 0)
        run.assert_called_once_with(
            "/tmp/turbobusd.sock",
            "/tmp/turbobus-worker.sock",
        )

    @unittest.skipUnless(hasattr(socket, "AF_UNIX"), "Unix domain sockets are unavailable")
    def test_worker_module_subprocess_serves_worker_socket(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            worker_socket = os.path.join(tmpdir, "worker.sock")
            daemon_socket = os.path.join(tmpdir, "daemon.sock")
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "turbobus.worker",
                    "--daemon-socket-path",
                    daemon_socket,
                    "--socket-path",
                    worker_socket,
                ],
                cwd=os.getcwd(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                self._wait_for_socket(worker_socket)

                worker_response = _send_worker_message(worker_socket, "{not-json")
                worker_payload = decode_worker_response_envelope(worker_response)
                self.assertFalse(worker_payload.ok)
                self.assertEqual(worker_payload.final_state, "parse_failed")

                process.terminate()
                stdout, stderr = process.communicate(timeout=5)
                self.assertIsNotNone(process.returncode, stderr)
                self.assertEqual(stdout, "")
            finally:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=5)

    def _wait_for_socket(self, socket_path: str) -> None:
        for _ in range(100):
            if os.path.exists(socket_path):
                return
            time.sleep(0.01)
        self.fail(f"worker socket was not created: {socket_path}")


def _send_worker_message(socket_path: str, message: str | bytes) -> str:
    payload = message if isinstance(message, bytes) else message.encode("utf-8")
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        client.connect(socket_path)
        client.sendall(payload + b"\n")
        data = b""
        while True:
            chunk = client.recv(65536)
            if not chunk:
                break
            data += chunk
            if b"\n" in data:
                break
        return data.partition(b"\n")[0].decode("utf-8")
    finally:
        client.close()


def production_worker_startup_evidence() -> dict[str, object]:
    return {
        "startup_source": "worker_process_daemon_inventory",
        "daemon_socket_path": "/tmp/turbobusd.sock",
        "topology_snapshot_id": "topology-production-v1",
        "inventory_source": "cuda_nvml",
        "inventory_version": 1,
        "inventory_discovered_at": 1.0,
        "gpu_count": 2,
        "pcie_path_count": 2,
        "fabric_link_count": 1,
        "require_authenticated_peers": False,
        "daemon_peer_identity": None,
        "daemon_peer_authenticated": False,
    }


if __name__ == "__main__":
    unittest.main()
