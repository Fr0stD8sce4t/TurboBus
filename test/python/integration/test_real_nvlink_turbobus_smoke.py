from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

from turbobus.client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer
from turbobus.runtime_options import RuntimeOptions
from turbobus.runtime_session import TurboBusRuntimeSession
from turbobus.schema import WorkloadKind


def _real_smoke_enabled() -> bool:
    return os.environ.get("TURBOBUS_REAL_NVLINK_SMOKE") == "1"


def _require_torch():
    try:
        import torch
    except ImportError as exc:
        raise unittest.SkipTest("PyTorch is required for real NVLink smoke") from exc
    if not torch.cuda.is_available():
        raise unittest.SkipTest("CUDA is unavailable")
    return torch


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _path_bytes(receipt, kind: str) -> int:
    total = 0
    for item in receipt.path_stats:
        if str(item.get("kind", "")).lower() == kind:
            total += int(item.get("bytes", 0) or 0)
    return total


def _path_chunks(receipt, kind: str) -> int:
    total = 0
    for item in receipt.path_stats:
        if str(item.get("kind", "")).lower() == kind:
            total += int(item.get("chunk_count", item.get("chunks", 0)) or 0)
    return total


def _start_service(command: list[str]) -> subprocess.Popen:
    return subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _wait_for_socket(
    socket_path: str,
    process: subprocess.Popen,
    *,
    timeout_seconds: float = 20.0,
) -> None:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None
    while time.time() < deadline:
        returncode = process.poll()
        if returncode is not None:
            stdout, stderr = process.communicate(timeout=1.0)
            raise RuntimeError(
                f"service exited before socket became ready: rc={returncode} "
                f"stdout={stdout!r} stderr={stderr!r}"
            )
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            client.connect(socket_path)
            return
        except OSError as exc:
            last_error = exc
            time.sleep(0.05)
        finally:
            client.close()
    raise TimeoutError(
        f"socket did not become ready: {socket_path}; last_error={last_error}"
    )


def _stop_service(process: subprocess.Popen | None) -> tuple[str, str]:
    if process is None:
        return "", ""
    if process.poll() is None:
        process.terminate()
        try:
            return process.communicate(timeout=5.0)
        except subprocess.TimeoutExpired:
            process.kill()
            return process.communicate(timeout=5.0)
    return process.communicate(timeout=1.0)


@unittest.skipUnless(
    _real_smoke_enabled(),
    "set TURBOBUS_REAL_NVLINK_SMOKE=1 to run real GPU/NVLink smoke",
)
class RealNvlinkTurboBusSmokeTest(unittest.TestCase):
    def test_h2d_uses_nvlink_relay_path(self) -> None:
        torch = _require_torch()
        target_gpu = _env_int("TURBOBUS_TARGET_GPU", 5)
        relay_gpu = _env_int("TURBOBUS_RELAY_GPU", 6)
        mib = _env_int("TURBOBUS_SMOKE_MIB", 1024)
        chunk_mib = _env_int("TURBOBUS_CHUNK_MIB", 16)

        if torch.cuda.device_count() <= max(target_gpu, relay_gpu):
            raise unittest.SkipTest("configured GPU index is out of range")
        if not torch.cuda.can_device_access_peer(target_gpu, relay_gpu):
            raise unittest.SkipTest("configured target/relay pair has no CUDA P2P")

        size_bytes = mib * 1024 * 1024
        chunk_bytes = chunk_mib * 1024 * 1024
        job_id = os.environ.get("TURBOBUS_SMOKE_JOB_ID", "real-nvlink-smoke")
        run_id = uuid.uuid4().hex[:8]

        cpu_buffer = SharedPinnedCpuBuffer.allocate(
            buffer_id=f"cpu-{run_id}",
            job_id=job_id,
            size_bytes=size_bytes,
            name_prefix="turbobus-real-smoke",
        )
        session: TurboBusRuntimeSession | None = None
        daemon_process: subprocess.Popen | None = None
        worker_process: subprocess.Popen | None = None
        try:
            source = torch.empty(size_bytes, dtype=torch.uint8, pin_memory=True)
            source.random_(0, 256)
            cpu_buffer.write(source.numpy().tobytes())

            torch.cuda.set_device(target_gpu)
            target = torch.empty(
                size_bytes,
                dtype=torch.uint8,
                device=f"cuda:{target_gpu}",
            )
            gpu_buffer = CudaIpcDeviceBuffer.from_device_pointer(
                buffer_id=f"gpu-{run_id}",
                job_id=job_id,
                device_index=target_gpu,
                size_bytes=size_bytes,
                device_ptr=target.data_ptr(),
            )

            with tempfile.TemporaryDirectory(prefix="turbobus-real-smoke-") as tmpdir:
                daemon_socket = os.path.join(tmpdir, "daemon.sock")
                worker_socket = os.path.join(tmpdir, "worker.sock")
                daemon_process = _start_service(
                    [
                        sys.executable,
                        "-m",
                        "turbobus.daemon",
                        "--socket-path",
                        daemon_socket,
                        "--target-gpu",
                        str(target_gpu),
                        "--min-relays",
                        "1",
                        "--max-sessions-per-relay",
                        "1",
                        "--max-inflight-chunks-per-relay",
                        "128",
                        "--profile-max-age-seconds",
                        "3600.0",
                    ]
                )
                _wait_for_socket(daemon_socket, daemon_process)

                worker_process = _start_service(
                    [
                        sys.executable,
                        "-m",
                        "turbobus.worker",
                        "--daemon-socket-path",
                        daemon_socket,
                        "--socket-path",
                        worker_socket,
                        "--chunk-bytes",
                        str(chunk_bytes),
                        "--profile-bytes",
                        str(min(size_bytes, 256 * 1024 * 1024)),
                    ]
                )
                _wait_for_socket(worker_socket, worker_process)

                session = TurboBusRuntimeSession.open_production_socket(
                    job_id=job_id,
                    daemon_socket_path=daemon_socket,
                    worker_socket_path=worker_socket,
                    runtime_options=RuntimeOptions(
                        chunk_bytes=chunk_bytes,
                        profile_bytes=min(size_bytes, 256 * 1024 * 1024),
                        profile_on_first_transfer=True,
                        daemon_socket_path=daemon_socket,
                        worker_socket_path=worker_socket,
                        daemon_max_inflight_chunks=128,
                        daemon_profile_max_age_seconds=3600.0,
                    ),
                )

                start = time.perf_counter()
                receipt = session.fetch_h2d(
                    cpu_buffer,
                    gpu_buffer,
                    chunk_bytes=chunk_bytes,
                    workload_kind=WorkloadKind.MODEL_WEIGHTS,
                    metadata={
                        "test": "real_nvlink_turbobus_smoke",
                        "target_gpu": target_gpu,
                        "relay_gpu": relay_gpu,
                        "size_bytes": size_bytes,
                        "chunk_bytes": chunk_bytes,
                    },
                    intent_id=f"intent-{run_id}",
                )
                elapsed = time.perf_counter() - start

                torch.cuda.synchronize(target_gpu)
                observed = target.cpu()
                expected = torch.frombuffer(bytearray(cpu_buffer.read()), dtype=torch.uint8)
                content_match = torch.equal(observed, expected)

                direct_bytes = _path_bytes(receipt, "direct")
                relay_bytes = _path_bytes(receipt, "relay")
                direct_chunks = _path_chunks(receipt, "direct")
                relay_chunks = _path_chunks(receipt, "relay")
                throughput_gib_s = (
                    size_bytes / (1024**3) / elapsed
                    if elapsed > 0
                    else 0.0
                )

                print(
                    "\nreal_nvlink_turbobus_smoke "
                    f"target_gpu={target_gpu} relay_gpu={relay_gpu} "
                    f"session_id={session.session_id} "
                    f"relay_gpus={list(session.relay_gpus or [])} "
                    f"state={getattr(receipt.state, 'value', receipt.state)} "
                    f"bytes_total={receipt.bytes_total} "
                    f"bytes_completed={receipt.bytes_completed} "
                    f"direct_bytes={direct_bytes} "
                    f"relay_bytes={relay_bytes} "
                    f"direct_chunks={direct_chunks} "
                    f"relay_chunks={relay_chunks} "
                    f"elapsed_ms={elapsed * 1000:.3f} "
                    f"throughput_gib_s={throughput_gib_s:.2f} "
                    f"verify={'OK' if content_match else 'FAIL'} "
                    f"receipt_id={receipt.receipt_id} "
                    f"decision_id={receipt.decision_id} "
                    f"ticket_id={receipt.ticket_id}"
                )

                self.assertTrue(content_match)
                self.assertEqual(receipt.bytes_completed, size_bytes)
                self.assertGreater(relay_bytes, 0)
                self.assertGreater(relay_chunks, 0)
        finally:
            if session is not None:
                session.close()
            worker_stdout, worker_stderr = _stop_service(worker_process)
            daemon_stdout, daemon_stderr = _stop_service(daemon_process)
            if worker_stdout.strip() or worker_stderr.strip():
                print(
                    "\nworker_service_output "
                    f"stdout={worker_stdout.strip()!r} "
                    f"stderr={worker_stderr.strip()!r}"
                )
            if daemon_stdout.strip() or daemon_stderr.strip():
                print(
                    "\ndaemon_service_output "
                    f"stdout={daemon_stdout.strip()!r} "
                    f"stderr={daemon_stderr.strip()!r}"
                )
            cpu_buffer.release()


if __name__ == "__main__":
    unittest.main(verbosity=2)
