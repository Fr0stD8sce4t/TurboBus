from __future__ import annotations

import os
import tempfile
import time
import unittest
import uuid

from turbobus.client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer
from turbobus.daemon.startup import DaemonStartupConfig
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
                session = TurboBusRuntimeSession.open_managed_production_socket(
                    job_id=job_id,
                    daemon_socket_path=daemon_socket,
                    worker_socket_path=worker_socket,
                    daemon_startup_config=DaemonStartupConfig(
                        target_gpu=target_gpu,
                        min_relay_count=1,
                        require_fabric=True,
                        require_pcie=True,
                        require_peer_credentials=False,
                        max_sessions_per_relay=1,
                        max_inflight_chunks_per_relay=128,
                        profile_max_age_seconds=3600.0,
                    ),
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
            cpu_buffer.release()


if __name__ == "__main__":
    unittest.main(verbosity=2)
