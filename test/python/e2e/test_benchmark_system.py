from __future__ import annotations

import contextlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from turbobus.schema import TransferReceipt, TransferStatusState

BENCHMARKS = Path(__file__).resolve().parents[3] / "benchmarks"
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(BENCHMARKS))

import model_loading  # noqa: E402
import paper_validation  # noqa: E402
import training_offload  # noqa: E402


class BenchmarkSystemTest(unittest.TestCase):
    def test_model_loading_runtime_path_outputs_transfer_evidence(self) -> None:
        args = parse_model_args(
            "--bucket-count",
            "2",
            "--bucket-bytes",
            "64",
            "--iterations",
            "2",
            "--warmup",
            "1",
            "--run-id",
            "run-1",
        )
        runtime = FakeRuntime()

        result = model_loading.run_benchmark(
            args,
            session_factory=runtime.session_factory,
            buffer_factory=runtime.buffer_factory,
            loader_factory=runtime.model_loader_factory,
        )

        self.assert_runtime_result(
            result,
            transfer_bytes=128,
            source_key="source_buffer_id",
            destination_key="destination_buffer_id",
        )
        self.assertEqual(result["summary"]["iterations"], 2)
        self.assertEqual(result["summary"]["relay_bytes"], 64)
        self.assertTrue(runtime.buffers.released)
        self.assertEqual(runtime.session.events[:3], ["register_cuda", "open", "register_cpu"])
        self.assertEqual(runtime.model_loader.loaded, [["bucket-0", "bucket-1"]] * 3)

    def test_training_offload_runtime_path_outputs_prefetch_and_offload_evidence(self) -> None:
        args = parse_training_args(
            "--bucket-count",
            "4",
            "--active-buckets",
            "2",
            "--bucket-bytes",
            "64",
            "--iterations",
            "2",
            "--warmup",
            "1",
        )
        runtime = FakeRuntime()

        result = training_offload.run_benchmark(
            args,
            session_factory=runtime.session_factory,
            buffer_factory=runtime.buffer_factory,
            manager_factory=runtime.training_manager_factory,
        )

        self.assert_runtime_result(
            result,
            transfer_bytes=128,
            source_key="cpu_buffer_id",
            destination_key="gpu_buffer_id",
        )
        self.assertEqual(result["summary"]["prefetch"]["relay_bytes"], 64)
        self.assertEqual(result["summary"]["offload"]["relay_bytes"], 64)
        self.assertEqual(result["samples"][1]["bucket_names"], ["bucket-2", "bucket-3"])
        self.assertEqual(len(runtime.training_manager.prefetched), 3)
        self.assertEqual(len(runtime.training_manager.offloaded), 3)

    def test_benchmark_help_entrypoints_still_run(self) -> None:
        for script in (
            "benchmarks/model_loading.py",
            "benchmarks/training_offload.py",
            "benchmarks/paper_validation.py",
        ):
            with self.subTest(script=script):
                completed = subprocess.run(
                    [sys.executable, script, "--help"],
                    cwd=REPO_ROOT,
                    capture_output=True,
                    text=True,
                    check=False,
                )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertIn("usage:", completed.stdout)

    def test_json_outputs_are_report_consumable(self) -> None:
        runtime = FakeRuntime()
        model_result = model_loading.run_benchmark(
            parse_model_args("--bucket-count", "1", "--bucket-bytes", "96"),
            session_factory=runtime.session_factory,
            buffer_factory=runtime.buffer_factory,
            loader_factory=runtime.model_loader_factory,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "model.json"
            model_loading.write_json(str(output), model_result)
            data = json.loads(output.read_text(encoding="utf-8"))

        metrics = paper_validation.collect_model_metrics(data)
        self.assertEqual(metrics[0]["correctness_status"], "complete")
        self.assertEqual(metrics[0]["relay_bytes"], 48)
        self.assertEqual(metrics[0]["source_buffer_id"], "cpu-buffer")
        self.assertEqual(metrics[0]["destination_buffer_id"], "gpu-buffer")

    def assert_runtime_result(
        self,
        result: dict,
        *,
        transfer_bytes: int,
        source_key: str,
        destination_key: str,
    ) -> None:
        self.assertEqual(result["config"]["session_id"], "session-1")
        self.assertEqual(result["config"][source_key], "cpu-buffer")
        self.assertEqual(result["config"][destination_key], "gpu-buffer")
        if "bytes" in result["summary"]:
            summary = result["summary"]
        else:
            summary = result["summary"]["prefetch"]
        self.assertEqual(summary["bytes"], transfer_bytes)
        self.assertEqual(summary["bytes_completed"], transfer_bytes)
        self.assertTrue(summary["executed"])
        self.assertTrue(summary["verified"])
        self.assertTrue(summary["content_match"])


class FakeRuntime:
    def __init__(self) -> None:
        self.session = FakeSession()
        self.buffers = FakeBuffers()
        self.session_factory = FakeSessionFactory(self.session)
        self.buffer_factory = FakeBufferFactory(self.buffers)
        self.model_loader = None
        self.training_manager = None

    def model_loader_factory(self, args, session, buffers):
        self.model_loader = FakeModelLoader(args)
        return self.model_loader

    def training_manager_factory(self, args, session, buffers):
        self.training_manager = FakeTrainingManager(args)
        return self.training_manager


class FakeSessionFactory:
    def __init__(self, session) -> None:
        self.session = session

    @contextlib.contextmanager
    def open(self, args):
        try:
            yield self.session
        finally:
            self.session.close()


class FakeSession:
    def __init__(self) -> None:
        self.events = []

    def register_cuda_buffer(self, buffer) -> None:
        self.events.append("register_cuda")

    def open_session(self) -> str:
        self.events.append("open")
        return "session-1"

    def register_cpu_buffer(self, buffer) -> None:
        self.events.append("register_cpu")

    def close(self) -> None:
        self.events.append("close")


class FakeBufferFactory:
    def __init__(self, buffers) -> None:
        self.buffers = buffers

    def allocate(self, args):
        return self.buffers


class FakeBuffers:
    def __init__(self) -> None:
        self.cpu_buffer = FakeBuffer("cpu-buffer")
        self.gpu_buffer = FakeBuffer("gpu-buffer")
        self.released = False

    def release(self) -> None:
        self.released = True


class FakeBuffer:
    def __init__(self, buffer_id: str) -> None:
        self.buffer_id = buffer_id


class FakeModelLoader:
    def __init__(self, args) -> None:
        self.args = args
        self.loaded = []
        self.calls = 0

    def load_batch(self, names):
        selected = list(names)
        self.loaded.append(selected)
        return FakeBatch(self._receipt("load", len(selected) * int(self.args.bucket_bytes)))

    def _receipt(self, prefix: str, byte_count: int) -> TransferReceipt:
        self.calls += 1
        return make_receipt(f"{prefix}-{self.calls}", byte_count)


class FakeTrainingManager:
    def __init__(self, args) -> None:
        self.args = args
        self.prefetched = []
        self.offloaded = []
        self.calls = 0

    def prefetch_batch(self, names):
        selected = list(names)
        self.prefetched.append(selected)
        return FakeBatch(self._receipt("prefetch", len(selected) * int(self.args.bucket_bytes)))

    def offload_batch(self, names):
        selected = list(names)
        self.offloaded.append(selected)
        return FakeBatch(self._receipt("offload", len(selected) * int(self.args.bucket_bytes)))

    def _receipt(self, prefix: str, byte_count: int) -> TransferReceipt:
        self.calls += 1
        return make_receipt(f"{prefix}-{self.calls}", byte_count)


class FakeBatch:
    def __init__(self, receipt: TransferReceipt) -> None:
        handle = FakeHandle(receipt)
        self.handles = (handle, handle)

    def wait(self) -> None:
        return None


class FakeHandle:
    def __init__(self, receipt: TransferReceipt) -> None:
        self.receipt = receipt


def make_receipt(suffix: str, total_bytes: int) -> TransferReceipt:
    direct_bytes = total_bytes // 2
    relay_bytes = total_bytes - direct_bytes
    return TransferReceipt(
        receipt_id=f"receipt-{suffix}",
        ticket_id=f"ticket-{suffix}",
        intent_id=f"intent-{suffix}",
        decision_id=f"decision-{suffix}",
        topology_snapshot_id="topology-1",
        job_id="job-1",
        session_id="session-1",
        state=TransferStatusState.COMPLETE,
        bytes_total=total_bytes,
        bytes_completed=total_bytes,
        path_stats=(
            {"kind": "direct", "bytes": direct_bytes, "chunk_count": 1},
            {"kind": "relay", "bytes": relay_bytes, "chunk_count": 1},
        ),
        metadata={
            "fallback_reason": "none",
            "completion_source": "worker",
            "executed": True,
            "verified": True,
            "verified_bytes": total_bytes,
            "content_match": True,
            "verification_source": "fixture_worker",
            "verification_method": "fixture_compare",
        },
    )


def parse_model_args(*extra: str):
    args = model_loading.build_parser().parse_args(base_runtime_cli() + list(extra))
    model_loading.validate_args(args)
    return args


def parse_training_args(*extra: str):
    args = training_offload.build_parser().parse_args(base_runtime_cli() + list(extra))
    training_offload.validate_args(args)
    return args


def base_runtime_cli() -> list[str]:
    return [
        "--target-gpu",
        "0",
        "--job-id",
        "job-1",
        "--daemon-socket-path",
        "/tmp/turbobusd.sock",
        "--worker-socket-path",
        "/tmp/turbobusw.sock",
        "--iterations",
        "1",
    ]


if __name__ == "__main__":
    unittest.main()
