from __future__ import annotations

import contextlib
from pathlib import Path
import sys
import unittest

from turbobus.offload_store import BlockState
from turbobus.schema import TransferReceipt
from turbobus.state_offload import StateDescriptor, StateOffloadCore, StateOffloadSpec
from test.python.fixtures.runtime_evidence import make_runtime_intent, make_runtime_receipt

BENCHMARKS = Path(__file__).resolve().parents[3] / "benchmarks"
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(BENCHMARKS))

import model_loading  # noqa: E402
import optimizer_offload  # noqa: E402
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
            core_factory=runtime.model_core_factory,
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
        self.assertEqual(runtime.model_core.transfer_calls, [["bucket-0", "bucket-1"]] * 3)

    def test_model_loading_cli_accepts_total_chunk_mode_and_no_verify(self) -> None:
        args = parse_model_args(
            "--total-mib",
            "1",
            "--chunk-mib",
            "1",
            "--iters",
            "2",
            "--mode",
            "direct-only",
            "--no-verify",
        )

        self.assertEqual(args.bucket_count, 1)
        self.assertEqual(args.bucket_bytes, 1024 * 1024)
        self.assertEqual(args.chunk_bytes, 1024 * 1024)
        self.assertEqual(args.iterations, 2)
        self.assertEqual(model_loading.benchmark_transfer_mode(args), "direct")
        self.assertFalse(args.verify)
        self.assertEqual(model_loading.config_dict(args)["mode"], "direct-only")
        self.assertFalse(model_loading.config_dict(args)["verify"])

    def test_model_loading_passes_mode_and_verification_policy_to_core(self) -> None:
        args = parse_model_args(
            "--bucket-count",
            "1",
            "--bucket-bytes",
            "64",
            "--chunk-bytes",
            "16",
            "--mode",
            "pooled",
            "--no-verify",
        )
        buffers = FakeBuffers()
        session = FakeSession()
        core = model_loading.make_core(args, session, buffers)

        self.assertIsNotNone(core)
        self.assertEqual(core.transfer_context.policy_hints["transfer_mode"], "pool")
        self.assertTrue(core.transfer_context.policy_hints["skip_verification"])
        self.assertEqual(core.transfer_context.metadata["mode"], "pooled")
        self.assertTrue(core.transfer_context.metadata["skip_verification"])
        self.assertEqual(core.names(), ["bucket-0"])

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
            core_factory=runtime.training_core_factory,
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
        self.assertEqual(len(runtime.training_core.prefetched), 3)
        self.assertEqual(len(runtime.training_core.offloaded), 3)

    def test_optimizer_offload_runtime_path_outputs_prefetch_and_offload_evidence(self) -> None:
        args = parse_optimizer_args(
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

        result = optimizer_offload.run_benchmark(
            args,
            session_factory=runtime.session_factory,
            buffer_factory=runtime.buffer_factory,
            core_factory=runtime.optimizer_core_factory,
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
        self.assertEqual(len(runtime.optimizer_core.prefetched), 3)
        self.assertEqual(len(runtime.optimizer_core.offloaded), 3)

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
        self.model_core = None
        self.training_core = None
        self.optimizer_core = None

    def model_core_factory(self, args, session, buffers):
        self.model_core = model_loading.make_core(args, session, buffers)
        return self.model_core

    def training_core_factory(self, args, session, buffers):
        self.training_core = training_offload.make_core(args, session, buffers)
        return self.training_core

    def optimizer_core_factory(self, args, session, buffers):
        self.optimizer_core = optimizer_offload.make_core(args, session, buffers)
        return self.optimizer_core


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

    def make_state_offload(self, spec, cpu_buffer, gpu_buffer, **kwargs):
        return FakeStateOffloadCore(
            phase=str(getattr(spec, "state_kind", "state")),
            policy_hints=kwargs.get("policy_hints"),
            metadata=kwargs.get("metadata"),
            workload_kind=kwargs.get("workload_kind"),
            intent_prefix=kwargs.get("intent_prefix"),
        )

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


class FakeBatch:
    def __init__(self, receipt: TransferReceipt) -> None:
        handle = FakeHandle(receipt)
        self.handles = (handle, handle)

    def wait(self) -> None:
        return None


class FakeHandle:
    def __init__(self, receipt: TransferReceipt) -> None:
        self.receipt = receipt


class FakeStateOffloadCore:
    def __init__(
        self,
        *,
        phase: str,
        policy_hints=None,
        metadata=None,
        workload_kind=None,
        intent_prefix=None,
    ) -> None:
        self.phase = phase
        self.transfer_context = type("TransferContext", (), {})()
        self.transfer_context.policy_hints = {} if policy_hints is None else dict(policy_hints)
        self.transfer_context.metadata = {} if metadata is None else dict(metadata)
        self.transfer_context.workload_kind = workload_kind
        self.transfer_context.intent_prefix = intent_prefix
        self.prefetched: list[list[str]] = []
        self.offloaded: list[list[str]] = []
        self.transfer_calls: list[list[str]] = []
        self._descriptors: dict[str, StateDescriptor] = {}

    def register_registry(self, registry, *, replace: bool = False):
        descriptors = list(registry.rebuild())
        self._descriptors = {descriptor.name: descriptor for descriptor in descriptors}
        return descriptors

    def names(self) -> list[str]:
        return sorted(self._descriptors)

    def submit_prefetch_states(self, names, *, operation: str = "submit_prefetch_states"):
        selected = list(names)
        self.prefetched.append(selected)
        self.transfer_calls.append(selected)
        return FakeBatch(make_receipt(f"{self.phase}-{operation}", self._byte_count(selected)))

    def submit_offload_states(self, names, *, operation: str = "submit_offload_states"):
        selected = list(names)
        self.offloaded.append(selected)
        self.transfer_calls.append(selected)
        return FakeBatch(make_receipt(f"{self.phase}-{operation}", self._byte_count(selected)))

    def _byte_count(self, names: list[str]) -> int:
        return sum(
            int(self._descriptors[name].byte_count or 0)
            if name in self._descriptors
            else 0
            for name in names
        )

    def state(self, name: str):
        descriptor = self._descriptors[str(name)]
        return type(
            "StateView",
            (),
            {
                "state": BlockState.CPU,
                "bytes": int(descriptor.byte_count or 0),
                "cpu_offset": int(descriptor.cpu_offset),
            },
        )()

def make_receipt(suffix: str, total_bytes: int) -> TransferReceipt:
    intent = make_runtime_intent(suffix, total_bytes=total_bytes)
    return make_runtime_receipt(intent, receipt_id=f"receipt-{suffix}")


def parse_model_args(*extra: str):
    args = model_loading.build_parser().parse_args(base_runtime_cli() + list(extra))
    model_loading.validate_args(args)
    return args


def parse_training_args(*extra: str):
    args = training_offload.build_parser().parse_args(base_runtime_cli() + list(extra))
    training_offload.validate_args(args)
    return args


def parse_optimizer_args(*extra: str):
    args = optimizer_offload.build_parser().parse_args(base_runtime_cli() + list(extra))
    optimizer_offload.validate_args(args)
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
