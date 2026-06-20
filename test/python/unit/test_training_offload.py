from __future__ import annotations

import unittest

from turbobus.schema import WorkloadKind
from turbobus.state_offload import PackedStateRegistry, make_training_state_offload
from test.python.fixtures.runtime_evidence import FakeRuntimeSession


class FakeTensor:
    def __init__(self, bytes_: int) -> None:
        self._bytes = bytes_

    def numel(self) -> int:
        return self._bytes

    def element_size(self) -> int:
        return 1


class TrainingStateOffloadTest(unittest.TestCase):
    def test_training_factory_returns_state_core(self) -> None:
        core = make_training_state_offload(
            FakeRuntimeSession(),
            FakeTensor(128),
            object(),
            intent_prefix="training-state",
        )

        core.register_registry(
            PackedStateRegistry(
                prefix="bucket-",
                cpu_tensor=FakeTensor(128),
                gpu_tensor=object(),
                bucket_bytes=64,
                bucket_count=1,
            )
        )
        core.submit_prefetch_states(["bucket-0"], operation="prefetch").wait()

        lifecycle = core.last_transfer_lifecycle
        self.assertEqual(core.transfer_context.workload_kind, WorkloadKind.TRAINING_STATE)
        self.assertEqual(core.spec.state_kind, "training_state")
        self.assertEqual(lifecycle.evidence["state_kind"], "training_state")
        self.assertEqual(lifecycle.evidence["bucket_count"], 1)

    def test_training_metadata_rejects_physical_policy_keys(self) -> None:
        with self.assertRaisesRegex(ValueError, "physical paths"):
            make_training_state_offload(
                FakeRuntimeSession(),
                FakeTensor(128),
                object(),
                metadata={"relay_gpu": 1},
                intent_prefix="training-state",
            )


if __name__ == "__main__":
    unittest.main()
