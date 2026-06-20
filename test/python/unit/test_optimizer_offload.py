from __future__ import annotations

import unittest

from turbobus.offload_store import BlockState
from turbobus.state_offload import PackedStateRegistry, make_optimizer_state_offload
from test.python.fixtures.runtime_evidence import FakeRuntimeSession


class FakeTensor:
    def __init__(self, bytes_: int) -> None:
        self._bytes = bytes_

    def numel(self) -> int:
        return self._bytes

    def element_size(self) -> int:
        return 1


class OptimizerStateOffloadTest(unittest.TestCase):
    def test_register_and_offload_optimizer_states(self) -> None:
        core = make_optimizer_state_offload(
            FakeRuntimeSession(),
            FakeTensor(256),
            object(),
            intent_prefix="optimizer-state",
        )
        core.register_registry(
            PackedStateRegistry(
                prefix="bucket-",
                cpu_tensor=FakeTensor(256),
                gpu_tensor=object(),
                bucket_bytes=64,
                bucket_count=2,
            )
        )

        core.submit_prefetch_states(["bucket-0", "bucket-1"], operation="prefetch").wait()
        core.submit_offload_states(["bucket-0", "bucket-1"], operation="offload").wait()

        self.assertEqual(core.names(), ["bucket-0", "bucket-1"])
        self.assertEqual(core.state("bucket-0").state, BlockState.CPU)
        self.assertEqual(core.last_transfer_lifecycle.names, ("bucket-0", "bucket-1"))
        self.assertEqual(core.spec.state_kind, "optimizer_state")

    def test_optimizer_metadata_rejects_physical_policy_keys(self) -> None:
        with self.assertRaisesRegex(ValueError, "physical paths"):
            make_optimizer_state_offload(
                FakeRuntimeSession(),
                FakeTensor(256),
                object(),
                metadata={"relay_gpu": 1},
                intent_prefix="optimizer-state",
            )


if __name__ == "__main__":
    unittest.main()
