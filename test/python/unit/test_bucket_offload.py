from __future__ import annotations

import unittest

from turbobus.offload_store import BlockState
from turbobus.state_offload import PackedStateRegistry, StateOffloadCore, StateOffloadSpec
from test.python.fixtures.runtime_evidence import FakeRuntimeSession


class FakeTensor:
    def __init__(self, bytes_: int) -> None:
        self._bytes = bytes_

    def numel(self) -> int:
        return self._bytes

    def element_size(self) -> int:
        return 1


class PackedStateRegistryTest(unittest.TestCase):
    def test_packed_registry_registers_and_moves_state_ranges(self) -> None:
        core = make_core()

        core.register_registry(
            PackedStateRegistry(
                prefix="bucket-",
                cpu_tensor=FakeTensor(256),
                gpu_tensor=object(),
                bucket_bytes=64,
                bucket_count=2,
            )
        )
        core.submit_prefetch_states(["bucket-0", "bucket-1"]).wait()
        core.submit_offload_states(["bucket-0", "bucket-1"]).wait()

        self.assertEqual(core.names(), ["bucket-0", "bucket-1"])
        self.assertEqual(core.state("bucket-0").state, BlockState.CPU)
        self.assertEqual(core.state("bucket-1").cpu_offset, 64)


def make_core() -> StateOffloadCore:
    session = FakeRuntimeSession()
    context = session.make_transfer_context(
        FakeTensor(256),
        object(),
        intent_prefix="packed-state",
    )
    return StateOffloadCore(
        session,
        context,
        StateOffloadSpec(
            state_kind="packed_test_state",
            evidence_prefix="packed-state",
            item_field="bucket_names",
            item_count_field="bucket_count",
            binding_field="bucket_bindings",
            range_field="bucket_ranges",
        ),
    )


if __name__ == "__main__":
    unittest.main()
