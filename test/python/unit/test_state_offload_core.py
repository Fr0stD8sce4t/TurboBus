from __future__ import annotations

import unittest

from turbobus.offload_store import BlockState
from turbobus.state_offload import (
    StateDescriptor,
    StateOffloadCore,
    StateOffloadSpec,
    StateOffloadTransaction,
    StaticStateRegistry,
    training_state_spec,
    validate_optimizer_state_metadata,
)
from test.python.fixtures.runtime_evidence import FakeRuntimeSession


class FakeTensor:
    def __init__(self, bytes_: int) -> None:
        self._bytes = bytes_

    def numel(self) -> int:
        return self._bytes

    def element_size(self) -> int:
        return 1


class StateOffloadCoreTest(unittest.TestCase):
    def test_registers_states_idempotently_and_moves_batches(self) -> None:
        core = make_core()

        registered = core.register_states(
            [
                StateDescriptor(
                    name="state-0",
                    state_id="state-id-0",
                    cpu_tensor=FakeTensor(64),
                    gpu_tensor=object(),
                    byte_count=64,
                )
            ]
        )
        duplicate = core.register_states(
            [
                StateDescriptor(
                    name="state-0",
                    state_id="state-id-0",
                    cpu_tensor=FakeTensor(64),
                    gpu_tensor=object(),
                    byte_count=64,
                )
            ]
        )

        self.assertEqual(len(registered), 1)
        self.assertEqual(duplicate, [])
        core.prefetch_states(["state-0"]).wait()
        core.offload_states(["state-0"]).wait()
        self.assertEqual(core.block("state-0").state, BlockState.CPU)
        self.assertGreaterEqual(len(core.transfer_lifecycle_history), 4)

    def test_empty_batch_does_not_create_lifecycle(self) -> None:
        core = make_core()

        batch = core.submit_prefetch_states([])

        self.assertEqual(batch.names, ())
        self.assertEqual(core.transfer_lifecycle_history, ())

    def test_register_registry_and_replace_state(self) -> None:
        core = make_core()
        first = StaticStateRegistry(
            [
                StateDescriptor(
                    name="state-0",
                    state_id="state-id-0",
                    cpu_tensor=FakeTensor(64),
                    gpu_tensor=object(),
                    byte_count=64,
                )
            ]
        )
        second = StaticStateRegistry(
            [
                StateDescriptor(
                    name="state-0",
                    state_id="state-id-1",
                    cpu_tensor=FakeTensor(32),
                    gpu_tensor=object(),
                    byte_count=32,
                )
            ]
        )

        core.register_registry(first)
        replaced = core.register_registry(second, replace=True)

        self.assertEqual(len(replaced), 1)
        self.assertEqual(core.block("state-0").block_id, "state-id-1")
        self.assertEqual(core.block("state-0").bytes, 32)

    def test_spec_metadata_rejects_physical_policy(self) -> None:
        with self.assertRaisesRegex(ValueError, "physical paths"):
            validate_optimizer_state_metadata(
                {"relay_gpu": 1},
                field_name="optimizer metadata",
            )

    def test_transaction_success_and_rollback(self) -> None:
        core = make_core()
        core.register_states(
            [
                StateDescriptor(
                    name="state-0",
                    state_id="state-id-0",
                    cpu_tensor=FakeTensor(64),
                    gpu_tensor=object(),
                    byte_count=64,
                )
            ]
        )
        calls: list[str] = []

        transaction = StateOffloadTransaction(
            core,
            ("state-0",),
            restore_before_step=lambda names: calls.append("restore"),
            capture_after_step=lambda names: calls.append("capture"),
            rollback_restore=lambda names: calls.append("rollback"),
        ).begin()
        transaction.prefetch_before_step()
        self.assertEqual(transaction.state, "prefetched")
        transaction.commit_after_step()

        self.assertEqual(transaction.state, "committed")
        self.assertEqual(calls, ["restore", "capture"])

        rollback = StateOffloadTransaction(
            core,
            ("state-0",),
            rollback_restore=lambda names: calls.append("rollback"),
        ).begin()
        rollback.rollback_on_error()
        self.assertEqual(rollback.state, "rolled_back")
        self.assertEqual(calls[-1], "rollback")

    def test_transaction_rejects_illegal_state_transitions(self) -> None:
        core = make_core()
        core.register_states(
            [
                StateDescriptor(
                    name="state-0",
                    state_id="state-id-0",
                    cpu_tensor=FakeTensor(64),
                    gpu_tensor=object(),
                    byte_count=64,
                )
            ]
        )

        transaction = StateOffloadTransaction(core, ("state-0",)).begin()
        with self.assertRaisesRegex(RuntimeError, "prefetch before commit"):
            transaction.commit_after_step()
        transaction.prefetch_before_step()
        with self.assertRaisesRegex(RuntimeError, "already prefetched"):
            transaction.prefetch_before_step()
        transaction.commit_after_step()
        with self.assertRaisesRegex(RuntimeError, "cannot roll back after commit"):
            transaction.rollback_on_error()


def make_core() -> StateOffloadCore:
    session = FakeRuntimeSession()
    context = session.make_transfer_context(
        FakeTensor(256),
        object(),
        intent_prefix="state-core",
    )
    return StateOffloadCore(
        session,
        context,
        StateOffloadSpec(
            state_kind="test_state_core",
            evidence_prefix="test-state",
        ),
    )


if __name__ == "__main__":
    unittest.main()
