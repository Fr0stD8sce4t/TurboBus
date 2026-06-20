from __future__ import annotations

import copy
from types import SimpleNamespace
import unittest

from turbobus.state_offload import (
    TorchOptimizerStateRegistry,
    TorchOptimizerStateIndex,
    TorchOptimizerTransactionAdapter,
    make_optimizer_state_offload,
)
from turbobus.offload_store import BlockState
from test.python.fixtures.runtime_evidence import FakeRuntimeSession


try:
    import torch
except ImportError:
    torch = None


class TorchOptimizerTransactionAdapterTest(unittest.TestCase):
    def test_state_index_builds_stable_bucket_names_without_torch(self) -> None:
        optimizer = SimpleNamespace(state={}, param_groups=[])
        index = TorchOptimizerStateIndex(optimizer, name_prefix="optimizer/state")

        self.assertEqual(
            index.bucket_name(3, "momentum/buffer"),
            "optimizer_state/param_3/momentum_buffer",
        )

    @unittest.skipIf(torch is None, "PyTorch is not installed")
    def test_registers_adam_optimizer_state_tensors(self) -> None:
        model = make_model()
        optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
        run_adam_step(model, optimizer)
        adapter = make_adapter(optimizer)

        registered = adapter.register_optimizer_state()

        names = adapter.names()
        self.assertEqual(len(registered), len(names))
        self.assertIn("optimizer_state/param_0/exp_avg", names)
        self.assertIn("optimizer_state/param_0/exp_avg_sq", names)
        self.assertTrue(any(name.endswith("/step") for name in names))
        self.assertEqual(adapter.core.names(), names)
        self.assertTrue(all(adapter.core.state(name).bytes > 0 for name in names))

    @unittest.skipIf(torch is None, "PyTorch is not installed")
    def test_prefetch_and_offload_wrap_manager_transfers(self) -> None:
        model = make_model()
        optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
        run_adam_step(model, optimizer)
        adapter = make_adapter(optimizer)
        adapter.register_optimizer_state()

        prefetch = adapter.prefetch_state()
        prefetch.wait()
        offload = adapter.offload_state()
        offload.wait()

        self.assertEqual(prefetch.names, tuple(adapter.names()))
        self.assertEqual(offload.names, tuple(adapter.names()))
        self.assertTrue(
            all(adapter.core.state(name).state == BlockState.CPU for name in adapter.names())
        )
        self.assertGreaterEqual(len(adapter.core.transfer_lifecycle_history), 4)

    @unittest.skipIf(torch is None, "PyTorch is not installed")
    def test_prefetch_restores_state_from_last_offload_snapshot(self) -> None:
        model = make_model()
        optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
        run_adam_step(model, optimizer)
        adapter = make_adapter(optimizer)
        adapter.register_optimizer_state()
        adapter.offload_state().wait()
        expected = clone_optimizer_tensor_state(optimizer)

        with torch.no_grad():
            for state in optimizer.state.values():
                for value in state.values():
                    if isinstance(value, torch.Tensor):
                        value.zero_()

        adapter.prefetch_state().wait()

        assert_optimizer_tensor_state_close(self, optimizer, expected)

    @unittest.skipIf(torch is None, "PyTorch is not installed")
    def test_training_matches_baseline_with_prefetch_step_offload(self) -> None:
        baseline_model = make_model()
        tested_model = copy.deepcopy(baseline_model)
        baseline_optimizer = torch.optim.Adam(baseline_model.parameters(), lr=0.01)
        tested_optimizer = torch.optim.Adam(tested_model.parameters(), lr=0.01)
        adapter = make_adapter(tested_optimizer)
        samples = [
            torch.tensor([[0.5, -0.25, 0.75, 1.0]], dtype=torch.float32),
            torch.tensor([[-0.2, 0.3, 0.4, -0.8]], dtype=torch.float32),
            torch.tensor([[0.9, 0.1, -0.5, 0.2]], dtype=torch.float32),
        ]

        first_sample, *remaining_samples = samples
        run_adam_step(baseline_model, baseline_optimizer, first_sample)
        run_adam_step(tested_model, tested_optimizer, first_sample)
        adapter.register_optimizer_state()
        adapter.offload_state().wait()

        for sample in remaining_samples:
            run_adam_step(baseline_model, baseline_optimizer, sample)
            adapter.prefetch_state().wait()
            run_adam_step(tested_model, tested_optimizer, sample)
            adapter.offload_state().wait()

        for baseline_param, tested_param in zip(
            baseline_model.parameters(),
            tested_model.parameters(),
        ):
            self.assertTrue(torch.allclose(baseline_param, tested_param))
        assert_optimizer_tensor_state_close(
            self,
            tested_optimizer,
            clone_optimizer_tensor_state(baseline_optimizer),
        )

    @unittest.skipIf(torch is None, "PyTorch is not installed")
    def test_non_tensor_state_is_skipped(self) -> None:
        parameter = torch.nn.Parameter(torch.tensor([1.0]))
        optimizer = torch.optim.SGD([parameter], lr=0.1)
        optimizer.state[parameter]["metadata"] = "not-a-tensor"
        optimizer.state[parameter]["momentum_buffer"] = torch.ones_like(parameter)
        adapter = make_adapter(optimizer)

        adapter.register_optimizer_state()

        self.assertEqual(adapter.names(), ["optimizer_state/param_0/momentum_buffer"])

    @unittest.skipIf(torch is None, "PyTorch is not installed")
    def test_registry_rebuild_tracks_new_sgd_momentum_state(self) -> None:
        parameter = torch.nn.Parameter(torch.tensor([1.0]))
        optimizer = torch.optim.SGD([parameter], lr=0.1, momentum=0.9)
        registry = TorchOptimizerStateRegistry(optimizer)

        self.assertEqual(registry.rebuild(), ())
        loss = parameter.pow(2).sum()
        loss.backward()
        optimizer.step()

        names = [bucket.name for bucket in registry.rebuild()]
        self.assertEqual(names, ["optimizer_state/param_0/momentum_buffer"])

    @unittest.skipIf(torch is None, "PyTorch is not installed")
    def test_transaction_step_matches_baseline_adamw(self) -> None:
        baseline_model = make_model()
        tested_model = copy.deepcopy(baseline_model)
        baseline_optimizer = torch.optim.AdamW(baseline_model.parameters(), lr=0.01)
        tested_optimizer = torch.optim.AdamW(tested_model.parameters(), lr=0.01)
        adapter = make_adapter(tested_optimizer)
        samples = [
            torch.tensor([[0.5, -0.25, 0.75, 1.0]], dtype=torch.float32),
            torch.tensor([[-0.2, 0.3, 0.4, -0.8]], dtype=torch.float32),
            torch.tensor([[0.9, 0.1, -0.5, 0.2]], dtype=torch.float32),
        ]

        for sample in samples:
            run_step_with_existing_grad(baseline_model, baseline_optimizer, sample)
            tested_optimizer.zero_grad()
            loss = tested_model(sample).pow(2).sum()
            loss.backward()
            adapter.step()

        for baseline_param, tested_param in zip(
            baseline_model.parameters(),
            tested_model.parameters(),
        ):
            self.assertTrue(torch.allclose(baseline_param, tested_param))
        assert_optimizer_tensor_state_close(
            self,
            tested_optimizer,
            clone_optimizer_tensor_state(baseline_optimizer),
        )

    @unittest.skipIf(torch is None, "PyTorch is not installed")
    def test_transaction_rolls_back_on_optimizer_error(self) -> None:
        model = make_model()
        optimizer = FailingOptimizer(model.parameters(), lr=0.01)
        run_adam_step(model, optimizer.inner)
        adapter = TorchOptimizerTransactionAdapter(
            optimizer,
            make_core_for_optimizer(),
        )
        adapter.register_optimizer_state()
        adapter.offload_state().wait()
        expected = clone_optimizer_tensor_state(optimizer.inner)

        with self.assertRaisesRegex(RuntimeError, "boom"):
            adapter.step()

        assert_optimizer_tensor_state_close(self, optimizer.inner, expected)


def make_model():
    torch.manual_seed(7)
    return torch.nn.Linear(4, 2)


def run_adam_step(model, optimizer, sample=None) -> None:
    if sample is None:
        sample = torch.tensor([[0.25, -0.5, 1.0, 0.75]], dtype=torch.float32)
    optimizer.zero_grad()
    loss = model(sample).pow(2).sum()
    loss.backward()
    optimizer.step()


def make_adapter(optimizer) -> TorchOptimizerTransactionAdapter:
    return TorchOptimizerTransactionAdapter(optimizer, make_core_for_optimizer())


def make_core_for_optimizer():
    return make_optimizer_state_offload(
        FakeRuntimeSession(),
        object(),
        object(),
        intent_prefix="torch-optimizer-intent",
    )


def run_step_with_existing_grad(model, optimizer, sample) -> None:
    optimizer.zero_grad()
    loss = model(sample).pow(2).sum()
    loss.backward()
    optimizer.step()


class FailingOptimizer:
    def __init__(self, params, *, lr: float) -> None:
        self.inner = torch.optim.Adam(params, lr=lr)
        self.state = self.inner.state
        self.param_groups = self.inner.param_groups

    def step(self, closure=None):
        raise RuntimeError("boom")

    def zero_grad(self):
        return self.inner.zero_grad()


def clone_optimizer_tensor_state(optimizer) -> list[dict[str, torch.Tensor]]:
    snapshots: list[dict[str, torch.Tensor]] = []
    for group in optimizer.param_groups:
        for param in group["params"]:
            state = optimizer.state.get(param, {})
            snapshots.append(
                {
                    str(key): value.detach().cpu().clone()
                    for key, value in state.items()
                    if isinstance(value, torch.Tensor)
                }
            )
    return snapshots


def assert_optimizer_tensor_state_close(
    test_case: unittest.TestCase,
    optimizer,
    expected: list[dict[str, torch.Tensor]],
) -> None:
    actual = clone_optimizer_tensor_state(optimizer)
    test_case.assertEqual(
        [[*state] for state in actual],
        [[*state] for state in expected],
    )
    for actual_state, expected_state in zip(actual, expected):
        for key, expected_tensor in expected_state.items():
            test_case.assertTrue(
                torch.allclose(actual_state[key], expected_tensor),
                msg=f"optimizer state mismatch: {key}",
            )


if __name__ == "__main__":
    unittest.main()
