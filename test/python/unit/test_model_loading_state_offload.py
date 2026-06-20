from __future__ import annotations

import unittest

from turbobus.model_manifest import ModelWeightManifest, ModelWeightTensor
from turbobus.state_offload import (
    ModelWeightStateRegistry,
    make_model_weight_state_offload,
    model_weight_manifest_extra_evidence,
)
from test.python.fixtures.runtime_evidence import FakeRuntimeSession


class FakeTensor:
    def __init__(self, bytes_: int) -> None:
        self._bytes = bytes_

    def numel(self) -> int:
        return self._bytes

    def element_size(self) -> int:
        return 1


class ModelWeightStateOffloadTest(unittest.TestCase):
    def test_model_weight_registry_drives_lifecycle_evidence(self) -> None:
        cpu_buffer = FakeTensor(256)
        gpu_buffer = object()
        registry = ModelWeightStateRegistry(
            ModelWeightManifest(
                (
                    ModelWeightTensor(
                        name="layer.weight",
                        dtype="float16",
                        shape=(2, 2),
                        byte_count=64,
                        cpu_offset=0,
                        gpu_offset=0,
                    ),
                )
            ),
            cpu_buffer=cpu_buffer,
            gpu_buffer=gpu_buffer,
        )
        core = make_model_weight_state_offload(
            FakeRuntimeSession(),
            cpu_buffer,
            gpu_buffer,
            intent_prefix="model-weight-state",
            extra_evidence=model_weight_manifest_extra_evidence(registry),
        )

        core.register_registry(registry)
        core.submit_prefetch_states(["layer.weight"], operation="load_batch").wait()

        self.assertEqual(core.names(), ["layer.weight"])
        self.assertEqual(core.spec.state_kind, "model_weights")
        self.assertEqual(core.last_transfer_lifecycle.evidence["manifest_tensor_count"], 1)
        self.assertEqual(
            core.last_transfer_lifecycle.evidence["manifest_tensor_names"],
            ["layer.weight"],
        )


if __name__ == "__main__":
    unittest.main()
