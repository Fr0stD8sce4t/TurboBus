from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import tempfile
import time
import unittest

from turbobus import native_plan, native_runtime, profile as runtime_profile
from turbobus import tensor_validation
from turbobus.planner_engine import PlannerEngine
from turbobus.runtime_engine import RuntimeOptions, TransferHandle


class NativeHandle:
    def __init__(self, handle_id: int = 1) -> None:
        self.id = handle_id


class SuccessfulRuntime:
    def __init__(self) -> None:
        self.wait_calls = 0

    def wait(self, handle: TransferHandle) -> None:
        self.wait_calls += 1


class FailingRuntime:
    def wait(self, handle: TransferHandle) -> None:
        raise RuntimeError("simulated wait failure")


class TransferHandleTest(unittest.TestCase):
    def test_wait_marks_complete(self) -> None:
        runtime = SuccessfulRuntime()
        handle = TransferHandle(runtime, NativeHandle())

        handle.wait()
        handle.wait()

        self.assertEqual(handle.status, "complete")
        self.assertTrue(handle.done)
        self.assertEqual(runtime.wait_calls, 1)

    def test_wait_failure_marks_failed(self) -> None:
        handle = TransferHandle(FailingRuntime(), NativeHandle())

        with self.assertRaises(RuntimeError):
            handle.wait()

        self.assertEqual(handle.status, "failed")
        self.assertEqual(handle.error, "simulated wait failure")


class RuntimeOptionsTest(unittest.TestCase):
    def test_from_tuning_json_reads_best_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "tune.json"
            path.write_text(
                '{"best": {"chunk_bytes": 4194304, "staging_slots": 3}}',
                encoding="utf-8",
            )

            options = RuntimeOptions.from_tuning_json(path)

        self.assertEqual(options.chunk_bytes, 4194304)
        self.assertEqual(options.staging_slots, 3)

    def test_from_profile_json_reads_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "profile.json"
            path.write_text(
                '{"config": {"chunk_bytes": 8388608, "profile_bytes": 16777216}}',
                encoding="utf-8",
            )

            options = RuntimeOptions.from_profile_json(path)

        self.assertEqual(options.chunk_bytes, 8388608)
        self.assertEqual(options.profile_bytes, 16777216)
        self.assertEqual(options.staging_slots, 2)


class DaemonProfileTest(unittest.TestCase):
    def test_profile_to_daemon_dict_serializes_backend_profile(self) -> None:
        relay = SimpleNamespace(
            relay_device=1,
            target_device=0,
            h2d_bw_gbps=7.6,
            d2h_bw_gbps=8.6,
            p2p_bw_gbps=40.0,
            effective_bw_gbps=7.6,
            effective_d2h_bw_gbps=8.6,
            p2p_enabled=True,
        )
        profile = SimpleNamespace(
            target_device=0,
            direct_h2d_bw_gbps=7.5,
            direct_d2h_bw_gbps=8.5,
            relays=[relay],
        )

        payload = runtime_profile.profile_to_daemon_dict(profile)

        self.assertEqual(payload["target_device"], 0)
        self.assertEqual(payload["direct_h2d_bw_gbps"], 7.5)
        self.assertEqual(payload["direct_d2h_bw_gbps"], 8.5)
        self.assertEqual(payload["relays"][0]["relay_device"], 1)
        self.assertTrue(payload["relays"][0]["p2p_enabled"])

    def test_profile_from_daemon_entry_rejects_missing_direct_bandwidth(self) -> None:
        entry = {
            "updated_at": time.time(),
            "profile": {
                "target_device": 0,
                "direct_h2d_bw_gbps": 0.0,
                "relays": [],
            },
        }

        with self.assertRaisesRegex(ValueError, "direct_h2d"):
            runtime_profile.profile_from_daemon_entry(entry, target_gpu=0)

    def test_daemon_profile_freshness_uses_updated_at(self) -> None:
        self.assertTrue(
            runtime_profile.daemon_profile_is_fresh(
                {"updated_at": time.time()},
                max_age_seconds=60.0,
            )
        )
        self.assertFalse(
            runtime_profile.daemon_profile_is_fresh(
                {"updated_at": time.time() - 3600.0},
                max_age_seconds=60.0,
            )
        )


class RangeAndPlanConversionTest(unittest.TestCase):
    def test_range_fields_accepts_dicts_tuples_and_objects(self) -> None:
        self.assertEqual(
            native_plan.range_fields({"src_offset": 1, "dst_offset": 2, "bytes": 3}),
            (1, 2, 3),
        )
        self.assertEqual(native_plan.range_fields((4, 5, 6)), (4, 5, 6))
        self.assertEqual(
            native_plan.range_fields(
                SimpleNamespace(src_offset=7, dst_offset=8, bytes=9)
            ),
            (7, 8, 9),
        )

    def test_native_ranges_accepts_dicts_and_tuples(self) -> None:
        class NativeRange:
            def __init__(self) -> None:
                self.src_offset = 0
                self.dst_offset = 0
                self.bytes = 0

        old_extension = native_runtime._turbobus
        native_runtime._turbobus = type("Ext", (), {"TransferRange": NativeRange})
        try:
            ranges = native_plan.native_ranges(
                [
                    {"src_offset": 0, "dst_offset": 16, "bytes": 8},
                    (32, 64, 8),
                ],
                source_bytes=128,
                destination_bytes=128,
            )
        finally:
            native_runtime._turbobus = old_extension

        self.assertEqual(len(ranges), 2)
        self.assertEqual(ranges[0].src_offset, 0)
        self.assertEqual(ranges[0].dst_offset, 16)
        self.assertEqual(ranges[1].src_offset, 32)
        self.assertEqual(ranges[1].dst_offset, 64)

    def test_native_ranges_rejects_out_of_bounds(self) -> None:
        class NativeRange:
            pass

        old_extension = native_runtime._turbobus
        native_runtime._turbobus = type("Ext", (), {"TransferRange": NativeRange})
        try:
            with self.assertRaises(ValueError):
                native_plan.native_ranges(
                    [(120, 0, 16)],
                    source_bytes=128,
                    destination_bytes=128,
                )
        finally:
            native_runtime._turbobus = old_extension

    def test_native_transfer_plan_preserves_daemon_assignments(self) -> None:
        class NativePlan:
            def __init__(self) -> None:
                self.total_bytes = 0
                self.chunk_bytes = 0
                self.assignments = []

        class NativeAssignment:
            def __init__(self) -> None:
                self.path = None
                self.chunks = []

        class NativePath:
            def __init__(self) -> None:
                self.kind_value = None
                self.direction_value = None
                self.target_device = -1
                self.relay_device = -1
                self.h2d_bw_gbps = 0.0
                self.d2h_bw_gbps = 0.0
                self.p2p_bw_gbps = 0.0
                self.effective_bw_gbps = 0.0
                self.enabled = False

        class NativeChunk:
            def __init__(self) -> None:
                self.src_offset = 0
                self.dst_offset = 0
                self.bytes = 0

        class PathKind:
            RelayH2DThenP2P = "relay-h2d"
            RelayP2PThenD2H = "relay-d2h"
            DirectH2D = "direct-h2d"
            DirectD2H = "direct-d2h"

        class TransferDirection:
            H2D = "h2d"
            D2H = "d2h"

        old_extension = native_runtime._turbobus
        native_runtime._turbobus = type(
            "Ext",
            (),
            {
                "TransferPlan": NativePlan,
                "PathAssignment": NativeAssignment,
                "Path": NativePath,
                "Chunk": NativeChunk,
                "PathKind": PathKind,
                "TransferDirection": TransferDirection,
            },
        )
        try:
            plan = native_plan.native_transfer_plan(
                {
                    "total_bytes": 16,
                    "chunk_bytes": 16,
                    "assignments": [
                        {
                            "path": {
                                "kind": "relay",
                                "direction": "h2d",
                                "target_device": 0,
                                "relay_device": 1,
                                "h2d_bw_gbps": 12.0,
                                "p2p_bw_gbps": 50.0,
                                "effective_bw_gbps": 10.0,
                                "enabled": True,
                            },
                            "chunks": [
                                {"src_offset": 0, "dst_offset": 8, "bytes": 16},
                            ],
                        }
                    ],
                }
            )
        finally:
            native_runtime._turbobus = old_extension

        self.assertEqual(plan.total_bytes, 16)
        self.assertEqual(plan.chunk_bytes, 16)
        self.assertEqual(len(plan.assignments), 1)
        assignment = plan.assignments[0]
        self.assertEqual(assignment.path.kind_value, "relay-h2d")
        self.assertEqual(assignment.path.direction_value, "h2d")
        self.assertEqual(assignment.path.relay_device, 1)
        self.assertEqual(assignment.chunks[0].src_offset, 0)
        self.assertEqual(assignment.chunks[0].dst_offset, 8)
        self.assertEqual(assignment.chunks[0].bytes, 16)

    def test_native_transfer_plan_accepts_python_planner_model(self) -> None:
        class NativePlan:
            def __init__(self) -> None:
                self.total_bytes = 0
                self.chunk_bytes = 0
                self.assignments = []

        class NativeAssignment:
            def __init__(self) -> None:
                self.path = None
                self.chunks = []

        class NativePath:
            def __init__(self) -> None:
                self.kind_value = None
                self.direction_value = None
                self.target_device = -1
                self.relay_device = -1
                self.h2d_bw_gbps = 0.0
                self.d2h_bw_gbps = 0.0
                self.p2p_bw_gbps = 0.0
                self.effective_bw_gbps = 0.0
                self.enabled = False

        class NativeChunk:
            def __init__(self) -> None:
                self.src_offset = 0
                self.dst_offset = 0
                self.bytes = 0

        class PathKind:
            RelayH2DThenP2P = "relay-h2d"
            RelayP2PThenD2H = "relay-d2h"
            DirectH2D = "direct-h2d"
            DirectD2H = "direct-d2h"

        class TransferDirection:
            H2D = "h2d"
            D2H = "d2h"

        profile = SimpleNamespace(
            target_device=0,
            direct_h2d_bw_gbps=1.0,
            direct_d2h_bw_gbps=1.0,
            relays=[
                SimpleNamespace(
                    relay_device=1,
                    target_device=0,
                    h2d_bw_gbps=8.0,
                    d2h_bw_gbps=7.0,
                    p2p_bw_gbps=40.0,
                    effective_bw_gbps=8.0,
                    effective_d2h_bw_gbps=7.0,
                    p2p_enabled=True,
                )
            ],
        )
        planner_plan = PlannerEngine().plan(
            total_bytes=64,
            chunk_bytes=16,
            profile=profile,
            mode="pool",
        )

        old_extension = native_runtime._turbobus
        native_runtime._turbobus = type(
            "Ext",
            (),
            {
                "TransferPlan": NativePlan,
                "PathAssignment": NativeAssignment,
                "Path": NativePath,
                "Chunk": NativeChunk,
                "PathKind": PathKind,
                "TransferDirection": TransferDirection,
            },
        )
        try:
            native = native_plan.native_transfer_plan(planner_plan)
        finally:
            native_runtime._turbobus = old_extension

        self.assertEqual(native.total_bytes, 64)
        self.assertEqual(native.chunk_bytes, 16)
        self.assertEqual(
            {assignment.path.kind_value for assignment in native.assignments},
            {"direct-h2d", "relay-h2d"},
        )
        self.assertEqual(
            sum(chunk.bytes for assignment in native.assignments for chunk in assignment.chunks),
            64,
        )

    def test_native_transfer_plan_rejects_total_byte_mismatch(self) -> None:
        class NativePlan:
            def __init__(self) -> None:
                self.total_bytes = 0
                self.chunk_bytes = 0
                self.assignments = []

        class NativeAssignment:
            def __init__(self) -> None:
                self.path = None
                self.chunks = []

        class NativePath:
            pass

        class NativeChunk:
            pass

        class PathKind:
            RelayH2DThenP2P = "relay-h2d"
            RelayP2PThenD2H = "relay-d2h"
            DirectH2D = "direct-h2d"
            DirectD2H = "direct-d2h"

        class TransferDirection:
            H2D = "h2d"
            D2H = "d2h"

        old_extension = native_runtime._turbobus
        native_runtime._turbobus = type(
            "Ext",
            (),
            {
                "TransferPlan": NativePlan,
                "PathAssignment": NativeAssignment,
                "Path": NativePath,
                "Chunk": NativeChunk,
                "PathKind": PathKind,
                "TransferDirection": TransferDirection,
            },
        )
        try:
            with self.assertRaisesRegex(
                ValueError,
                "total_bytes must match assigned chunk bytes",
            ):
                native_plan.native_transfer_plan(
                    {
                        "total_bytes": 32,
                        "chunk_bytes": 16,
                        "assignments": [
                            {
                                "path": {
                                    "kind": "relay",
                                    "direction": "h2d",
                                    "target_device": 0,
                                    "relay_device": 1,
                                    "enabled": True,
                                },
                                "chunks": [
                                    {
                                        "src_offset": 0,
                                        "dst_offset": 0,
                                        "bytes": 16,
                                    },
                                ],
                            }
                        ],
                    }
                )
        finally:
            native_runtime._turbobus = old_extension

    def test_native_transfer_plan_rejects_disabled_path(self) -> None:
        class NativePlan:
            pass

        old_extension = native_runtime._turbobus
        native_runtime._turbobus = type("Ext", (), {"TransferPlan": NativePlan})
        try:
            with self.assertRaisesRegex(ValueError, "disabled path"):
                native_plan.native_transfer_plan(
                    {
                        "total_bytes": 16,
                        "chunk_bytes": 16,
                        "assignments": [
                            {
                                "path": {"kind": "direct", "direction": "h2d", "enabled": False},
                                "chunks": [{"src_offset": 0, "dst_offset": 0, "bytes": 16}],
                            }
                        ],
                    }
                )
        finally:
            native_runtime._turbobus = old_extension

    def test_native_transfer_plan_rejects_invalid_chunk_range(self) -> None:
        class NativePlan:
            def __init__(self) -> None:
                self.total_bytes = 0
                self.chunk_bytes = 0
                self.assignments = []

        class NativeAssignment:
            pass

        class NativePath:
            pass

        class NativeChunk:
            pass

        class PathKind:
            DirectH2D = "direct-h2d"
            RelayH2DThenP2P = "relay-h2d"
            DirectD2H = "direct-d2h"
            RelayP2PThenD2H = "relay-d2h"

        class TransferDirection:
            H2D = "h2d"
            D2H = "d2h"

        old_extension = native_runtime._turbobus
        native_runtime._turbobus = type(
            "Ext",
            (),
            {
                "TransferPlan": NativePlan,
                "PathAssignment": NativeAssignment,
                "Path": NativePath,
                "Chunk": NativeChunk,
                "PathKind": PathKind,
                "TransferDirection": TransferDirection,
            },
        )
        try:
            with self.assertRaisesRegex(ValueError, "chunk offsets"):
                native_plan.native_transfer_plan(
                    {
                        "total_bytes": 16,
                        "chunk_bytes": 16,
                        "assignments": [
                            {
                                "path": {"kind": "direct", "direction": "h2d", "enabled": True},
                                "chunks": [{"src_offset": 0, "dst_offset": 0, "bytes": 0}],
                            }
                        ],
                    }
                )
        finally:
            native_runtime._turbobus = old_extension

    def test_native_transfer_plan_rejects_direction_kind_mismatch(self) -> None:
        class NativePlan:
            def __init__(self) -> None:
                self.total_bytes = 0
                self.chunk_bytes = 0
                self.assignments = []

        class NativeAssignment:
            pass

        class NativePath:
            pass

        class NativeChunk:
            pass

        class PathKind:
            DirectH2D = "direct-h2d"
            RelayH2DThenP2P = "relay-h2d"
            DirectD2H = "direct-d2h"
            RelayP2PThenD2H = "relay-d2h"

        class TransferDirection:
            H2D = "h2d"
            D2H = "d2h"

        old_extension = native_runtime._turbobus
        native_runtime._turbobus = type(
            "Ext",
            (),
            {
                "TransferPlan": NativePlan,
                "PathAssignment": NativeAssignment,
                "Path": NativePath,
                "Chunk": NativeChunk,
                "PathKind": PathKind,
                "TransferDirection": TransferDirection,
            },
        )
        try:
            with self.assertRaisesRegex(ValueError, "unsupported transfer plan path"):
                native_plan.native_transfer_plan(
                    {
                        "total_bytes": 16,
                        "chunk_bytes": 16,
                        "assignments": [
                            {
                                "path": {"kind": "sideways", "direction": "h2d", "enabled": True},
                                "chunks": [{"src_offset": 0, "dst_offset": 0, "bytes": 16}],
                            }
                        ],
                    }
                )
        finally:
            native_runtime._turbobus = old_extension

    def test_range_tensor_validation_does_not_require_equal_sizes_for_d2h(self) -> None:
        class TensorType:
            pass

        class FakeDevice:
            def __init__(self, type_: str, index: int | None = None) -> None:
                self.type = type_
                self.index = index

        class FakeTensor(TensorType):
            def __init__(
                self,
                numel: int,
                *,
                device_type: str,
                device_index: int | None = None,
                pinned: bool = False,
            ) -> None:
                self._numel = numel
                self.device = FakeDevice(device_type, device_index)
                self._pinned = pinned

            def numel(self) -> int:
                return self._numel

            def element_size(self) -> int:
                return 1

            def is_pinned(self) -> bool:
                return self._pinned

            def is_contiguous(self) -> bool:
                return True

        old_torch = tensor_validation.torch
        tensor_validation.torch = type("Torch", (), {"Tensor": TensorType})
        try:
            cpu = FakeTensor(128, device_type="cpu", pinned=True)
            gpu = FakeTensor(1024, device_type="cuda", device_index=6)

            source_bytes, destination_bytes = tensor_validation.validate_range_tensors(
                cpu,
                gpu,
                target_gpu=6,
                direction="d2h",
            )
        finally:
            tensor_validation.torch = old_torch

        self.assertEqual(source_bytes, 1024)
        self.assertEqual(destination_bytes, 128)


if __name__ == "__main__":
    unittest.main()
