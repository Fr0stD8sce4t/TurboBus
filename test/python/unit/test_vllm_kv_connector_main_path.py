from __future__ import annotations

from dataclasses import dataclass
import unittest

from turbobus.adapters.vllm_kv_connector import (
    KVConnectorRole,
    TurboBusConnector,
    TurboBusConnectorMetadata,
    TurboBusRequestMetadata,
    _receipt_trace_from_receipts,
    clear_connector_events,
    clear_saved_prefixes,
    get_saved_prefix,
)
from turbobus.adapters.vllm_events import get_connector_events
from turbobus.adapters.vllm_prefix_store import store_saved_prefix
from turbobus.schema import TransferIntent, TransferReceipt, TransferStatusState


class TurboBusKVConnectorMainPathTest(unittest.TestCase):
    def setUp(self) -> None:
        clear_saved_prefixes()
        clear_connector_events()

    def tearDown(self) -> None:
        clear_saved_prefixes()
        clear_connector_events()

    def test_save_kv_generates_transfer_intent_and_stores_receipt_trace(self) -> None:
        client = FakeTurboBusClient()
        connector = make_connector(client, restore_enabled=True)
        kv_layer = FakeTensor(shape=(2, 4, 8), strides=(32, 8, 1), itemsize=1)
        connector.register_kv_caches({"layer.0": kv_layer})
        metadata = TurboBusConnectorMetadata()
        metadata.add_save_request(
            TurboBusRequestMetadata(
                request_id="request-save",
                prefix_key="prefix-a",
                block_ids=(1, 2),
                matched_tokens=32,
                block_count=2,
            )
        )
        connector.bind_connector_metadata(metadata)
        connector._backing_pool = FakeBackingPool()

        connector.save_kv_layer("layer.0", kv_layer, attn_metadata=None)
        connector.wait_for_save()

        self.assertEqual(len(client.submitted), 1)
        intent = client.submitted[0]
        self.assertEqual(intent.direction, "d2h")
        self.assertEqual(intent.workload_kind.value, "kv_cache")
        self.assertEqual(intent.job_id, "job-vllm")
        self.assertEqual(intent.session_id, "session-vllm")
        self.assertEqual(intent.source_buffer_id, "gpu-buffer")
        self.assertEqual(intent.destination_buffer_id, "cpu-buffer")
        self.assertEqual(intent.total_bytes, 32)
        self.assertEqual(intent.metadata["vllm_operation"], "save")
        self.assertEqual(intent.metadata["vllm_lifecycle"], "save_kv_layer")
        self.assertEqual(intent.metadata["prefix_key"], "prefix-a")

        saved = get_saved_prefix("prefix-a", "session-vllm")
        self.assertIsNotNone(saved)
        assert saved is not None
        self.assertEqual(saved.source_request_id, "request-save")
        self.assertEqual(saved.receipt_ids, "receipt-1")
        self.assertEqual(saved.decision_ids, "decision-1")
        self.assertEqual(saved.ticket_ids, "ticket-1")
        self.assertEqual(saved.fallback_reason, "relay quota fallback")
        self.assertEqual(saved.direct_bytes, 16)
        self.assertEqual(saved.relay_bytes, 16)

    def test_restore_kv_uses_saved_prefix_and_records_new_receipt_trace(self) -> None:
        client = FakeTurboBusClient()
        connector = make_connector(client, restore_enabled=True)
        kv_layer = FakeTensor(shape=(2, 4, 8), strides=(32, 8, 1), itemsize=1)
        connector.register_kv_caches({"layer.0": kv_layer})
        saved_prefix = make_saved_prefix(
            key="prefix-a",
            cpu_backings=[FakeBackingTensor("cpu-layer-0")],
            source_request_id="request-save",
        )
        connector._store_prefix(saved_prefix)
        store_saved_prefix(saved_prefix)
        clear_connector_events()
        metadata = TurboBusConnectorMetadata()
        metadata.add_request(
            TurboBusRequestMetadata(
                request_id="request-restore",
                prefix_key="prefix-a",
                block_ids=(1, 2),
                matched_tokens=32,
                block_count=2,
            )
        )
        connector.bind_connector_metadata(metadata)

        connector.start_load_kv(forward_context=None)

        self.assertEqual(len(client.submitted), 1)
        intent = client.submitted[0]
        self.assertEqual(intent.direction, "h2d")
        self.assertEqual(intent.workload_kind.value, "kv_cache")
        self.assertEqual(intent.source_buffer_id, "cpu-buffer")
        self.assertEqual(intent.destination_buffer_id, "gpu-buffer")
        self.assertEqual(intent.total_bytes, 32)
        self.assertEqual(intent.metadata["vllm_operation"], "restore")
        self.assertEqual(intent.metadata["source_request_id"], "request-save")

        restore_events = [
            event for event in get_connector_events() if event["event"] == "restore"
        ]
        self.assertEqual(len(restore_events), 1)
        self.assertEqual(restore_events[0]["receipt_ids"], "receipt-1")
        self.assertEqual(restore_events[0]["decision_ids"], "decision-1")
        self.assertEqual(restore_events[0]["ticket_ids"], "ticket-1")
        self.assertEqual(restore_events[0]["fallback_reason"], "relay quota fallback")
        self.assertEqual(restore_events[0]["direct_bytes"], 16)
        self.assertEqual(restore_events[0]["relay_bytes"], 16)

    def test_prefix_hit_and_miss_are_stable(self) -> None:
        connector = make_connector(FakeTurboBusClient(), restore_enabled=True)
        miss_request = FakeRequest(
            request_id="request-miss",
            num_tokens=32,
            kv_transfer_params={
                "turbobus.do_restore": True,
                "turbobus.prefix_key": "missing",
                "turbobus.matched_tokens": 32,
            },
        )

        self.assertEqual(connector.get_num_new_matched_tokens(miss_request, 0), (0, False))
        miss_events = [
            event for event in get_connector_events() if event["event"] == "match_miss"
        ]
        self.assertEqual(len(miss_events), 1)
        self.assertEqual(miss_events[0]["prefix_key"], "missing")

        saved_prefix = make_saved_prefix(
            key="present",
            cpu_backings=[FakeBackingTensor("cpu-layer-0")],
            matched_tokens=32,
        )
        connector._store_prefix(saved_prefix)
        store_saved_prefix(saved_prefix)
        clear_connector_events()
        hit_request = FakeRequest(
            request_id="request-hit",
            num_tokens=64,
            kv_transfer_params={
                "turbobus.do_restore": True,
                "turbobus.prefix_key": "present",
                "turbobus.matched_tokens": 32,
            },
        )

        self.assertEqual(connector.get_num_new_matched_tokens(hit_request, 8), (24, True))
        hit_events = [
            event for event in get_connector_events() if event["event"] == "match"
        ]
        self.assertEqual(len(hit_events), 1)
        self.assertEqual(hit_events[0]["prefix_key"], "present")
        self.assertEqual(hit_events[0]["available_tokens"], 24)

    def test_receipt_trace_rejects_unverified_complete_receipt(self) -> None:
        intent = make_intent()
        receipt = make_receipt(intent, metadata=unverified_metadata())

        with self.assertRaisesRegex(ValueError, "verification evidence"):
            _receipt_trace_from_receipts([receipt])


def make_connector(client: "FakeTurboBusClient", *, restore_enabled: bool) -> TurboBusConnector:
    connector = TurboBusConnector(make_vllm_config(restore_enabled), KVConnectorRole.WORKER)
    connector.client = client
    return connector


def make_vllm_config(restore_enabled: bool):
    return SimpleObject(
        cache_config=SimpleObject(block_size=16),
        kv_transfer_config=FakeKVTransferConfig(
            {
                "turbobus.job_id": "job-vllm",
                "turbobus.session_id": "session-vllm",
                "turbobus.cpu_buffer_id": "cpu-buffer",
                "turbobus.gpu_buffer_id": "gpu-buffer",
                "turbobus.chunk_bytes": 16,
                "turbobus.restore_enabled": restore_enabled,
                "turbobus.wait_timeout_seconds": 0,
            }
        ),
    )


def make_saved_prefix(
    *,
    key: str,
    cpu_backings: list[object],
    source_request_id: str = "request-save",
    matched_tokens: int = 32,
):
    from turbobus.adapters.vllm_prefix_store import TurboBusSavedPrefix

    return TurboBusSavedPrefix(
        key=key,
        cpu_backings=cpu_backings,
        block_count=2,
        matched_tokens=matched_tokens,
        session_id="session-vllm",
        source_request_id=source_request_id,
        bytes=32,
        direct_chunks=1,
        relay_chunks=1,
        direct_bytes=16,
        relay_bytes=16,
        receipt_ids="receipt-save",
        decision_ids="decision-save",
        topology_snapshot_ids="topology-save",
        ticket_ids="ticket-save",
        fallback_reason="relay quota fallback",
    )


class FakeTurboBusClient:
    def __init__(self) -> None:
        self.submitted: list[TransferIntent] = []
        self.receipts: dict[str, TransferReceipt] = {}

    def submit_transfer_intent(self, intent: TransferIntent) -> TransferReceipt:
        self.submitted.append(intent)
        receipt = make_receipt(
            intent,
            receipt_id=f"receipt-{len(self.submitted)}",
            ticket_id=f"ticket-{len(self.submitted)}",
            decision_id=f"decision-{len(self.submitted)}",
            topology_snapshot_id=f"topology-{len(self.submitted)}",
        )
        self.receipts[intent.intent_id] = receipt
        return receipt

    def wait_transfer_receipt(
        self,
        intent_id: str,
        timeout_seconds: float | None = None,
    ) -> TransferReceipt:
        try:
            return self.receipts[intent_id]
        except KeyError as exc:
            raise KeyError(intent_id) from exc


class FakeKVTransferConfig:
    engine_id = "session-vllm"

    def __init__(self, values: dict[str, object]) -> None:
        self.values = dict(values)

    def get_from_extra_config(self, key: str, default):
        return self.values.get(key, default)


class SimpleObject:
    def __init__(self, **fields) -> None:
        self.__dict__.update(fields)


@dataclass
class FakeRequest:
    request_id: str
    num_tokens: int
    kv_transfer_params: dict[str, object]


@dataclass
class FakeBackingTensor:
    name: str


class FakeBackingPool:
    def acquire(self, block_count: int, kv_caches: list[object]):
        return [FakeBackingTensor(f"cpu-layer-{index}") for index, _ in enumerate(kv_caches)], False


class FakeTensor:
    def __init__(self, *, shape: tuple[int, ...], strides: tuple[int, ...], itemsize: int) -> None:
        self.shape = shape
        self._strides = strides
        self._itemsize = itemsize

    def stride(self, index: int) -> int:
        return self._strides[index]

    def element_size(self) -> int:
        return self._itemsize


def make_intent() -> TransferIntent:
    return TransferIntent(
        intent_id="intent-test",
        job_id="job-vllm",
        session_id="session-vllm",
        source_buffer_id="cpu-buffer",
        destination_buffer_id="gpu-buffer",
        direction="h2d",
        total_bytes=32,
        ranges=({"src_offset": 0, "dst_offset": 0, "bytes": 32},),
        workload_kind="kv_cache",
    )


def make_receipt(
    intent: TransferIntent,
    *,
    receipt_id: str = "receipt-1",
    ticket_id: str = "ticket-1",
    decision_id: str = "decision-1",
    topology_snapshot_id: str = "topology-1",
    metadata: dict[str, object] | None = None,
) -> TransferReceipt:
    return TransferReceipt(
        receipt_id=receipt_id,
        ticket_id=ticket_id,
        intent_id=intent.intent_id,
        decision_id=decision_id,
        topology_snapshot_id=topology_snapshot_id,
        job_id=intent.job_id,
        session_id=intent.session_id,
        state=TransferStatusState.COMPLETE,
        bytes_total=intent.total_bytes,
        bytes_completed=intent.total_bytes,
        path_stats=(
            {"kind": "direct", "bytes": intent.total_bytes // 2, "chunk_count": 1},
            {
                "kind": "relay",
                "bytes": intent.total_bytes - intent.total_bytes // 2,
                "chunk_count": 1,
            },
        ),
        metadata=verified_metadata(intent) if metadata is None else metadata,
    )


def verified_metadata(intent: TransferIntent) -> dict[str, object]:
    return {
        "fallback_reason": "relay quota fallback",
        "completion_source": "worker",
        "executed": True,
        "verified": True,
        "verified_bytes": intent.total_bytes,
        "content_match": True,
        "verification_source": "fixture_worker",
        "verification_method": "fixture_compare",
    }


def unverified_metadata() -> dict[str, object]:
    return {
        "completion_source": "worker",
        "executed": True,
        "verified": False,
        "verified_bytes": 0,
        "content_match": False,
    }


if __name__ == "__main__":
    unittest.main()
