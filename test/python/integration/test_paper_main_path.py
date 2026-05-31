from __future__ import annotations

import unittest

from turbobus.api import TurboBusClient
from turbobus.client import CudaIpcDeviceBuffer, SharedPinnedCpuBufferAllocator
from turbobus.client_transfer import (
    WorkerIntentTransferExecutor,
    make_worker_managed_transfer_client,
)
from turbobus.daemon.protocol import WorkerTransferAuthorizationRequest
from turbobus.daemon.server import TurboBusDaemon
from turbobus.schema import TransferIntent, TransferReceipt, TransferStatusState, WorkloadKind
from turbobus.topology import DaemonResourceInventory, FabricLinkRecord, GpuInventoryRecord, PciePathRecord
from test.python.fixtures.topology import StaticTopologyProvider
from test.python.integration.test_client_worker_transfer import (
    CompleteExecutor,
    FakeCudaBackend,
    FakeDirectBackend,
    daemon_with_relay_path,
)
from turbobus.worker import WorkerTransferClient


class PaperMainPathTest(unittest.TestCase):
    def test_direct_path_runs_without_relay_lease_and_records_receipt(self) -> None:
        daemon = daemon_with_relay_path()
        backend = FakeDirectBackend()
        transfer_client = make_worker_managed_transfer_client(
            daemon,
            target_gpu=0,
            relay_gpus=[1],
            backend=backend,
            max_inflight_chunks=8,
        )
        allocator = SharedPinnedCpuBufferAllocator(name_prefix="tb-paper-main")

        with allocator.allocate("cpu-buffer", "job-1", 64) as source:
            target = _target_buffer("gpu-buffer", "job-1", 64)
            result = transfer_client.fetch_shared_cpu_to_cuda_ipc(
                source,
                target,
                ranges=({"src_offset": 0, "dst_offset": 0, "bytes": 64},),
                chunk_bytes=16,
                mode="direct",
                job_id="job-1",
            )

        self.assertEqual(result.state, "complete")
        self.assertIsNone(result.lease_token)
        self.assertEqual(result.lease_tokens, ())
        self.assertIsNone(result.authorization_request)
        self.assertEqual(result.plan["stats"]["resolved_mode"], "direct")
        self.assertEqual(result.plan["stats"]["direct_bytes"], 64)
        self.assertEqual(result.plan["stats"]["relay_bytes"], 0)
        self.assertEqual(_plan_path_kinds(result.plan["plan"]), {"direct"})
        self.assertEqual(len(backend.fetches), 1)
        self.assertEqual(result.final_status["bytes_completed"], 64)

    def test_relay_path_issues_ticket_and_worker_receipt(self) -> None:
        daemon = daemon_with_relay_path()
        executor = CompleteExecutor()
        transfer_client = make_worker_managed_transfer_client(
            daemon,
            target_gpu=0,
            relay_gpus=[1],
            worker_client=WorkerTransferClient(daemon, executor=executor),
            max_inflight_chunks=8,
        )
        allocator = SharedPinnedCpuBufferAllocator(name_prefix="tb-paper-main")

        with allocator.allocate("cpu-buffer", "job-1", 64) as source:
            target = _target_buffer("gpu-buffer", "job-1", 64)
            result = transfer_client.fetch_shared_cpu_to_cuda_ipc(
                source,
                target,
                ranges=({"src_offset": 0, "dst_offset": 0, "bytes": 64},),
                chunk_bytes=16,
                mode="relay",
                job_id="job-1",
            )

        self.assertEqual(result.state, "complete")
        self.assertIsNotNone(result.lease_token)
        self.assertIsNotNone(result.authorization_request)
        self.assertIsNotNone(result.worker_completion)
        self.assertEqual(result.plan["stats"]["resolved_mode"], "relay")
        self.assertEqual(result.plan["stats"]["direct_bytes"], 0)
        self.assertEqual(result.plan["stats"]["relay_bytes"], 64)
        self.assertEqual(_plan_path_kinds(result.plan["plan"]), {"relay"})
        self.assertEqual(executor.requests[0].data_plane.plan, result.plan["plan"])
        self.assertEqual(result.worker_completion.final_state, "complete")
        self.assertEqual(result.final_status["bytes_completed"], 64)

    def test_pool_path_mixes_direct_and_relay_chunks(self) -> None:
        daemon = daemon_with_relay_path()
        executor = CompleteExecutor()
        transfer_client = make_worker_managed_transfer_client(
            daemon,
            target_gpu=0,
            relay_gpus=[1],
            worker_client=WorkerTransferClient(daemon, executor=executor),
            max_inflight_chunks=8,
        )
        allocator = SharedPinnedCpuBufferAllocator(name_prefix="tb-paper-main")

        with allocator.allocate("cpu-buffer", "job-1", 64) as source:
            target = _target_buffer("gpu-buffer", "job-1", 64)
            result = transfer_client.fetch_shared_cpu_to_cuda_ipc(
                source,
                target,
                ranges=({"src_offset": 0, "dst_offset": 0, "bytes": 64},),
                chunk_bytes=16,
                mode="pool",
                job_id="job-1",
            )

        self.assertEqual(result.plan["stats"]["resolved_mode"], "pool")
        self.assertEqual(_plan_path_kinds(result.plan["plan"]), {"direct", "relay"})
        self.assertEqual(_assigned_bytes(result.plan["plan"]), 64)
        self.assertEqual(result.plan["stats"]["direct_bytes"] + result.plan["stats"]["relay_bytes"], 64)
        self.assertEqual(result.final_status["bytes_completed"], 64)

    def test_relay_quota_fallback_runs_direct_without_worker_authorization(self) -> None:
        daemon = daemon_with_relay_path(max_inflight_chunks_per_relay=1)
        backend = FakeDirectBackend()
        transfer_client = make_worker_managed_transfer_client(
            daemon,
            target_gpu=0,
            relay_gpus=[1],
            backend=backend,
            max_inflight_chunks=8,
        )
        allocator = SharedPinnedCpuBufferAllocator(name_prefix="tb-paper-main")

        with allocator.allocate("cpu-buffer", "job-1", 64) as source:
            target = _target_buffer("gpu-buffer", "job-1", 64)
            result = transfer_client.fetch_shared_cpu_to_cuda_ipc(
                source,
                target,
                ranges=({"src_offset": 0, "dst_offset": 0, "bytes": 64},),
                chunk_bytes=16,
                mode="pool",
                job_id="job-1",
            )

        self.assertEqual(result.plan["stats"]["resolved_mode"], "direct")
        self.assertIn("quota", result.plan["stats"]["fallback_reason"])
        self.assertEqual(result.lease_tokens, ())
        self.assertIsNone(result.authorization_request)
        self.assertEqual(len(backend.fetches), 1)
        self.assertEqual(daemon.describe().payload["relay_quotas"][1]["active_chunks"], 0)

    def test_worker_authorization_rejects_unapproved_buffer_and_ranges(self) -> None:
        daemon = daemon_with_relay_path()
        session_id = _register_job_buffers(daemon, "job-1")
        planned = daemon.plan_transfer(
            session_id=session_id,
            total_bytes=64,
            chunk_bytes=16,
            mode="relay",
            direction="h2d",
            job_id="job-1",
            buffer_ids=["job-1-cpu", "job-1-gpu"],
            ranges=({"src_offset": 0, "dst_offset": 0, "bytes": 64},),
        )
        self.assertTrue(planned.ok, planned.error)
        token = planned.payload["lease_tokens"][0]

        wrong_buffer = daemon.authorize_worker_transfer(
            WorkerTransferAuthorizationRequest(
                transfer_id=planned.payload["transfer_id"],
                lease_id=token["lease_id"],
                token=token["token"],
                session_id=session_id,
                job_id="job-1",
                src_buffer_id="other-cpu",
                dst_buffer_id="job-1-gpu",
                direction="h2d",
                relay_gpu=1,
            )
        )
        self.assertFalse(wrong_buffer.ok)
        self.assertIn("buffer", wrong_buffer.error)

        wrong_ranges = daemon.authorize_worker_transfer(
            WorkerTransferAuthorizationRequest(
                transfer_id=planned.payload["transfer_id"],
                lease_id=token["lease_id"],
                token=token["token"],
                session_id=session_id,
                job_id="job-1",
                src_buffer_id="job-1-cpu",
                dst_buffer_id="job-1-gpu",
                direction="h2d",
                ranges=({"src_offset": 0, "dst_offset": 0, "bytes": 16},),
                relay_gpu=1,
            )
        )
        self.assertFalse(wrong_ranges.ok)
        self.assertIn("ranges", wrong_ranges.error)

    def test_multiple_jobs_do_not_exceed_relay_quota(self) -> None:
        daemon = _daemon_with_profile([1], max_inflight_chunks_per_relay=2)
        first_session = _register_job_buffers(daemon, "job-1")
        second_session = _register_job_buffers(daemon, "job-2")

        first = daemon.plan_transfer(
            session_id=first_session,
            total_bytes=32,
            chunk_bytes=16,
            mode="relay",
            direction="h2d",
            job_id="job-1",
            buffer_ids=["job-1-cpu", "job-1-gpu"],
            ranges=({"src_offset": 0, "dst_offset": 0, "bytes": 32},),
        )
        self.assertTrue(first.ok, first.error)
        self.assertEqual(first.payload["stats"]["relay_chunks"], 2)
        self.assertEqual(daemon.describe().payload["relay_quotas"][1]["active_chunks"], 2)

        second = daemon.plan_transfer(
            session_id=second_session,
            total_bytes=16,
            chunk_bytes=16,
            mode="relay",
            direction="h2d",
            job_id="job-2",
            buffer_ids=["job-2-cpu", "job-2-gpu"],
            ranges=({"src_offset": 0, "dst_offset": 0, "bytes": 16},),
        )
        self.assertTrue(second.ok, second.error)
        self.assertEqual(second.payload["stats"]["resolved_mode"], "direct")
        self.assertIn("relay", second.payload["stats"]["fallback_reason"])
        self.assertEqual(second.payload["lease_tokens"], [])
        self.assertEqual(daemon.describe().payload["relay_quotas"][1]["active_chunks"], 2)

    def test_transfer_intent_links_decision_ticket_and_receipt(self) -> None:
        daemon = _daemon_with_profile([1])
        session_id = _register_job_buffers(daemon, "job-1")
        intent = TransferIntent(
            intent_id="intent-1",
            job_id="job-1",
            session_id=session_id,
            source_buffer_id="job-1-cpu",
            destination_buffer_id="job-1-gpu",
            direction="h2d",
            total_bytes=64,
            ranges=({"src_offset": 0, "dst_offset": 0, "bytes": 64},),
            workload_kind=WorkloadKind.KV_CACHE,
            policy_hints={"chunk_bytes": 16},
        )

        submitted = daemon.submit_transfer_intent(intent)

        self.assertTrue(submitted.ok, submitted.error)
        receipt = TransferReceipt(**submitted.payload["receipt"])
        decision = submitted.payload["decision"]
        ticket = submitted.payload["ticket"]
        self.assertEqual(receipt.intent_id, intent.intent_id)
        self.assertEqual(receipt.decision_id, decision["decision_id"])
        self.assertEqual(receipt.ticket_id, ticket["ticket_id"])
        self.assertEqual(ticket["intent_id"], intent.intent_id)
        self.assertEqual(ticket["source_buffer_id"], "job-1-cpu")
        self.assertEqual(ticket["destination_buffer_id"], "job-1-gpu")
        self.assertEqual(receipt.state, TransferStatusState.SUBMITTED)
        self.assertEqual(receipt.bytes_total, 64)
        self.assertEqual(_assigned_bytes(decision["plan"]), 64)

        intent_only_completion = daemon.transfer_status(
            submitted.payload["transfer_id"],
            state="complete",
            bytes_completed=64,
        )
        self.assertFalse(intent_only_completion.ok)
        self.assertIn("execution evidence", intent_only_completion.error)

        completed = daemon.transfer_status(
            submitted.payload["transfer_id"],
            state="complete",
            bytes_completed=64,
            completion_source="worker",
        )
        self.assertTrue(completed.ok, completed.error)
        waited = daemon.wait_transfer_receipt(intent.intent_id)
        self.assertTrue(waited.ok, waited.error)
        final_receipt = TransferReceipt(**waited.payload["receipt"])
        self.assertEqual(final_receipt.state, TransferStatusState.COMPLETE)
        self.assertEqual(final_receipt.bytes_completed, 64)
        self.assertEqual(final_receipt.metadata["completion_source"], "worker")
        self.assertTrue(final_receipt.metadata["executed"])

    def test_public_client_executes_h2d_intent_through_worker(self) -> None:
        daemon = _daemon_with_profile([1])
        session_id = _register_session_and_job(daemon, "job-1")
        executor = CompleteExecutor()
        allocator = SharedPinnedCpuBufferAllocator(name_prefix="tb-paper-main")

        with allocator.allocate("job-1-cpu", "job-1", 64) as source:
            target = _target_buffer("job-1-gpu", "job-1", 64)
            _register_buffer_objects(daemon, source, target)
            receipt = TurboBusClient(
                daemon=daemon,
                transfer_executor=WorkerIntentTransferExecutor(
                    buffers={
                        source.buffer_id: source,
                        target.buffer_id: target,
                    },
                    worker_client=WorkerTransferClient(daemon, executor=executor),
                ),
            ).submit_transfer_intent(
                _intent(
                    intent_id="intent-public-h2d",
                    session_id=session_id,
                    direction="h2d",
                    source_buffer_id=source.buffer_id,
                    destination_buffer_id=target.buffer_id,
                )
            )

        self.assertEqual(receipt.state, TransferStatusState.COMPLETE)
        self.assertEqual(receipt.bytes_completed, 64)
        self.assertEqual(receipt.metadata["completion_source"], "worker")
        self.assertTrue(receipt.metadata["executed"])
        self.assertEqual(len(executor.requests), 1)
        self.assertEqual(executor.requests[0].authorization.direction, "h2d")
        profile = daemon.describe().payload
        self.assertEqual(profile["transfer_statuses"][receipt.metadata["transfer_id"]]["state"], "complete")

    def test_public_client_executes_d2h_intent_through_worker(self) -> None:
        daemon = _daemon_with_profile([1])
        session_id = _register_session_and_job(daemon, "job-1")
        executor = CompleteExecutor()
        allocator = SharedPinnedCpuBufferAllocator(name_prefix="tb-paper-main")

        with allocator.allocate("job-1-cpu", "job-1", 64) as target:
            source = _target_buffer("job-1-gpu", "job-1", 64)
            _register_buffer_objects(daemon, source, target)
            receipt = TurboBusClient(
                daemon=daemon,
                transfer_executor=WorkerIntentTransferExecutor(
                    buffers={
                        source.buffer_id: source,
                        target.buffer_id: target,
                    },
                    worker_client=WorkerTransferClient(daemon, executor=executor),
                ),
            ).submit_transfer_intent(
                _intent(
                    intent_id="intent-public-d2h",
                    session_id=session_id,
                    direction="d2h",
                    source_buffer_id=source.buffer_id,
                    destination_buffer_id=target.buffer_id,
                )
            )

        self.assertEqual(receipt.state, TransferStatusState.COMPLETE)
        self.assertEqual(receipt.bytes_completed, 64)
        self.assertEqual(receipt.metadata["completion_source"], "worker")
        self.assertTrue(receipt.metadata["executed"])
        self.assertEqual(len(executor.requests), 1)
        self.assertEqual(executor.requests[0].authorization.direction, "d2h")


def _target_buffer(buffer_id: str, job_id: str, size_bytes: int) -> CudaIpcDeviceBuffer:
    return CudaIpcDeviceBuffer.from_device_pointer(
        buffer_id=buffer_id,
        job_id=job_id,
        device_index=0,
        size_bytes=size_bytes,
        device_ptr=4096,
        backend=FakeCudaBackend(),
    )


def _intent(
    *,
    intent_id: str,
    session_id: str,
    direction: str,
    source_buffer_id: str,
    destination_buffer_id: str,
) -> TransferIntent:
    return TransferIntent(
        intent_id=intent_id,
        job_id="job-1",
        session_id=session_id,
        source_buffer_id=source_buffer_id,
        destination_buffer_id=destination_buffer_id,
        direction=direction,
        total_bytes=64,
        ranges=({"src_offset": 0, "dst_offset": 0, "bytes": 64},),
        workload_kind=WorkloadKind.KV_CACHE,
        policy_hints={"chunk_bytes": 16},
    )


def _register_session_and_job(daemon: TurboBusDaemon, job_id: str) -> str:
    session = daemon.register_session(
        target_gpu=0,
        requested_relays=[1],
        max_inflight_chunks=8,
    )
    assert session.ok, session.error
    session_id = session.payload["session"]["session_id"]
    assert daemon.register_job(job_id=job_id, session_id=session_id).ok
    return session_id


def _register_buffer_objects(
    daemon: TurboBusDaemon,
    *buffers,
) -> None:
    for buffer in buffers:
        registration = buffer.buffer_registration()
        response = daemon.register_buffer(
            buffer_id=registration.buffer_id,
            job_id=registration.job_id,
            kind=registration.kind,
            size_bytes=registration.size_bytes,
            device_index=registration.device_index,
            address=registration.address,
            pinned=registration.pinned,
            handle_type=registration.handle_type,
            metadata=registration.metadata,
        )
        assert response.ok, response.error


def _register_job_buffers(daemon: TurboBusDaemon, job_id: str) -> str:
    session = daemon.register_session(
        target_gpu=0,
        requested_relays=[1],
        max_inflight_chunks=8,
    )
    assert session.ok, session.error
    session_id = session.payload["session"]["session_id"]
    assert daemon.register_job(job_id=job_id, session_id=session_id).ok
    assert daemon.register_buffer(
        buffer_id=f"{job_id}-cpu",
        job_id=job_id,
        kind="cpu_pinned",
        size_bytes=64,
        pinned=True,
        handle_type="shared_pinned_cpu",
        metadata={
            "shared_memory_name": f"tb-{job_id}-src",
            "offset_bytes": 0,
            "shared_memory_size_bytes": 64,
        },
    ).ok
    assert daemon.register_buffer(
        buffer_id=f"{job_id}-gpu",
        job_id=job_id,
        kind="gpu",
        size_bytes=64,
        device_index=0,
        handle_type="cuda_ipc_device",
        metadata={"cuda_ipc_handle": (b"t" * 64).hex()},
    ).ok
    return session_id


def _daemon_with_profile(
    relay_gpus: list[int],
    *,
    max_inflight_chunks_per_relay: int = 8,
) -> TurboBusDaemon:
    daemon = TurboBusDaemon(
        relay_gpus=relay_gpus,
        max_sessions_per_relay=4,
        max_inflight_chunks_per_relay=max_inflight_chunks_per_relay,
        topology_provider=StaticTopologyProvider(
            DaemonResourceInventory(
                gpus=tuple(
                    [GpuInventoryRecord(device_id=0, role="target")]
                    + [GpuInventoryRecord(device_id=gpu, role="relay") for gpu in relay_gpus]
                ),
                pcie_paths=tuple(PciePathRecord(device_id=gpu) for gpu in relay_gpus),
                fabric_links=tuple(
                    FabricLinkRecord(
                        src_device_id=gpu,
                        dst_device_id=0,
                        fabric="nvlink",
                        enabled=True,
                    )
                    for gpu in relay_gpus
                ),
                source="test",
            )
        ),
    )
    daemon.put_profile(
        target_gpu=0,
        relay_gpus=relay_gpus,
        profile={
            "target_device": 0,
            "direct_h2d_bw_gbps": 1.0,
            "direct_d2h_bw_gbps": 1.0,
            "relays": [
                {
                    "relay_device": gpu,
                    "target_device": 0,
                    "h2d_bw_gbps": 8.0,
                    "d2h_bw_gbps": 7.0,
                    "p2p_bw_gbps": 40.0,
                    "effective_bw_gbps": 8.0,
                    "effective_d2h_bw_gbps": 7.0,
                    "p2p_enabled": True,
                }
                for gpu in relay_gpus
            ],
        },
    )
    return daemon


def _plan_path_kinds(plan: dict) -> set[str]:
    return {
        str(assignment["path"]["kind"])
        for assignment in plan.get("assignments", ()) or ()
    }


def _assigned_bytes(plan: dict) -> int:
    return sum(
        int(chunk["bytes"])
        for assignment in plan.get("assignments", ()) or ()
        for chunk in assignment.get("chunks", ()) or ()
    )


if __name__ == "__main__":
    unittest.main()
