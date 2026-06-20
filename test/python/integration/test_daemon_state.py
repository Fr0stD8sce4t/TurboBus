from __future__ import annotations

from dataclasses import asdict
import time
import unittest

from turbobus.daemon.server import TurboBusDaemon
from turbobus.schema import (
    DaemonRequest,
    PeerIdentity,
    RequestType,
    TransferIntent,
    TransferStatusState,
    WorkerTransferAuthorizationRequest,
    WorkloadKind,
)
from turbobus.topology import (
    DaemonResourceInventory,
    FabricLinkRecord,
    GpuInventoryRecord,
    PciePathRecord,
)
from test.python.fixtures.topology import StaticTopologyProvider


CUDA_IPC_TARGET_HANDLE = (b"t" * 64).hex()
CUDA_IPC_SOURCE_HANDLE = (b"s" * 64).hex()
TRANSFER_BYTES = 16 * 1024 * 1024
CHUNK_BYTES = 4 * 1024 * 1024


def _inventory(*, fabric_enabled: bool = True) -> DaemonResourceInventory:
    return DaemonResourceInventory(
        gpus=(
            GpuInventoryRecord(device_id=0, role="target"),
            GpuInventoryRecord(device_id=1, role="relay"),
            GpuInventoryRecord(device_id=2, role="relay"),
        ),
        pcie_paths=(
            PciePathRecord(device_id=0, bandwidth_gbps=7.5),
            PciePathRecord(device_id=1, bandwidth_gbps=8.0),
            PciePathRecord(device_id=2, bandwidth_gbps=8.0),
        ),
        fabric_links=(
            FabricLinkRecord(
                src_device_id=1,
                dst_device_id=0,
                fabric="nvlink",
                bandwidth_gbps=40.0,
                enabled=bool(fabric_enabled),
            ),
            FabricLinkRecord(
                src_device_id=2,
                dst_device_id=0,
                fabric="nvlink",
                bandwidth_gbps=40.0,
                enabled=bool(fabric_enabled),
            ),
        ),
        source="daemon-state-test",
        discovered_at=time.time(),
    )


def _daemon(*, fabric_enabled: bool = True, **kwargs) -> TurboBusDaemon:
    kwargs.setdefault("relay_gpus", [1, 2])
    kwargs.setdefault("topology_provider", StaticTopologyProvider(_inventory(
        fabric_enabled=fabric_enabled,
    )))
    return TurboBusDaemon(**kwargs)


def _relay_profile(*, relay_gpus: tuple[int, ...] = (1, 2)) -> dict:
    return {
        "target_device": 0,
        "direct_h2d_bw_gbps": 1.0,
        "direct_d2h_bw_gbps": 1.0,
        "relays": [
            {
                "relay_device": relay_gpu,
                "target_device": 0,
                "h2d_bw_gbps": 8.0,
                "d2h_bw_gbps": 7.0,
                "p2p_bw_gbps": 40.0,
                "effective_bw_gbps": 8.0,
                "effective_d2h_bw_gbps": 7.0,
                "p2p_enabled": True,
            }
            for relay_gpu in relay_gpus
        ],
    }


def _cuda_metadata(handle: str = CUDA_IPC_TARGET_HANDLE) -> dict:
    return {
        "cuda_ipc_handle": handle,
        "device_offset_bytes": 0,
        "allocation_base_ptr": 4096,
        "allocation_size_bytes": TRANSFER_BYTES,
    }


def _register_runtime_buffers(
    daemon: TurboBusDaemon,
    *,
    job_id: str = "job-1",
    session_id: str | None = None,
    cpu_buffer_id: str = "cpu-buffer",
    gpu_buffer_id: str = "gpu-buffer",
) -> tuple[str, str, str]:
    if session_id is None:
        registered = daemon.register_session(
            target_gpu=0,
            max_inflight_chunks=8,
            worker_relay_capable=True,
        )
        assert registered.ok, registered.error
        session_id = registered.payload["session"]["session_id"]
    assert daemon.register_job(job_id=job_id, session_id=session_id).ok
    assert daemon.register_buffer(
        buffer_id=cpu_buffer_id,
        job_id=job_id,
        kind="cpu_pinned",
        size_bytes=TRANSFER_BYTES,
        pinned=True,
        handle_type="shared_pinned_cpu",
        metadata={
            "shared_memory_name": f"tb-{job_id}-cpu",
            "offset_bytes": 0,
            "shared_memory_size_bytes": TRANSFER_BYTES,
        },
    ).ok
    assert daemon.register_buffer(
        buffer_id=gpu_buffer_id,
        job_id=job_id,
        kind="gpu",
        size_bytes=TRANSFER_BYTES,
        device_index=0,
        handle_type="cuda_ipc_device",
        metadata=_cuda_metadata(),
    ).ok
    return session_id, cpu_buffer_id, gpu_buffer_id


def _intent(
    *,
    session_id: str,
    job_id: str = "job-1",
    intent_id: str = "intent-1",
    source_buffer_id: str = "cpu-buffer",
    destination_buffer_id: str = "gpu-buffer",
    direction: str = "h2d",
    total_bytes: int = TRANSFER_BYTES,
    chunk_bytes: int = CHUNK_BYTES,
    transfer_mode: str = "pool",
) -> TransferIntent:
    return TransferIntent(
        intent_id=intent_id,
        job_id=job_id,
        session_id=session_id,
        source_buffer_id=source_buffer_id,
        destination_buffer_id=destination_buffer_id,
        direction=direction,
        total_bytes=total_bytes,
        ranges=({"src_offset": 0, "dst_offset": 0, "bytes": total_bytes},),
        workload_kind=WorkloadKind.KV_CACHE,
        policy_hints={
            "chunk_bytes": int(chunk_bytes),
            "transfer_mode": str(transfer_mode),
        },
        metadata={"test": "daemon-state"},
    )


class DaemonStateTest(unittest.TestCase):
    def test_session_lifecycle_releases_relay_quota(self) -> None:
        daemon = _daemon(relay_gpus=[1], max_sessions_per_relay=1)

        first = daemon.register_session(target_gpu=0, worker_relay_capable=True)
        self.assertTrue(first.ok)
        second = daemon.register_session(target_gpu=0, worker_relay_capable=True)
        self.assertFalse(second.ok)
        self.assertIn("unavailable", second.error)

        closed = daemon.close_session(first.payload["session"]["session_id"])
        third = daemon.register_session(target_gpu=0, worker_relay_capable=True)

        self.assertTrue(closed.ok)
        self.assertTrue(third.ok)

    def test_submit_transfer_intent_plans_pooled_paths_and_issues_worker_ticket(self) -> None:
        daemon = _daemon(max_inflight_chunks_per_relay=8)
        session_id, cpu_buffer_id, gpu_buffer_id = _register_runtime_buffers(daemon)
        self.assertTrue(
            daemon.put_profile(
                target_gpu=0,
                relay_gpus=[1, 2],
                profile=_relay_profile(),
            ).ok
        )

        submitted = daemon.submit_transfer_intent(
            _intent(
                session_id=session_id,
                source_buffer_id=cpu_buffer_id,
                destination_buffer_id=gpu_buffer_id,
            )
        )

        self.assertTrue(submitted.ok, submitted.error)
        payload = submitted.payload
        self.assertEqual(payload["receipt"]["intent_id"], "intent-1")
        self.assertEqual(payload["receipt"]["state"], "submitted")
        self.assertIsNotNone(payload["ticket"])
        self.assertGreaterEqual(len(payload["lease_tokens"]), 1)
        assignments = payload["ticket"]["plan"]["assignments"]
        self.assertTrue(any(item["path"]["kind"] == "relay" for item in assignments))
        self.assertEqual(payload["decision"]["job_id"], "job-1")

    def test_worker_authorization_uses_daemon_issued_lease_and_registered_buffers(self) -> None:
        daemon = _daemon(max_inflight_chunks_per_relay=8)
        session_id, cpu_buffer_id, gpu_buffer_id = _register_runtime_buffers(daemon)
        daemon.put_profile(target_gpu=0, relay_gpus=[1, 2], profile=_relay_profile())
        submitted = daemon.submit_transfer_intent(
            _intent(
                session_id=session_id,
                source_buffer_id=cpu_buffer_id,
                destination_buffer_id=gpu_buffer_id,
            )
        )
        self.assertTrue(submitted.ok, submitted.error)
        lease = submitted.payload["lease_tokens"][0]
        transfer_id = submitted.payload["transfer_id"]

        validated = daemon.validate_lease(
            lease_id=lease["lease_id"],
            token=lease["token"],
            session_id=session_id,
            relay_gpu=lease["relay_gpu"],
            job_id="job-1",
            buffer_ids=[cpu_buffer_id, gpu_buffer_id],
        )
        authorized = daemon.authorize_worker_transfer(
            WorkerTransferAuthorizationRequest(
                transfer_id=transfer_id,
                lease_id=lease["lease_id"],
                token=lease["token"],
                session_id=session_id,
                job_id="job-1",
                src_buffer_id=cpu_buffer_id,
                dst_buffer_id=gpu_buffer_id,
                direction="h2d",
                relay_gpu=lease["relay_gpu"],
            )
        )

        self.assertTrue(validated.ok, validated.error)
        self.assertTrue(authorized.ok, authorized.error)
        self.assertEqual(
            authorized.payload["src_buffer"]["buffer_id"],
            cpu_buffer_id,
        )
        self.assertEqual(
            authorized.payload["dst_buffer"]["buffer_id"],
            gpu_buffer_id,
        )

    def test_handle_request_submit_intent_enforces_authenticated_job_ownership(self) -> None:
        daemon = _daemon(require_authenticated_peers=True)
        owner = PeerIdentity(
            authenticated=True,
            source="test",
            user_id="1000",
            process_id=42,
        )
        other = PeerIdentity(
            authenticated=True,
            source="test",
            user_id="2000",
            process_id=84,
        )
        registered = daemon.handle_request(
            DaemonRequest(
                request_type=RequestType.REGISTER_SESSION,
                payload={
                    "target_gpu": 0,
                    "max_inflight_chunks": 8,
                    "worker_relay_capable": True,
                },
                peer_identity=owner,
            )
        )
        self.assertTrue(registered.ok, registered.error)
        session_id = registered.payload["session"]["session_id"]
        self.assertTrue(
            daemon.handle_request(
                DaemonRequest(
                    request_type=RequestType.REGISTER_JOB,
                    payload={"job_id": "job-1", "session_id": session_id},
                    peer_identity=owner,
                )
            ).ok
        )
        self.assertTrue(
            daemon.handle_request(
                DaemonRequest(
                    request_type=RequestType.REGISTER_BUFFER,
                    payload={
                        "buffer_id": "cpu-buffer",
                        "job_id": "job-1",
                        "kind": "cpu_pinned",
            "size_bytes": TRANSFER_BYTES,
                        "pinned": True,
                        "handle_type": "shared_pinned_cpu",
                        "metadata": {
                            "shared_memory_name": "tb-job-1-cpu",
                            "offset_bytes": 0,
                            "shared_memory_size_bytes": TRANSFER_BYTES,
                        },
                    },
                    peer_identity=owner,
                )
            ).ok
        )
        self.assertTrue(
            daemon.handle_request(
                DaemonRequest(
                    request_type=RequestType.REGISTER_BUFFER,
                    payload={
                        "buffer_id": "gpu-buffer",
                        "job_id": "job-1",
                        "kind": "gpu",
                        "size_bytes": TRANSFER_BYTES,
                        "device_index": 0,
                        "handle_type": "cuda_ipc_device",
                        "metadata": _cuda_metadata(),
                    },
                    peer_identity=owner,
                )
            ).ok
        )
        daemon.put_profile(target_gpu=0, relay_gpus=[1, 2], profile=_relay_profile())

        denied = daemon.handle_request(
            DaemonRequest(
                request_type=RequestType.SUBMIT_TRANSFER_INTENT,
                session_id=session_id,
                payload={"intent": asdict(_intent(session_id=session_id))},
                peer_identity=other,
            )
        )
        allowed = daemon.handle_request(
            DaemonRequest(
                request_type=RequestType.SUBMIT_TRANSFER_INTENT,
                session_id=session_id,
                payload={"intent": asdict(_intent(
                    session_id=session_id,
                    intent_id="owner-intent",
                ))},
                peer_identity=owner,
            )
        )

        self.assertFalse(denied.ok)
        self.assertIn("buffer owner", denied.error)
        self.assertTrue(allowed.ok, allowed.error)

    def test_transfer_status_completion_updates_wait_receipt_and_cleanup(self) -> None:
        daemon = _daemon(max_inflight_chunks_per_relay=8)
        session_id, cpu_buffer_id, gpu_buffer_id = _register_runtime_buffers(daemon)
        daemon.put_profile(target_gpu=0, relay_gpus=[1, 2], profile=_relay_profile())
        submitted = daemon.submit_transfer_intent(_intent(session_id=session_id))
        self.assertTrue(submitted.ok, submitted.error)
        transfer_id = submitted.payload["transfer_id"]
        ticket = submitted.payload["ticket"]

        completed = daemon.transfer_status(
            transfer_id,
            state=TransferStatusState.COMPLETE.value,
            bytes_completed=TRANSFER_BYTES,
            completion_source="worker",
            completion_evidence={
                "source": "integration-test",
                "ticket_id": ticket["ticket_id"],
                "transfer_id": transfer_id,
                "plan_generation": ticket["metadata"]["plan_generation"],
                "owner_binding": ticket["metadata"]["owner_binding"],
                "executed": True,
                "verified": True,
                "verified_bytes": TRANSFER_BYTES,
                "content_match": True,
            },
        )
        waited = daemon.wait_transfer_receipt("intent-1", timeout_seconds=0)
        cleanup = daemon.cleanup(
            target_kind="job",
            target_id="job-1",
            reason="job_exit",
            force=True,
        )

        self.assertTrue(completed.ok, completed.error)
        self.assertTrue(waited.ok, waited.error)
        self.assertEqual(waited.payload["receipt"]["state"], "complete")
        self.assertEqual(waited.payload["receipt"]["bytes_completed"], TRANSFER_BYTES)
        self.assertTrue(cleanup.ok, cleanup.error)
        self.assertEqual(cleanup.payload["removed"]["jobs"], 1)
        self.assertEqual(cleanup.payload["removed"]["buffers"], 2)
        self.assertNotIn(cpu_buffer_id, daemon.describe().payload["buffers"])
        self.assertNotIn(gpu_buffer_id, daemon.describe().payload["buffers"])

    def test_submit_transfer_intent_falls_back_direct_without_fabric_path(self) -> None:
        daemon = _daemon(fabric_enabled=False, max_inflight_chunks_per_relay=8)
        session_id, _, _ = _register_runtime_buffers(daemon)
        daemon.put_profile(target_gpu=0, relay_gpus=[1, 2], profile=_relay_profile())

        submitted = daemon.submit_transfer_intent(
            _intent(session_id=session_id, transfer_mode="pool")
        )

        self.assertTrue(submitted.ok, submitted.error)
        self.assertEqual(submitted.payload["decision"]["state"], "fallback")
        self.assertEqual(submitted.payload["reservations"], [])
        assignments = submitted.payload["ticket"]["plan"]["assignments"]
        self.assertTrue(all(item["path"]["kind"] == "direct" for item in assignments))

    def test_submit_transfer_intent_rejects_unknown_session_through_request_router(self) -> None:
        daemon = _daemon()

        response = daemon.handle_request(
            DaemonRequest(
                request_type=RequestType.SUBMIT_TRANSFER_INTENT,
                session_id="missing",
                payload={"intent": asdict(_intent(session_id="missing"))},
            )
        )

        self.assertFalse(response.ok)
        self.assertIn("unknown session", response.error)


if __name__ == "__main__":
    unittest.main()
