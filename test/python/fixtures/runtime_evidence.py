from __future__ import annotations

from dataclasses import asdict
from typing import Mapping

from turbobus.offload.context import TransferContext
from turbobus.runtime_session import TurboBusRuntimeSession
from turbobus.schema import TransferIntent, TransferReceipt, TransferStatusState, WorkloadKind


class FakeRuntimeSession(TurboBusRuntimeSession):
    def __init__(
        self,
        *,
        job_id: str = "job-1",
        session_id: str = "session-1",
        cpu_buffer_id: str = "cpu-buffer",
        gpu_buffer_id: str = "gpu-buffer",
    ) -> None:
        self.job_id = str(job_id)
        self.user_id = None
        self._session_id = str(session_id)
        self._closed = False
        self.cpu_buffer_id = str(cpu_buffer_id)
        self.gpu_buffer_id = str(gpu_buffer_id)
        self.submitted: list[TransferIntent] = []
        self.waited: list[tuple[str, float | None]] = []
        self.intents: dict[str, dict[str, object]] = {}
        self.receipts: dict[str, dict[str, object]] = {}
        self.transfer_contexts: dict[str, dict[str, object]] = {}
        self.transfer_evidence: dict[str, dict[str, object]] = {}

    def open_session(self) -> str:
        return self._session_id

    def close(self) -> None:
        self._closed = True

    def _register_pending_buffers(self) -> None:
        return None

    def make_transfer_context(
        self,
        cpu_buffer,
        gpu_buffer,
        *,
        workload_kind: WorkloadKind | str = WorkloadKind.GENERIC,
        priority: int = 0,
        policy_hints: Mapping[str, object] | None = None,
        metadata: Mapping[str, object] | None = None,
        intent_prefix: str | None = None,
        wait_timeout_seconds: float | None = None,
    ) -> TransferContext:
        context = TransferContext(
            job_id=self.job_id,
            session_id=self.session_id,
            cpu_buffer_id=self.cpu_buffer_id,
            gpu_buffer_id=self.gpu_buffer_id,
            cpu_buffer=cpu_buffer,
            gpu_buffer=gpu_buffer,
            workload_kind=workload_kind,
            priority=priority,
            policy_hints={} if policy_hints is None else policy_hints,
            metadata={} if metadata is None else metadata,
            intent_prefix=intent_prefix,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        self.record_transfer_context(
            context_id=f"context-{context.intent_prefix}",
            workload_kind=context.workload_kind.value,
            cpu_buffer_id=context.cpu_buffer_id,
            gpu_buffer_id=context.gpu_buffer_id,
            intent_prefix=context.intent_prefix,
            priority=context.priority,
            policy_hints=context.policy_hints,
            metadata=context.metadata,
        )
        return context

    def make_state_offload(
        self,
        spec,
        cpu_buffer,
        gpu_buffer,
        *,
        workload_kind: WorkloadKind | str | None = None,
        priority: int = 0,
        policy_hints: Mapping[str, object] | None = None,
        metadata: Mapping[str, object] | None = None,
        intent_prefix: str | None = None,
        wait_timeout_seconds: float | None = None,
    ):
        # /*
        #  * ========================================================================
        #  * 步骤1：构造测试用 state offload core
        #  * ========================================================================
        #  * 数据源：FakeRuntimeSession 与 StateOffloadSpec
        #  * 操作：
        #  *   1) 使用 fake 自身 make_transfer_context
        #  *   2) 返回真实 StateOffloadCore 验证语义
        #  */
        logger = __import__("logging").getLogger(__name__)
        logger.info("开始构造测试用 state offload core...")

        # // 1.1 延迟导入避免测试 fixture 形成循环依赖
        from turbobus.state_offload import StateOffloadCore, workload_kind_for_spec

        # // 1.2 用 spec 解析 workload kind 并创建 context
        resolved_workload_kind = (
            workload_kind_for_spec(spec) if workload_kind is None else workload_kind
        )
        context = self.make_transfer_context(
            cpu_buffer,
            gpu_buffer,
            workload_kind=resolved_workload_kind,
            priority=priority,
            policy_hints=policy_hints,
            metadata=spec.validate_metadata(metadata),
            intent_prefix=intent_prefix,
            wait_timeout_seconds=wait_timeout_seconds,
        )

        # // 1.3 返回统一 StateOffloadCore
        core = StateOffloadCore(self, context, spec)
        logger.info("测试用 state offload core 构造完成, state_kind: %s", spec.state_kind)
        return core

    def submit_transfer_intent(
        self,
        intent: TransferIntent,
        *,
        wait: bool = False,
        timeout_seconds: float | None = None,
    ) -> TransferReceipt:
        self.submitted.append(intent)
        self._record_intent(intent)
        receipt = make_runtime_receipt(
            intent,
            receipt_id=f"submitted-{intent.intent_id}",
        )
        self._record_receipt(receipt)
        return receipt

    def wait_transfer_receipt(
        self,
        intent_id: str,
        timeout_seconds: float | None = None,
    ) -> TransferReceipt:
        self.waited.append((str(intent_id), timeout_seconds))
        intent = next(item for item in self.submitted if item.intent_id == intent_id)
        receipt = make_runtime_receipt(intent, receipt_id=f"receipt-{intent_id}")
        self._record_receipt(receipt)
        return receipt

    def runtime_entrypoint_snapshot(self) -> dict[str, object]:
        return {
            "schema": "turbobus.runtime_session_entrypoint.v1",
            "entrypoint": "TurboBusRuntimeSession",
            "job_id": self.job_id,
            "session": {"session_id": self.session_id},
            "plan_source": "daemon_scheduler",
            "route_policy_visible_to_application": False,
            "route_policy_visible_to_transfer": False,
            "intents": dict(self.intents),
            "receipts": dict(self.receipts),
            "transfer_contexts": dict(self.transfer_contexts),
            "transfer_evidence": dict(self.transfer_evidence),
            "state_contexts": dict(self.transfer_contexts),
            "state_evidence": dict(self.transfer_evidence),
            "managed_services": None,
            "buffer_lifecycle": {},
            "profile_bootstrap": {},
            "closed": self.closed,
        }

    def record_transfer_context(
        self,
        *,
        context_id: str,
        workload_kind: str,
        cpu_buffer_id: str,
        gpu_buffer_id: str,
        intent_prefix: str,
        priority: int,
        policy_hints: Mapping[str, object],
        metadata: Mapping[str, object],
        state: str = "created",
        error: str | None = None,
    ) -> None:
        self.transfer_contexts[str(context_id)] = {
            "context_id": str(context_id),
            "workload_kind": str(workload_kind),
            "cpu_buffer_id": str(cpu_buffer_id),
            "gpu_buffer_id": str(gpu_buffer_id),
            "intent_prefix": str(intent_prefix),
            "priority": int(priority),
            "policy_hints": dict(policy_hints),
            "metadata": dict(metadata),
            "state": str(state),
            "route_policy_visible_to_state": False,
            "route_policy_visible_to_transfer": False,
        }
        if error is not None:
            self.transfer_contexts[str(context_id)]["error"] = str(error)

    def record_state_transfer_context(self, **kwargs) -> None:
        self.record_transfer_context(**kwargs)

    def record_transfer_lifecycle_evidence(
        self,
        *,
        evidence_id: str,
        operation: str,
        intent_ids,
        receipt_ids,
    ) -> None:
        normalized_intents = [str(item) for item in intent_ids]
        normalized_receipts = [str(item) for item in receipt_ids]
        observed_receipts = {
            str(item.get("receipt_id"))
            for item in self.receipts.values()
            if isinstance(item, Mapping)
        }
        self.transfer_evidence[str(evidence_id)] = {
            "evidence_id": str(evidence_id),
            "operation": str(operation),
            "intent_ids": normalized_intents,
            "receipt_ids": normalized_receipts,
            "intents_recorded": all(item in self.intents for item in normalized_intents),
            "receipts_recorded": all(item in observed_receipts for item in normalized_receipts),
        }

    def record_state_lifecycle_evidence(self, **kwargs) -> None:
        self.record_transfer_lifecycle_evidence(**kwargs)

    def recover_transfer_state(
        self,
        *,
        intent_id: str | None = None,
        transfer_id: str | None = None,
    ) -> dict[str, object]:
        return {
            "source": "fixture_runtime",
            "state": TransferStatusState.COMPLETE.value,
            "archived": False,
            "receipt": {"intent_id": intent_id, "transfer_id": transfer_id},
        }

    def _record_intent(self, intent: TransferIntent) -> None:
        self.intents[str(intent.intent_id)] = {
            "intent_id": str(intent.intent_id),
            "job_id": str(intent.job_id),
            "session_id": str(intent.session_id),
            "source_buffer_id": str(intent.source_buffer_id),
            "destination_buffer_id": str(intent.destination_buffer_id),
        }

    def _record_receipt(self, receipt: TransferReceipt) -> None:
        self.receipts[str(receipt.intent_id)] = {
            "intent_id": str(receipt.intent_id),
            "receipt_id": str(receipt.receipt_id),
            "state": receipt.state.value,
            "ticket_id": str(receipt.ticket_id),
            "decision_id": str(receipt.decision_id),
            "topology_snapshot_id": str(receipt.topology_snapshot_id),
            "bytes_total": int(receipt.bytes_total),
            "bytes_completed": int(receipt.bytes_completed),
        }


def make_runtime_intent(
    suffix: str,
    *,
    total_bytes: int,
    direction: str = "h2d",
    job_id: str = "job-1",
    session_id: str = "session-1",
    cpu_buffer_id: str = "cpu-buffer",
    gpu_buffer_id: str = "gpu-buffer",
    workload_kind: WorkloadKind | str = WorkloadKind.GENERIC,
) -> TransferIntent:
    source = cpu_buffer_id if direction == "h2d" else gpu_buffer_id
    destination = gpu_buffer_id if direction == "h2d" else cpu_buffer_id
    return TransferIntent(
        intent_id=f"intent-{suffix}",
        job_id=job_id,
        session_id=session_id,
        source_buffer_id=source,
        destination_buffer_id=destination,
        direction=direction,
        total_bytes=total_bytes,
        ranges=({"src_offset": 0, "dst_offset": 0, "bytes": total_bytes},),
        workload_kind=workload_kind,
    )


def make_runtime_receipt(
    intent: TransferIntent,
    *,
    receipt_id: str,
    metadata: dict[str, object] | None = None,
) -> TransferReceipt:
    direct_bytes = intent.total_bytes // 2
    relay_bytes = intent.total_bytes - direct_bytes
    return TransferReceipt(
        receipt_id=receipt_id,
        ticket_id=f"ticket-{intent.intent_id}",
        intent_id=intent.intent_id,
        decision_id=f"decision-{intent.intent_id}",
        topology_snapshot_id="topology-1",
        job_id=intent.job_id,
        session_id=intent.session_id,
        state=TransferStatusState.COMPLETE,
        bytes_total=intent.total_bytes,
        bytes_completed=intent.total_bytes,
        path_stats=(
            {"kind": "direct", "bytes": direct_bytes, "chunk_count": 1},
            {"kind": "relay", "bytes": relay_bytes, "chunk_count": 1},
        ),
        metadata=(
            verified_runtime_metadata(intent)
            if metadata is None
            else metadata
        ),
    )


def verified_runtime_metadata(intent: TransferIntent) -> dict[str, object]:
    ticket_id = f"ticket-{intent.intent_id}"
    transfer_id = f"transfer-{intent.intent_id}"
    plan_generation = 1
    direct_bytes = intent.total_bytes // 2
    relay_bytes = intent.total_bytes - direct_bytes
    mode = _execution_mode(direct_bytes=direct_bytes, relay_bytes=relay_bytes)
    completion_contract = {
        "ticket_id": ticket_id,
        "transfer_id": transfer_id,
        "plan_generation": plan_generation,
    }
    buffer_lifetime = _buffer_lifetime_evidence(intent)
    execution = {
        "completion_source": "worker",
        "executed": True,
        "mode": mode,
        "path": {
            "direct_bytes": direct_bytes,
            "relay_bytes": relay_bytes,
            "direct_chunks": 1,
            "relay_chunks": 1,
        },
        "verified": True,
        "verified_bytes": int(intent.total_bytes),
        "content_match": True,
    }
    transfer = {
        "transfer_id": transfer_id,
        "intent_id": intent.intent_id,
        "decision_id": f"decision-{intent.intent_id}",
        "topology_snapshot_id": "topology-1",
        "ticket_id": ticket_id,
        "job_id": intent.job_id,
        "session_id": intent.session_id,
        "state": TransferStatusState.COMPLETE.value,
        "bytes_total": int(intent.total_bytes),
        "bytes_completed": int(intent.total_bytes),
    }
    return {
        "payload": asdict(intent),
        "completion_source": "worker",
        "executed": True,
        "verified": True,
        "verified_bytes": int(intent.total_bytes),
        "content_match": True,
        "verification_source": "fixture_worker",
        "verification_method": "fixture_compare",
        "transfer_id": transfer_id,
        "evidence_transfer_id": transfer_id,
        "execution_ticket_id": ticket_id,
        "evidence_ticket_id": ticket_id,
        "plan_generation": plan_generation,
        "evidence_plan_generation": plan_generation,
        "completion_contract": completion_contract,
        "buffer_lifetime_evidence": buffer_lifetime,
        "worker_startup": {
            "startup_source": "fixture_worker",
            "topology_snapshot_id": "topology-1",
            "require_authenticated_peers": False,
        },
        "worker_async_pool": {
            "pool": "worker_async_execution_pool",
            "pool_ticket": f"pool-{intent.intent_id}",
            "state": "complete",
        },
        "reproduction_evidence": {
            "schema": "turbobus.reproduction_evidence.v1",
            "source": "TransferReceipt",
            "route_policy_source": "daemon_scheduler",
            "transfer": transfer,
            "execution": execution,
            "completion_contract": completion_contract,
            "buffer_lifetime": buffer_lifetime,
        },
    }


def unverified_runtime_metadata() -> dict[str, object]:
    return {
        "completion_source": "worker",
        "executed": True,
        "verified": False,
        "verified_bytes": 0,
        "content_match": False,
    }


def _buffer_lifetime_evidence(intent: TransferIntent) -> dict[str, object]:
    source_kind = (
        "shared_pinned_cpu"
        if intent.direction == "h2d"
        else "cuda_ipc_device"
    )
    destination_kind = (
        "cuda_ipc_device"
        if intent.direction == "h2d"
        else "shared_pinned_cpu"
    )
    return {
        "source_buffer": _buffer_lifetime_record(
            buffer_id=intent.source_buffer_id,
            session_id=intent.session_id,
            runtime_buffer_kind=source_kind,
        ),
        "destination_buffer": _buffer_lifetime_record(
            buffer_id=intent.destination_buffer_id,
            session_id=intent.session_id,
            runtime_buffer_kind=destination_kind,
        ),
    }


def _buffer_lifetime_record(
    *,
    buffer_id: str,
    session_id: str,
    runtime_buffer_kind: str,
) -> dict[str, object]:
    if runtime_buffer_kind == "shared_pinned_cpu":
        handle_type = "shared_pinned_cpu"
        resource_evidence = {
            "cpu_handle_type": "shared_pinned_cpu",
            "cpu_buffer_id": buffer_id,
            "cpu_buffer_opened": True,
        }
    else:
        handle_type = "cuda_ipc_device"
        resource_evidence = {
            "device_handle_type": "cuda_ipc_device",
            "device_buffer_id": buffer_id,
            "device_index": 0,
            "device_ptr": 1,
        }
    return {
        "buffer_id": buffer_id,
        "runtime_buffer_kind": runtime_buffer_kind,
        "runtime_session_id": session_id,
        "runtime_owned": False,
        "registration": {
            "buffer_id": buffer_id,
            "handle_type": handle_type,
            "metadata": {
                "runtime_session_id": session_id,
                "runtime_buffer_kind": runtime_buffer_kind,
            },
        },
        "resource_evidence": resource_evidence,
    }


def _execution_mode(*, direct_bytes: int, relay_bytes: int) -> str:
    if direct_bytes > 0 and relay_bytes > 0:
        return "mixed_pooled"
    if relay_bytes > 0:
        return "relay_only"
    return "direct_only"

