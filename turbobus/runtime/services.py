from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from ..client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer
from ..schema import DaemonResponse, TransferIntent, TransferReceipt, WorkloadKind

logger = logging.getLogger(__name__)


class RuntimeEvidenceRecorder:
    def __init__(self, session) -> None:
        self._session = session

    def record_transfer_lifecycle(
        self,
        *,
        evidence_id: str,
        operation: str,
        intent_ids: Sequence[str],
        receipt_ids: Sequence[str],
    ) -> None:
        # /*
        #  * ========================================================================
        #  * Step 1: Record transfer lifecycle evidence
        #  * ========================================================================
        #  * Target:
        #  *   1) Keep runtime entrypoint evidence under transfer_* naming.
        #  *   2) Delegate persistence to the existing RuntimeSession record core.
        #  */
        logger.info("Start recording transfer lifecycle evidence")

        # // 1.1 Validate the session is still open.
        self._session._require_open()

        # // 1.2 Write the canonical transfer evidence record.
        self._session._record_transfer_lifecycle_evidence_impl(
            evidence_id=evidence_id,
            operation=operation,
            intent_ids=tuple(str(intent_id) for intent_id in intent_ids),
            receipt_ids=tuple(str(receipt_id) for receipt_id in receipt_ids),
        )
        logger.info(
            "Finished recording transfer lifecycle evidence: evidence_id=%s",
            evidence_id,
        )

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
        # /*
        #  * ========================================================================
        #  * Step 2: Record transfer context evidence
        #  * ========================================================================
        #  * Target:
        #  *   1) Bind workload context to CPU/GPU runtime buffers.
        #  *   2) Keep workload layers from hand-building evidence dictionaries.
        #  */
        logger.info("Start recording transfer context evidence")

        # // 2.1 Validate the session is still open.
        self._session._require_open()

        # // 2.2 Write the canonical transfer context record.
        self._session._record_transfer_context_impl(
            context_id=context_id,
            workload_kind=workload_kind,
            cpu_buffer_id=cpu_buffer_id,
            gpu_buffer_id=gpu_buffer_id,
            intent_prefix=intent_prefix,
            priority=priority,
            policy_hints=policy_hints,
            metadata=metadata,
            state=state,
            error=error,
        )
        logger.info(
            "Finished recording transfer context evidence: context_id=%s",
            context_id,
        )


class RuntimeTransferService:
    def __init__(self, session) -> None:
        self._session = session

    def fetch_h2d(
        self,
        source: SharedPinnedCpuBuffer,
        target: CudaIpcDeviceBuffer,
        *,
        ranges: Iterable[Any] | None = None,
        chunk_bytes: int | None = None,
        workload_kind: WorkloadKind | str = WorkloadKind.GENERIC,
        priority: int = 0,
        metadata: Mapping[str, object] | None = None,
        policy_hints: Mapping[str, object] | None = None,
        intent_id: str | None = None,
    ) -> TransferReceipt:
        # /*
        #  * ========================================================================
        #  * Step 1: Submit H2D transfer
        #  * ========================================================================
        #  * Target:
        #  *   1) Normalize the direction to h2d.
        #  *   2) Reuse the RuntimeSession transfer core.
        #  */
        logger.info("Start submitting H2D transfer")

        # // 1.1 Submit through the canonical transfer core.
        receipt = self._session._submit_transfer_intent(
            source,
            target,
            direction="h2d",
            ranges=ranges,
            chunk_bytes=chunk_bytes,
            workload_kind=workload_kind,
            priority=priority,
            metadata=metadata,
            policy_hints=policy_hints,
            intent_id=intent_id,
        )
        logger.info("Finished submitting H2D transfer: intent_id=%s", receipt.intent_id)
        return receipt

    def offload_d2h(
        self,
        source: CudaIpcDeviceBuffer,
        target: SharedPinnedCpuBuffer,
        *,
        ranges: Iterable[Any] | None = None,
        chunk_bytes: int | None = None,
        workload_kind: WorkloadKind | str = WorkloadKind.GENERIC,
        priority: int = 0,
        metadata: Mapping[str, object] | None = None,
        policy_hints: Mapping[str, object] | None = None,
        intent_id: str | None = None,
    ) -> TransferReceipt:
        # /*
        #  * ========================================================================
        #  * Step 2: Submit D2H transfer
        #  * ========================================================================
        #  * Target:
        #  *   1) Normalize the direction to d2h.
        #  *   2) Reuse the RuntimeSession transfer core.
        #  */
        logger.info("Start submitting D2H transfer")

        # // 2.1 Submit through the canonical transfer core.
        receipt = self._session._submit_transfer_intent(
            source,
            target,
            direction="d2h",
            ranges=ranges,
            chunk_bytes=chunk_bytes,
            workload_kind=workload_kind,
            priority=priority,
            metadata=metadata,
            policy_hints=policy_hints,
            intent_id=intent_id,
        )
        logger.info("Finished submitting D2H transfer: intent_id=%s", receipt.intent_id)
        return receipt

    def submit_intent(
        self,
        intent: TransferIntent,
        *,
        wait: bool = True,
        timeout_seconds: float | None = None,
    ) -> TransferReceipt:
        # /*
        #  * ========================================================================
        #  * Step 3: Submit explicit TransferIntent
        #  * ========================================================================
        #  * Target:
        #  *   1) Keep public intent submission behind TransferService.
        #  *   2) Return the finalized TransferReceipt.
        #  */
        logger.info("Start submitting explicit TransferIntent")

        # // 3.1 Delegate to the RuntimeSession intent core.
        receipt = self._session._submit_runtime_intent(
            intent,
            wait=wait,
            timeout_seconds=timeout_seconds,
        )
        logger.info(
            "Finished submitting explicit TransferIntent: intent_id=%s",
            receipt.intent_id,
        )
        return receipt

    def wait_receipt(
        self,
        intent_id: str,
        timeout_seconds: float | None = None,
    ) -> TransferReceipt:
        # /*
        #  * ========================================================================
        #  * Step 4: Wait for transfer receipt
        #  * ========================================================================
        #  * Target:
        #  *   1) Wait only for transfers visible to this RuntimeSession.
        #  *   2) Reuse the canonical receipt finalizer.
        #  */
        logger.info("Start waiting for transfer receipt")

        # // 4.1 Delegate to the RuntimeSession wait core.
        receipt = self._session._wait_transfer_receipt_impl(
            intent_id,
            timeout_seconds=timeout_seconds,
        )
        logger.info("Finished waiting for transfer receipt: intent_id=%s", receipt.intent_id)
        return receipt

    def recover_state(
        self,
        *,
        intent_id: str | None = None,
        transfer_id: str | None = None,
    ) -> dict[str, object]:
        # /*
        #  * ========================================================================
        #  * Step 5: Recover transfer state
        #  * ========================================================================
        #  * Target:
        #  *   1) Recover only transfers visible to this RuntimeSession.
        #  *   2) Synchronize recovered receipts with runtime records.
        #  */
        logger.info("Start recovering transfer state")

        # // 5.1 Delegate to the RuntimeSession recovery core.
        recovery = self._session._recover_transfer_state_impl(
            intent_id=intent_id,
            transfer_id=transfer_id,
        )
        logger.info("Finished recovering transfer state")
        return recovery


class RuntimeBufferService:
    def __init__(self, session) -> None:
        self._session = session

    def register_cpu_buffer(
        self,
        buffer: SharedPinnedCpuBuffer,
        *,
        runtime_owned: bool = False,
    ) -> SharedPinnedCpuBuffer:
        # /*
        #  * ========================================================================
        #  * Step 1: Register CPU buffer
        #  * ========================================================================
        #  * Target:
        #  *   1) Validate runtime-owned buffer ownership.
        #  *   2) Bind the buffer to the RuntimeSession record core.
        #  */
        logger.info("Start registering CPU buffer")

        # // 1.1 Delegate to the RuntimeSession buffer core.
        result = self._session._register_cpu_buffer_impl(
            buffer,
            runtime_owned=runtime_owned,
        )
        logger.info("Finished registering CPU buffer: buffer_id=%s", result.buffer_id)
        return result

    def allocate_cpu_buffer(
        self,
        buffer_id: str,
        size_bytes: int,
        *,
        name_prefix: str = "turbobus-runtime",
    ) -> SharedPinnedCpuBuffer:
        # /*
        #  * ========================================================================
        #  * Step 2: Allocate and register CPU buffer
        #  * ========================================================================
        #  * Target:
        #  *   1) Allocate a runtime-owned pinned CPU buffer.
        #  *   2) Release local backing if registration fails.
        #  */
        logger.info("Start allocating CPU buffer")

        # // 2.1 Delegate to the RuntimeSession allocation core.
        result = self._session._allocate_cpu_buffer_impl(
            buffer_id,
            size_bytes,
            name_prefix=name_prefix,
        )
        logger.info("Finished allocating CPU buffer: buffer_id=%s", result.buffer_id)
        return result

    def register_cuda_buffer(self, buffer: CudaIpcDeviceBuffer) -> CudaIpcDeviceBuffer:
        # /*
        #  * ========================================================================
        #  * Step 3: Register CUDA buffer
        #  * ========================================================================
        #  * Target:
        #  *   1) Validate the CUDA buffer type.
        #  *   2) Bind the target GPU buffer to the RuntimeSession record core.
        #  */
        logger.info("Start registering CUDA buffer")

        # // 3.1 Delegate to the RuntimeSession CUDA buffer core.
        result = self._session._register_cuda_buffer_impl(buffer)
        logger.info("Finished registering CUDA buffer: buffer_id=%s", result.buffer_id)
        return result

    def cleanup_buffer(
        self,
        buffer_id: str,
        *,
        reason: str = "runtime_buffer_released",
        force: bool = False,
    ) -> DaemonResponse:
        # /*
        #  * ========================================================================
        #  * Step 4: Clean runtime buffer
        #  * ========================================================================
        #  * Target:
        #  *   1) Release daemon buffer and lease bindings.
        #  *   2) Release runtime-owned local CPU backing.
        #  */
        logger.info("Start cleaning runtime buffer")

        # // 4.1 Delegate to the RuntimeSession cleanup core.
        response = self._session._cleanup_buffer_impl(
            buffer_id,
            reason=reason,
            force=force,
        )
        logger.info("Finished cleaning runtime buffer: buffer_id=%s", buffer_id)
        return response


class RuntimeStateOffloadFactory:
    def __init__(self, session) -> None:
        self._session = session

    def make_transfer_context(self, cpu_buffer, gpu_buffer, **kwargs):
        # /*
        #  * ========================================================================
        #  * Step 1: Create transfer context
        #  * ========================================================================
        #  * Target:
        #  *   1) Bind workload settings to CPU/GPU buffers.
        #  *   2) Return the shared TransferContext used by state workloads.
        #  */
        logger.info("Start creating transfer context")

        # // 1.1 Delegate to the RuntimeSession context core.
        context = self._session._make_transfer_context_impl(
            cpu_buffer,
            gpu_buffer,
            **kwargs,
        )
        logger.info("Finished creating transfer context: context_id=%s", context.intent_prefix)
        return context

    def make_offload_store(self, cpu_buffer, gpu_buffer, **kwargs):
        # /*
        #  * ========================================================================
        #  * Step 2: Create low-level offload store
        #  * ========================================================================
        #  * Target:
        #  *   1) Create the shared TransferContext.
        #  *   2) Bind it to OffloadStore as the transport primitive.
        #  */
        logger.info("Start creating offload store")

        # // 2.1 Delegate to the RuntimeSession store core.
        store = self._session._make_offload_store_impl(
            cpu_buffer,
            gpu_buffer,
            **kwargs,
        )
        logger.info("Finished creating offload store")
        return store

    def make_state_offload(self, spec, cpu_buffer, gpu_buffer, **kwargs):
        # /*
        #  * ========================================================================
        #  * Step 3: Create state offload core
        #  * ========================================================================
        #  * Target:
        #  *   1) Resolve workload behavior from StateOffloadSpec.
        #  *   2) Return StateOffloadCore as the only state migration kernel.
        #  */
        logger.info("Start creating state offload core")

        # // 3.1 Delegate to the RuntimeSession state offload core.
        core = self._session._make_state_offload_impl(
            spec,
            cpu_buffer,
            gpu_buffer,
            **kwargs,
        )
        logger.info("Finished creating state offload core: state_kind=%s", spec.state_kind)
        return core


__all__ = [
    "RuntimeBufferService",
    "RuntimeEvidenceRecorder",
    "RuntimeStateOffloadFactory",
    "RuntimeTransferService",
]
