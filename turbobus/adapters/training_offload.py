from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from ..offload.blocks import BlockState, OffloadBlock, OffloadBlockInfo
from ..offload.context import AdapterTransferContext, forbidden_physical_policy_keys
from ..offload.lifecycle import adapter_lifecycle_evidence_from_handles
from ..offload.stats import TransferStats
from ..offload.store import OffloadBatch, OffloadStore
from ..schema import WorkloadKind


@dataclass(frozen=True)
class TrainingOffloadBucket:
    """One runtime-session-bound training-state bucket descriptor."""

    name: str
    bucket_id: object
    cpu_offset: int
    gpu_offset: int
    byte_count: int | None = None
    cpu_slot: object | None = None
    gpu_slot: object | None = None


@dataclass(frozen=True)
class TrainingOffloadLifecycle:
    operation: str
    names: tuple[str, ...]
    evidence: dict[str, Any] = field(default_factory=dict)


class TrainingOffloadManager(OffloadStore):
    """Runtime-session-owned training-state movement API over daemon transfer intent."""

    def __init__(
        self,
        runtime_session,
        cpu_buffer,
        gpu_buffer,
        *,
        workload_kind: WorkloadKind | str = WorkloadKind.TRAINING_STATE,
        priority: int = 0,
        metadata: Mapping[str, object] | None = None,
        intent_prefix: str | None = None,
        wait_timeout_seconds: float | None = None,
    ) -> None:
        context = runtime_session.make_adapter_transfer_context(
            cpu_buffer,
            gpu_buffer,
            workload_kind=workload_kind,
            priority=priority,
            metadata=_validate_training_offload_metadata(
                metadata,
                field_name="training offload metadata",
            ),
            intent_prefix=intent_prefix,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        if not isinstance(context, AdapterTransferContext):
            raise TypeError(
                "runtime session adapter context factory must return an AdapterTransferContext"
            )
        self._init_from_transfer_context(runtime_session, context, cpu_buffer, gpu_buffer)

    @classmethod
    def _from_transfer_context(
        cls,
        runtime_session,
        transfer_context: AdapterTransferContext,
        cpu_buffer,
        gpu_buffer,
    ) -> "TrainingOffloadManager":
        instance = cls.__new__(cls)
        instance._init_from_transfer_context(
            runtime_session,
            transfer_context,
            cpu_buffer,
            gpu_buffer,
        )
        return instance

    def _init_from_transfer_context(
        self,
        runtime_session,
        transfer_context: AdapterTransferContext,
        cpu_buffer,
        gpu_buffer,
    ) -> None:
        super().__init__(runtime_session, transfer_context)
        _validate_training_offload_metadata(
            transfer_context.metadata,
            field_name="training offload context metadata",
        )
        self.cpu_buffer = cpu_buffer
        self.gpu_buffer = gpu_buffer
        self._transfer_lifecycle_history: list[TrainingOffloadLifecycle] = []
        self._last_transfer_lifecycle: TrainingOffloadLifecycle | None = None

    @property
    def last_transfer_lifecycle(self) -> TrainingOffloadLifecycle | None:
        return self._last_transfer_lifecycle

    @property
    def transfer_lifecycle_history(self) -> tuple[TrainingOffloadLifecycle, ...]:
        return tuple(self._transfer_lifecycle_history)

    def register_buckets(
        self,
        buckets: Iterable[TrainingOffloadBucket],
    ) -> list[OffloadBlock]:
        registered: list[OffloadBlock] = []
        for bucket in buckets:
            registered.append(
                self.add_bucket(
                    bucket.name,
                    bucket_id=bucket.bucket_id,
                    cpu_slot=bucket.cpu_slot,
                    gpu_slot=bucket.gpu_slot,
                    cpu_offset=bucket.cpu_offset,
                    gpu_offset=bucket.gpu_offset,
                    byte_count=bucket.byte_count,
                )
            )
        return registered

    def add_bucket(
        self,
        name: str,
        *,
        bucket_id=None,
        cpu_slot=None,
        gpu_slot=None,
        cpu_offset: int = 0,
        gpu_offset: int = 0,
        byte_count: int | None = None,
    ) -> OffloadBlock:
        return self.add(
            name,
            self.cpu_buffer,
            self.gpu_buffer,
            block_id=name if bucket_id is None else bucket_id,
            cpu_slot=cpu_slot if cpu_slot is not None else bucket_id,
            gpu_slot=gpu_slot if gpu_slot is not None else bucket_id,
            cpu_offset=cpu_offset,
            gpu_offset=gpu_offset,
            byte_count=byte_count,
        )

    def add_packed_buckets(
        self,
        prefix: str,
        *,
        bucket_bytes: int,
        bucket_count: int,
        start_offset: int = 0,
    ) -> list[OffloadBlock]:
        if bucket_bytes <= 0:
            raise ValueError("bucket_bytes must be positive")
        if bucket_count <= 0:
            raise ValueError("bucket_count must be positive")
        if start_offset < 0:
            raise ValueError("start_offset must be non-negative")

        blocks = []
        for index in range(bucket_count):
            offset = start_offset + index * bucket_bytes
            blocks.append(
                self.add_bucket(
                    f"{prefix}{index}",
                    bucket_id=index,
                    cpu_offset=offset,
                    gpu_offset=offset,
                    byte_count=bucket_bytes,
                )
            )
        return blocks

    def bucket(self, name: str) -> OffloadBlock:
        return self.block(name)

    def bucket_info(self, name: str) -> OffloadBlockInfo:
        return self.block_info(name)

    def bucket_infos(self, names: Iterable[str] | None = None) -> list[OffloadBlockInfo]:
        return self.block_infos(names)

    def prefetch_bucket(self, name: str):
        handles = self._run_selected(
            [name],
            operation="prefetch_bucket",
            submitter=self.submit_prefetch_many,
        )
        return handles[0]

    def prefetch_buckets(self, names: Iterable[str]) -> list:
        return self._run_selected(
            names,
            operation="prefetch_buckets",
            submitter=self.submit_prefetch_many,
        )

    def submit_prefetch_buckets(self, names: Iterable[str]) -> OffloadBatch:
        return self._submit_selected(
            names,
            operation="submit_prefetch_buckets",
            submitter=self.submit_prefetch_many,
        )

    def prefetch_batch(self, names: Iterable[str]) -> OffloadBatch:
        return self.submit_prefetch_buckets(names)

    def prefetch_prefix(self, names: Iterable[str]) -> list:
        return self._run_selected(
            names,
            operation="prefetch_prefix",
            submitter=self.submit_prefetch_many,
        )

    def prefetch_all(self) -> list:
        return self._run_selected(
            self.names(),
            operation="prefetch_all",
            submitter=self.submit_prefetch_many,
        )

    def offload_bucket(self, name: str):
        handles = self._run_selected(
            [name],
            operation="offload_bucket",
            submitter=self.submit_evict_many,
        )
        return handles[0]

    def offload_buckets(self, names: Iterable[str]) -> list:
        return self._run_selected(
            names,
            operation="offload_buckets",
            submitter=self.submit_evict_many,
        )

    def submit_offload_buckets(self, names: Iterable[str]) -> OffloadBatch:
        return self._submit_selected(
            names,
            operation="submit_offload_buckets",
            submitter=self.submit_evict_many,
        )

    def offload_batch(self, names: Iterable[str]) -> OffloadBatch:
        return self.submit_offload_buckets(names)

    def offload_prefix(self, names: Iterable[str]) -> list:
        return self._run_selected(
            names,
            operation="offload_prefix",
            submitter=self.submit_evict_many,
        )

    def offload_all(self) -> list:
        return self._run_selected(
            self.names(),
            operation="offload_all",
            submitter=self.submit_evict_many,
        )

    def wait_all(self) -> None:
        self.wait_many(self.names())

    def wait_many(self, names: Iterable[str]) -> None:
        names = self._normalize_names(names)
        super().wait_many(names)
        self._record_transfer_lifecycle(
            operation="wait_many",
            names=names,
            handles=self._handles_for_names(names),
        )

    def transfer_stats(self, names: Iterable[str]) -> TransferStats:
        return self.transfer_stats_many(names)

    def mark_on_cpu(self, names: Iterable[str] | None = None) -> None:
        selected = self.names() if names is None else list(names)
        for name in selected:
            self.set_block_state(name, BlockState.CPU, clear_transfer_state=True)

    def mark_on_gpu(self, names: Iterable[str] | None = None) -> None:
        selected = self.names() if names is None else list(names)
        for name in selected:
            self.set_block_state(name, BlockState.GPU, clear_transfer_state=True)

    def _run_selected(
        self,
        names: Iterable[str],
        *,
        operation: str,
        submitter,
    ) -> list:
        names = self._normalize_names(names)
        if not names:
            return []
        batch = submitter(names)
        super().wait_many(names)
        self._record_transfer_lifecycle(
            operation=operation,
            names=names,
            handles=batch.handles,
        )
        return list(batch.handles)

    def _submit_selected(
        self,
        names: Iterable[str],
        *,
        operation: str,
        submitter,
    ) -> OffloadBatch:
        names = self._normalize_names(names)
        batch = submitter(names)
        self._record_transfer_lifecycle(
            operation=operation,
            names=names,
            handles=batch.handles,
        )
        return batch

    def _record_transfer_lifecycle(
        self,
        *,
        operation: str,
        names: list[str],
        handles: Iterable[object],
    ) -> None:
        handles = list(handles)
        if not handles:
            return
        lifecycle = TrainingOffloadLifecycle(
            operation=str(operation),
            names=tuple(names),
            evidence=self._transfer_lifecycle_evidence(
                operation=operation,
                names=names,
                handles=handles,
            ),
        )
        self._last_transfer_lifecycle = lifecycle
        self._transfer_lifecycle_history.append(lifecycle)

    def _transfer_lifecycle_evidence(
        self,
        *,
        operation: str,
        names: list[str],
        handles: Iterable[object],
    ) -> dict[str, Any]:
        transfer_stats = self.transfer_stats_many(names).as_dict()
        return adapter_lifecycle_evidence_from_handles(
            evidence_id=(
                f"training-state-{self.transfer_context.session_id}-"
                f"{self.transfer_context.intent_prefix}-"
                f"{len(self._transfer_lifecycle_history) + 1}"
            ),
            operation=operation,
            transfer_context=self.transfer_context,
            item_field="bucket_names",
            item_count_field="bucket_count",
            item_names=names,
            handles=handles,
            transfer_stats=transfer_stats,
            extra={
                "adapter": "training_offload_manager",
                "operation_direction": self._operation_direction(operation, names),
                "adapter_submit_source": "TurboBusRuntimeSession",
                "adapter_handle_source": "ReceiptTransferHandle",
                "policy_source": "daemon_scheduler",
                "route_policy_visible_to_adapter": False,
                "physical_route_source": "daemon_scheduler",
                "runtime_buffer_binding": self._runtime_buffer_binding_evidence(),
                "bucket_bindings": self._bucket_binding_evidence(names),
                "bucket_ranges": self._bucket_range_evidence(names),
            },
        )

    def _runtime_buffer_binding_evidence(self) -> dict[str, Any]:
        return {
            "job_id": self.transfer_context.job_id,
            "session_id": self.transfer_context.session_id,
            "workload_kind": str(self.transfer_context.workload_kind.value),
            "cpu_buffer_id": self.transfer_context.cpu_buffer_id,
            "gpu_buffer_id": self.transfer_context.gpu_buffer_id,
            "intent_prefix": self.transfer_context.intent_prefix,
            "policy_hints": dict(self.transfer_context.policy_hints),
            "metadata": dict(self.transfer_context.metadata),
        }

    def _bucket_binding_evidence(self, names: Iterable[str]) -> list[dict[str, Any]]:
        bindings: list[dict[str, Any]] = []
        for name in names:
            block = self.block(name)
            bindings.append(
                {
                    "name": str(name),
                    "bucket_id": block.block_id,
                    "cpu_slot": block.cpu_slot,
                    "gpu_slot": block.gpu_slot,
                    "cpu_offset": int(block.cpu_offset),
                    "gpu_offset": int(block.gpu_offset),
                    "byte_count": int(block.bytes),
                    "last_operation": block.last_operation,
                    "last_direction": self._block_direction(block.last_operation),
                }
            )
        return bindings

    def _bucket_range_evidence(self, names: Iterable[str]) -> list[dict[str, Any]]:
        ranges: list[dict[str, Any]] = []
        for name in names:
            info = self.block_info(name)
            direction = self._block_direction(info.last_operation)
            if direction == "d2h":
                src_offset = int(info.gpu_offset)
                dst_offset = int(info.cpu_offset)
            else:
                src_offset = int(info.cpu_offset)
                dst_offset = int(info.gpu_offset)
            ranges.append(
                {
                    "name": info.name,
                    "block_id": info.block_id,
                    "cpu_slot": info.cpu_slot,
                    "gpu_slot": info.gpu_slot,
                    "direction": direction,
                    "src_offset": src_offset,
                    "dst_offset": dst_offset,
                    "cpu_offset": int(info.cpu_offset),
                    "gpu_offset": int(info.gpu_offset),
                    "bytes": int(info.bytes),
                    "state": info.state.value,
                    "last_operation": info.last_operation,
                    "last_intent_id": info.last_intent_id,
                    "last_receipt_id": info.last_receipt_id,
                    "last_ticket_id": info.last_ticket_id,
                    "last_decision_id": info.last_decision_id,
                    "last_topology_snapshot_id": info.last_topology_snapshot_id,
                    "last_receipt_state": info.last_receipt_state,
                    "last_transfer_error": info.last_transfer_error,
                }
            )
        return ranges

    def _operation_direction(self, operation: str, names: Iterable[str]) -> str:
        normalized = str(operation)
        if normalized.startswith("prefetch"):
            return "h2d"
        if normalized.startswith("offload"):
            return "d2h"
        directions = {
            self._block_direction(self.block(name).last_operation)
            for name in names
            if self.block(name).last_operation is not None
        }
        directions.discard("unknown")
        if len(directions) == 1:
            return next(iter(directions))
        if len(directions) > 1:
            return "mixed"
        return "unknown"

    @staticmethod
    def _block_direction(operation: str | None) -> str:
        if operation == "prefetch":
            return "h2d"
        if operation == "evict":
            return "d2h"
        return "unknown"

    def _handles_for_names(self, names: Iterable[str]) -> list[object]:
        handles = []
        seen = set()
        for name in names:
            handle = self.block(name).last_handle
            if handle is None or id(handle) in seen:
                continue
            seen.add(id(handle))
            handles.append(handle)
        return handles

    @staticmethod
    def _normalize_names(names: Iterable[str]) -> list[str]:
        return [str(name) for name in names]


def _validate_training_offload_metadata(
    metadata: Mapping[str, object] | None,
    *,
    field_name: str,
) -> dict[str, object]:
    resolved = {} if metadata is None else dict(metadata)
    invalid_keys = forbidden_physical_policy_keys(resolved)
    if invalid_keys:
        raise ValueError(
            f"{field_name} must not choose physical paths: "
            + ", ".join(str(key) for key in invalid_keys)
        )
    return resolved


__all__ = [
    "TrainingOffloadBucket",
    "TrainingOffloadLifecycle",
    "TrainingOffloadManager",
]
