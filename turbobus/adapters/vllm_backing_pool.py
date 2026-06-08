from __future__ import annotations

import itertools
from typing import Any

from ..client import SharedPinnedCpuBuffer
from .vllm_prefix_store import TurboBusSavedPrefix


class TurboBusCPUBackingPool:
    def __init__(
        self,
        *,
        job_id: str | None = None,
        buffer_id_prefix: str = "vllm-kv-cpu",
    ) -> None:
        self.job_id = None if job_id is None else str(job_id)
        self.buffer_id_prefix = str(buffer_id_prefix)
        self._next_buffer_id = itertools.count(1)
        self._free_by_shape: dict[tuple[tuple[int, int], ...], list[list[Any]]] = {}

    def acquire(self, block_count: int, kv_caches: list[Any]) -> tuple[list[Any], bool]:
        signature = backing_signature(block_count, kv_caches)
        available = self._free_by_shape.get(signature)
        if available:
            return available.pop(), True
        return self._allocate_for_pool(block_count, kv_caches), False

    def release(
        self,
        block_count: int,
        kv_caches: list[Any],
        cpu_backings: list[Any],
    ) -> dict[str, Any]:
        signature = backing_signature(block_count, kv_caches)
        self._free_by_shape.setdefault(signature, []).append(list(cpu_backings))
        return {
            "action": "release_to_pool",
            "block_count": int(block_count),
            "backing_count": len(cpu_backings),
            "signature": [list(item) for item in signature],
            "free_groups_for_signature": len(self._free_by_shape[signature]),
        }

    def release_prefix(
        self,
        prefix: TurboBusSavedPrefix,
        kv_caches: list[Any],
    ) -> dict[str, Any]:
        evidence = self.release(prefix.block_count, kv_caches, prefix.cpu_backings)
        evidence.update(
            {
                "prefix_key": prefix.key,
                "job_id": prefix.job_id,
                "session_id": prefix.session_id,
                "source_request_id": prefix.source_request_id,
            }
        )
        return evidence

    def close_backings(self, cpu_backings: list[Any]) -> list[dict[str, Any]]:
        evidence = []
        for backing in cpu_backings:
            evidence.append(_close_backing(backing))
        return evidence

    def close_prefix(self, prefix: TurboBusSavedPrefix) -> dict[str, Any]:
        backing_evidence = self.close_backings(prefix.cpu_backings)
        return {
            "action": "close_prefix_backings",
            "prefix_key": prefix.key,
            "job_id": prefix.job_id,
            "session_id": prefix.session_id,
            "source_request_id": prefix.source_request_id,
            "backing_count": len(prefix.cpu_backings),
            "backings": backing_evidence,
        }

    def close(self) -> list[dict[str, Any]]:
        evidence = []
        for groups in self._free_by_shape.values():
            for cpu_backings in groups:
                evidence.append(
                    {
                        "action": "close_free_backing_group",
                        "backings": self.close_backings(cpu_backings),
                    }
                )
        self._free_by_shape.clear()
        return evidence

    @staticmethod
    def _allocate(
        block_count: int,
        kv_caches: list[Any],
        *,
        job_id: str | None = None,
        buffer_id_prefix: str = "vllm-kv-cpu",
    ) -> list[Any]:
        if job_id is not None:
            return _allocate_shared_cpu_backings(
                block_count,
                kv_caches,
                job_id=str(job_id),
                buffer_id_prefix=str(buffer_id_prefix),
                next_buffer_id=itertools.count(1),
            )
        try:
            import torch
        except ImportError as exc:  # pragma: no cover - import-time convenience only
            raise RuntimeError("PyTorch is required to allocate vLLM CPU backings") from exc

        slots_per_layer = max(1, int(block_count) * max_lanes_per_layer(kv_caches))
        backings = []
        for kv_cache in kv_caches:
            from .vllm import block_bytes_from_vllm_kv_tensor

            block_bytes = block_bytes_from_vllm_kv_tensor(kv_cache)
            backings.append(
                torch.empty(
                    slots_per_layer * block_bytes,
                    dtype=torch.uint8,
                    pin_memory=True,
                )
            )
        return backings

    def _allocate_for_pool(self, block_count: int, kv_caches: list[Any]) -> list[Any]:
        if self.job_id is None:
            return self._allocate(block_count, kv_caches)
        return _allocate_shared_cpu_backings(
            block_count,
            kv_caches,
            job_id=self.job_id,
            buffer_id_prefix=self.buffer_id_prefix,
            next_buffer_id=self._next_buffer_id,
        )


def _allocate_shared_cpu_backings(
    block_count: int,
    kv_caches: list[Any],
    *,
    job_id: str,
    buffer_id_prefix: str,
    next_buffer_id,
) -> list[SharedPinnedCpuBuffer]:
    from .vllm import block_bytes_from_vllm_kv_tensor

    slots_per_layer = max(1, int(block_count) * max_lanes_per_layer(kv_caches))
    backings = []
    for kv_cache in kv_caches:
        block_bytes = block_bytes_from_vllm_kv_tensor(kv_cache)
        backings.append(
            SharedPinnedCpuBuffer.allocate(
                buffer_id=f"{buffer_id_prefix}-{next(next_buffer_id)}",
                job_id=job_id,
                size_bytes=slots_per_layer * block_bytes,
                name_prefix="turbobus-vllm",
            )
        )
    return backings


def max_lanes_per_layer(kv_caches: list[Any]) -> int:
    return max(
        (
            int(kv_cache.shape[0]) if len(getattr(kv_cache, "shape", ())) >= 3 else 1
            for kv_cache in kv_caches
        ),
        default=1,
    )


def backing_signature(block_count: int, kv_caches: list[Any]) -> tuple[tuple[int, int], ...]:
    from .vllm import block_bytes_from_vllm_kv_tensor

    slots_per_layer = max(1, int(block_count) * max_lanes_per_layer(kv_caches))
    return tuple(
        (slots_per_layer, block_bytes_from_vllm_kv_tensor(kv_cache))
        for kv_cache in kv_caches
    )


def _close_backing(backing: Any) -> dict[str, Any]:
    evidence = {
        "backing_type": type(backing).__name__,
        "buffer_id": getattr(backing, "buffer_id", None),
    }
    release = getattr(backing, "release", None)
    if callable(release):
        release()
        evidence["action"] = "release"
        evidence["closed"] = bool(getattr(backing, "closed", False))
        return evidence
    close = getattr(backing, "close", None)
    if callable(close):
        close()
        evidence["action"] = "close"
        evidence["closed"] = bool(getattr(backing, "closed", False))
        return evidence
    evidence["action"] = "none"
    evidence["closed"] = bool(getattr(backing, "closed", False))
    return evidence


__all__ = [
    "TurboBusCPUBackingPool",
    "backing_signature",
    "max_lanes_per_layer",
]
