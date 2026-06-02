from __future__ import annotations

from typing import Any

try:
    import torch
except ImportError:  # pragma: no cover - import-time convenience only
    torch = None


def bind_torch(torch_module: Any) -> None:
    global torch
    torch = torch_module


def require_torch() -> None:
    if torch is None:
        raise RuntimeError("PyTorch is required for tensor based TurboBus APIs")


def validate_transfer_tensors(cpu_tensor, gpu_tensor, target_gpu: int, direction: str) -> int:
    if direction not in {"h2d", "d2h"}:
        raise ValueError(f"unsupported transfer direction: {direction}")
    validate_tensor_pair(cpu_tensor, gpu_tensor, target_gpu)

    if direction == "h2d":
        bytes_to_copy = cpu_tensor.numel() * cpu_tensor.element_size()
        if gpu_tensor.numel() * gpu_tensor.element_size() < bytes_to_copy:
            raise ValueError("gpu_tensor is smaller than cpu_tensor")
    else:
        bytes_to_copy = gpu_tensor.numel() * gpu_tensor.element_size()
        if cpu_tensor.numel() * cpu_tensor.element_size() < bytes_to_copy:
            raise ValueError("cpu_tensor is smaller than gpu_tensor")
    return bytes_to_copy


def validate_range_tensors(
    cpu_tensor,
    gpu_tensor,
    target_gpu: int,
    direction: str,
) -> tuple[int, int]:
    validate_tensor_pair(cpu_tensor, gpu_tensor, target_gpu)
    cpu_bytes = cpu_tensor.numel() * cpu_tensor.element_size()
    gpu_bytes = gpu_tensor.numel() * gpu_tensor.element_size()
    if direction == "h2d":
        return cpu_bytes, gpu_bytes
    if direction != "d2h":
        raise ValueError(f"unsupported transfer direction: {direction}")
    return gpu_bytes, cpu_bytes


def validate_tensor_pair(cpu_tensor, gpu_tensor, target_gpu: int) -> None:
    require_torch()
    if not isinstance(cpu_tensor, torch.Tensor):
        raise TypeError("cpu_tensor must be a torch.Tensor")
    if not isinstance(gpu_tensor, torch.Tensor):
        raise TypeError("gpu_tensor must be a torch.Tensor")
    if cpu_tensor.device.type != "cpu":
        raise ValueError("cpu_tensor must be on CPU")
    if not cpu_tensor.is_pinned():
        raise ValueError("cpu_tensor must be pinned memory")
    if gpu_tensor.device.type != "cuda":
        raise ValueError("gpu_tensor must be on CUDA")
    if gpu_tensor.device.index != target_gpu:
        raise ValueError("gpu_tensor must be on the runtime target_gpu")
    if not cpu_tensor.is_contiguous() or not gpu_tensor.is_contiguous():
        raise ValueError("cpu_tensor and gpu_tensor must be contiguous")
