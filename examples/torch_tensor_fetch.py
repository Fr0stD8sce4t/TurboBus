from __future__ import annotations

import argparse

from turbobus import CudaIpcDeviceBuffer, TurboBusRuntimeSession, WorkloadKind
from turbobus.runtime_options import RuntimeOptions


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch one pinned CPU buffer into a torch CUDA tensor through RuntimeSession"
    )
    parser.add_argument("--daemon-socket-path", required=True)
    parser.add_argument("--worker-socket-path", required=True)
    parser.add_argument("--job-id", default="example-torch-tensor-fetch")
    parser.add_argument("--cpu-buffer-id", default="example-cpu-buffer")
    parser.add_argument("--gpu-buffer-id", default="example-gpu-buffer")
    parser.add_argument("--target-gpu", type=int, default=0)
    parser.add_argument("--bytes", type=int, required=True)
    parser.add_argument("--chunk-bytes", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--intent-id", default="example-torch-tensor-fetch-0")
    parser.add_argument("--mode", choices=["auto", "pool", "direct"], default="auto")
    parser.add_argument("--wait-timeout-seconds", type=float, default=5.0)
    return parser


def validate_args(args) -> None:
    if args.bytes <= 0:
        raise ValueError("--bytes must be positive")
    if args.chunk_bytes <= 0:
        raise ValueError("--chunk-bytes must be positive")
    if args.target_gpu < 0:
        raise ValueError("--target-gpu must be non-negative")


def receipt_line(receipt) -> str:
    direct_bytes = 0
    relay_bytes = 0
    for path in receipt.path_stats:
        bytes_count = int(path.get("bytes", 0) or 0)
        if str(path.get("kind", "")).lower() == "relay":
            relay_bytes += bytes_count
        else:
            direct_bytes += bytes_count
    return (
        "runtime_receipt "
        f"intent_id={receipt.intent_id} "
        f"decision_id={receipt.decision_id} "
        f"topology_snapshot_id={receipt.topology_snapshot_id} "
        f"ticket_id={receipt.ticket_id} "
        f"state={receipt.state.value} "
        f"bytes_total={receipt.bytes_total} "
        f"bytes_completed={receipt.bytes_completed} "
        f"direct_bytes={direct_bytes} "
        f"relay_bytes={relay_bytes}"
    )


def main() -> None:
    args = build_parser().parse_args()
    validate_args(args)

    try:
        import torch
    except ImportError as exc:
        raise RuntimeError("torch tensor fetch example requires PyTorch") from exc
    if not torch.cuda.is_available():
        raise RuntimeError("torch tensor fetch example requires CUDA")

    torch.cuda.set_device(args.target_gpu)
    target = torch.empty(args.bytes, dtype=torch.uint8, device=f"cuda:{args.target_gpu}")
    runtime_options = RuntimeOptions(
        chunk_bytes=int(args.chunk_bytes),
        profile_on_first_transfer=True,
    )

    session = TurboBusRuntimeSession.open_production_socket(
        daemon_socket_path=args.daemon_socket_path,
        worker_socket_path=args.worker_socket_path,
        job_id=args.job_id,
        runtime_options=runtime_options,
    )
    try:
        gpu_buffer = CudaIpcDeviceBuffer.from_device_pointer(
            buffer_id=args.gpu_buffer_id,
            job_id=args.job_id,
            device_index=args.target_gpu,
            size_bytes=args.bytes,
            device_ptr=target.data_ptr(),
        )
        session.register_cuda_buffer(gpu_buffer)
        cpu_buffer = session.allocate_cpu_buffer(args.cpu_buffer_id, args.bytes)
        cpu_buffer.write(bytes((index % 251 for index in range(args.bytes))))
        receipt = session.fetch_h2d(
            cpu_buffer,
            gpu_buffer,
            chunk_bytes=args.chunk_bytes,
            workload_kind=WorkloadKind.GENERIC,
            policy_hints={"transfer_mode": args.mode},
            metadata={"example": "torch-tensor-fetch"},
            intent_id=args.intent_id,
        )
        if receipt.state.value not in {"complete", "failed", "canceled"}:
            receipt = session.wait_transfer_receipt(
                receipt.intent_id,
                timeout_seconds=args.wait_timeout_seconds,
            )
        print(receipt_line(receipt))
    finally:
        session.close()


if __name__ == "__main__":
    main()
