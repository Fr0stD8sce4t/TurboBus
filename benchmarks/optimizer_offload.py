from __future__ import annotations

import argparse
import contextlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import time
import uuid

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from daemon_support import benchmark_job_id, receipt_to_trace
from state_offload_common import (
    RuntimeBenchmarkState,
    RuntimeBuffers,
    allocate_torch_runtime_buffers,
    make_state_offload_core,
)
from turbobus.runtime.evidence import validate_runtime_receipts
from turbobus.schema import TransferReceipt, WorkloadKind


def active_bucket_count(args) -> int:
    return int(args.bucket_count if args.active_buckets is None else args.active_buckets)


def total_bytes(args) -> int:
    return int(args.bucket_count) * int(args.bucket_bytes)


def active_bucket_names(args, *, iteration: int) -> list[str]:
    active = active_bucket_count(args)
    start = (int(iteration) * active) % int(args.bucket_count)
    return [
        f"bucket-{(start + offset) % int(args.bucket_count)}"
        for offset in range(active)
    ]


def workload_kind(value: str) -> WorkloadKind:
    return WorkloadKind(str(value))


def run_benchmark(
    args,
    *,
    session_factory=None,
    buffer_factory=None,
    core_factory=None,
) -> dict:
    with runtime_context(
        args,
        session_factory=session_factory,
        buffer_factory=buffer_factory,
        core_factory=core_factory,
    ) as runtime:
        warmup_samples = run_warmup(runtime, args)
        samples = [
            run_iteration(runtime, args, iteration=iteration, phase="measure")
            for iteration in range(int(args.iterations))
        ]
        return {
            "config": config_dict(args),
            "warmup_samples": warmup_samples,
            "samples": samples,
            "summary": summarize_optimizer(samples),
        }


def run_warmup(runtime, args) -> list[dict]:
    return [
        run_iteration(runtime, args, iteration=iteration, phase="warmup")
        for iteration in range(int(args.warmup))
    ]


def run_iteration(runtime, args, *, iteration: int, phase: str) -> dict:
    names = active_bucket_names(args, iteration=iteration)
    start = time.perf_counter()
    prefetch = run_transfer(
        runtime.core.submit_prefetch_states(names, operation="prefetch"),
        operation="prefetch",
    )
    compute_ms = run_compute_delay(args.compute_delay_ms)
    offload = run_transfer(
        runtime.core.submit_offload_states(names, operation="offload"),
        operation="offload",
    )
    iteration_ms = (time.perf_counter() - start) * 1000.0
    transfer_ms = prefetch["transfer_ms"] + offload["transfer_ms"]
    return {
        "iteration": int(iteration),
        "phase": phase,
        "active_buckets": active_bucket_count(args),
        "bucket_names": names,
        "iteration_ms": iteration_ms,
        "transfer_ms": transfer_ms,
        "compute_ms": compute_ms,
        "prefetch": prefetch,
        "offload": offload,
    }


def run_transfer(batch, *, operation: str) -> dict:
    start = time.perf_counter()
    batch.wait()
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    receipt = first_receipt(batch)
    trace = receipt_to_trace(receipt)
    return {
        "operation": operation,
        "transfer_ms": elapsed_ms,
        "receipt": trace,
        "bytes": int(trace["bytes_total"]),
        "bytes_completed": int(trace["bytes_completed"]),
        "direct_bytes": int(trace["direct_bytes"]),
        "relay_bytes": int(trace["relay_bytes"]),
        "direct_chunks": int(trace["direct_chunks"]),
        "relay_chunks": int(trace["relay_chunks"]),
        "decision_id": str(trace["decision_id"]),
        "topology_snapshot_id": str(trace["topology_snapshot_id"]),
        "ticket_id": str(trace["ticket_id"]),
        "fallback_reason": str(trace.get("fallback_reason", "") or ""),
    }


def first_receipt(handles) -> TransferReceipt:
    unique = []
    seen = set()
    source_handles = getattr(handles, "receipt_handles", None)
    if source_handles is None:
        source_handles = getattr(handles, "handles", handles)
    for handle in source_handles:
        key = id(handle)
        if key in seen:
            continue
        seen.add(key)
        receipt = getattr(handle, "receipt", None)
        if receipt is None:
            receipt = getattr(handle, "_receipt", None)
        if isinstance(receipt, TransferReceipt):
            unique.append(receipt)
    if len(unique) != 1:
        raise RuntimeError(f"expected one receipt for batched transfer, got {len(unique)}")
    validate_runtime_receipts(unique, source="benchmark_optimizer_offload")
    return unique[0]


def run_compute_delay(delay_ms: float) -> float:
    delay_ms = float(delay_ms)
    if delay_ms <= 0.0:
        return 0.0
    start = time.perf_counter()
    time.sleep(delay_ms / 1000.0)
    return (time.perf_counter() - start) * 1000.0


def summarize_optimizer(samples: list[dict]) -> dict:
    if not samples:
        return {
            "iterations": 0,
            "median_iteration_ms": 0.0,
            "median_transfer_ms": 0.0,
            "median_compute_ms": 0.0,
            "median_gib_per_second": 0.0,
            "prefetch": empty_transfer_summary(),
            "offload": empty_transfer_summary(),
        }
    total = [sample["prefetch"]["bytes"] + sample["offload"]["bytes"] for sample in samples]
    transfer = [sample["transfer_ms"] for sample in samples]
    return {
        "iterations": len(samples),
        "median_iteration_ms": statistics.median(
            sample["iteration_ms"] for sample in samples
        ),
        "median_transfer_ms": statistics.median(transfer),
        "median_compute_ms": statistics.median(sample["compute_ms"] for sample in samples),
        "median_gib_per_second": statistics.median(
            (bytes_ / (1024**3)) / (ms / 1000.0) if ms > 0.0 else 0.0
            for bytes_, ms in zip(total, transfer, strict=False)
        ),
        "prefetch": summarize_transfer_side(samples, "prefetch"),
        "offload": summarize_transfer_side(samples, "offload"),
    }


def empty_transfer_summary() -> dict:
    return {
        "median_transfer_ms": 0.0,
        "bytes": 0,
        "bytes_completed": 0,
        "direct_bytes": 0,
        "relay_bytes": 0,
        "direct_chunks": 0,
        "relay_chunks": 0,
        "decision_ids": [],
        "topology_snapshot_ids": [],
        "ticket_ids": [],
        "receipt_ids": [],
        "fallback_reasons": [],
        "executed": False,
        "verified": False,
        "verified_bytes": 0,
        "content_match": False,
        "verification_sources": [],
        "verification_methods": [],
    }


def summarize_transfer_side(samples: list[dict], operation: str) -> dict:
    return {
        "median_transfer_ms": statistics.median(
            sample[operation]["transfer_ms"] for sample in samples
        ),
        "bytes": int(statistics.median(sample[operation]["bytes"] for sample in samples)),
        "bytes_completed": int(
            statistics.median(sample[operation]["bytes_completed"] for sample in samples)
        ),
        "direct_bytes": int(
            statistics.median(sample[operation]["direct_bytes"] for sample in samples)
        ),
        "relay_bytes": int(
            statistics.median(sample[operation]["relay_bytes"] for sample in samples)
        ),
        "direct_chunks": int(
            statistics.median(sample[operation]["direct_chunks"] for sample in samples)
        ),
        "relay_chunks": int(
            statistics.median(sample[operation]["relay_chunks"] for sample in samples)
        ),
        "decision_ids": sorted({sample[operation]["decision_id"] for sample in samples}),
        "topology_snapshot_ids": sorted(
            {sample[operation]["topology_snapshot_id"] for sample in samples}
        ),
        "ticket_ids": sorted({sample[operation]["ticket_id"] for sample in samples}),
        "receipt_ids": sorted(
            {
                str(sample[operation]["receipt"].get("receipt_id", ""))
                for sample in samples
                if sample[operation]["receipt"].get("receipt_id")
            }
        ),
        "fallback_reasons": sorted(
            {
                sample[operation]["fallback_reason"]
                for sample in samples
                if sample[operation]["fallback_reason"]
            }
        ),
        **summarize_receipt_evidence(samples, operation),
    }


def summarize_receipt_evidence(samples: list[dict], operation: str) -> dict[str, object]:
    metadata = [receipt_metadata(sample[operation]) for sample in samples]
    return {
        "executed": bool(metadata) and all(bool(item.get("executed")) for item in metadata),
        "verified": bool(metadata) and all(bool(item.get("verified")) for item in metadata),
        "verified_bytes": int(
            statistics.median(int(item.get("verified_bytes", 0) or 0) for item in metadata)
        ) if metadata else 0,
        "content_match": bool(metadata)
        and all(bool(item.get("content_match")) for item in metadata),
        "verification_sources": sorted(
            {
                str(item.get("verification_source"))
                for item in metadata
                if item.get("verification_source")
            }
        ),
        "verification_methods": sorted(
            {
                str(item.get("verification_method"))
                for item in metadata
                if item.get("verification_method")
            }
        ),
    }


def receipt_metadata(sample: dict) -> dict[str, object]:
    receipt = sample.get("receipt", {})
    if not isinstance(receipt, dict):
        return {}
    metadata = receipt.get("metadata", {})
    if not isinstance(metadata, dict):
        return {}
    return metadata


def config_dict(args) -> dict[str, object]:
    return {
        "session_id": args.session_id,
        "job_id": args.job_id,
        "cpu_buffer_id": args.cpu_buffer_id,
        "gpu_buffer_id": args.gpu_buffer_id,
        "workload_kind": args.workload_kind,
        "target_gpu": args.target_gpu,
        "bucket_count": int(args.bucket_count),
        "active_buckets": active_bucket_count(args),
        "bucket_bytes": int(args.bucket_bytes),
        "storage_layout": args.storage_layout,
        "chunk_bytes": int(args.chunk_bytes),
        "warmup": int(args.warmup),
        "iterations": int(args.iterations),
        "compute_delay_ms": float(args.compute_delay_ms),
        "policy": args.policy,
        "run_id": args.run_id,
        "daemon_socket_path": args.daemon_socket_path,
        "worker_socket_path": args.worker_socket_path,
        "start_services": bool(args.start_services),
        "profile_bytes": int(args.profile_bytes),
        "daemon_max_inflight_chunks": int(args.daemon_max_inflight_chunks),
        "daemon_profile_max_age_seconds": float(args.daemon_profile_max_age_seconds),
    }


@contextlib.contextmanager
def runtime_context(args, *, session_factory=None, buffer_factory=None, core_factory=None):
    if session_factory is None:
        session_factory = ProductionRuntimeSessionFactory()
    if buffer_factory is None:
        buffer_factory = TorchRuntimeBufferFactory()
    with session_factory.open(args) as session:
        buffers = buffer_factory.allocate(args)
        try:
            session.register_cuda_buffer(buffers.gpu_buffer)
            args.session_id = session.open_session()
            session.register_cpu_buffer(buffers.cpu_buffer)
            args.cpu_buffer_id = buffers.cpu_buffer.buffer_id
            args.gpu_buffer_id = buffers.gpu_buffer.buffer_id
            if core_factory is None:
                core = make_core(args, session, buffers)
            else:
                core = core_factory(args, session, buffers)
            yield RuntimeBenchmarkState(
                session=session,
                buffers=buffers,
                core=core,
            )
        finally:
            buffers.release()


def make_core(args, session, buffers):
    from turbobus.state_offload import optimizer_state_spec

    return make_state_offload_core(
        session=session,
        buffers=buffers,
        args=args,
        benchmark_name="optimizer-offload",
        spec=optimizer_state_spec(),
        workload_kind=workload_kind(args.workload_kind),
        active_bucket_count=active_bucket_count,
    )


class TorchRuntimeBufferFactory:
    def allocate(self, args) -> RuntimeBuffers:
        byte_count = total_bytes(args)
        return allocate_torch_runtime_buffers(
            args=args,
            byte_count=byte_count,
            benchmark_name="optimizer-offload",
            require_torch=require_torch,
        )


class ProductionRuntimeSessionFactory:
    @contextlib.contextmanager
    def open(self, args):
        daemon_process = None
        worker_process = None
        tmpdir = None
        session = None
        try:
            tmpdir = tempfile.TemporaryDirectory(prefix="turbobus-optimizer-offload-")
            daemon_socket = args.daemon_socket_path or os.path.join(tmpdir.name, "daemon.sock")
            worker_socket = args.worker_socket_path or os.path.join(tmpdir.name, "worker.sock")
            args.daemon_socket_path = daemon_socket
            args.worker_socket_path = worker_socket

            if args.start_services:
                daemon_process = start_daemon_process(args, daemon_socket)
                wait_for_socket(daemon_socket, daemon_process)
                worker_process = start_worker_process(args, daemon_socket, worker_socket)
                wait_for_socket(worker_socket, worker_process)

            from turbobus.runtime_options import RuntimeOptions
            from turbobus.runtime_session import TurboBusRuntimeSession

            session = TurboBusRuntimeSession.open_production_socket(
                job_id=args.job_id,
                daemon_socket_path=daemon_socket,
                worker_socket_path=worker_socket,
                runtime_options=RuntimeOptions(
                    chunk_bytes=int(args.chunk_bytes),
                    profile_bytes=int(args.profile_bytes),
                    profile_on_first_transfer=True,
                    daemon_socket_path=daemon_socket,
                    worker_socket_path=worker_socket,
                    daemon_max_inflight_chunks=int(args.daemon_max_inflight_chunks),
                    daemon_profile_max_age_seconds=float(
                        args.daemon_profile_max_age_seconds
                    ),
                ),
            )
            yield session
        finally:
            if session is not None:
                session.close()
            worker_stdout, worker_stderr = stop_service(worker_process)
            daemon_stdout, daemon_stderr = stop_service(daemon_process)
            print_service_output("worker", worker_stdout, worker_stderr)
            print_service_output("daemon", daemon_stdout, daemon_stderr)
            if tmpdir is not None:
                tmpdir.cleanup()


def require_torch():
    try:
        import torch
    except ImportError as exc:
        raise RuntimeError("optimizer offload benchmark requires PyTorch") from exc
    if not torch.cuda.is_available():
        raise RuntimeError("optimizer offload benchmark requires CUDA")
    return torch


def start_daemon_process(args, daemon_socket: str) -> subprocess.Popen:
    command = [
        sys.executable,
        "-m",
        "turbobus.daemon",
        "--socket-path",
        daemon_socket,
        "--target-gpu",
        str(args.target_gpu),
        "--min-relays",
        str(args.min_relays),
        "--max-sessions-per-relay",
        str(args.max_sessions_per_relay),
        "--max-inflight-chunks-per-relay",
        str(args.daemon_max_inflight_chunks),
        "--profile-max-age-seconds",
        str(args.daemon_profile_max_age_seconds),
    ]
    if args.allow_missing_fabric:
        command.append("--allow-missing-fabric")
    if args.allow_missing_pcie:
        command.append("--allow-missing-pcie")
    return start_service(command)


def start_worker_process(args, daemon_socket: str, worker_socket: str) -> subprocess.Popen:
    return start_service(
        [
            sys.executable,
            "-m",
            "turbobus.worker",
            "--daemon-socket-path",
            daemon_socket,
            "--socket-path",
            worker_socket,
            "--chunk-bytes",
            str(args.chunk_bytes),
            "--profile-bytes",
            str(args.profile_bytes),
        ]
    )


def start_service(command: list[str]) -> subprocess.Popen:
    return subprocess.Popen(
        command,
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def wait_for_socket(socket_path: str, process: subprocess.Popen) -> None:
    deadline = time.time() + 30.0
    while time.time() < deadline:
        if os.path.exists(socket_path):
            return
        if process.poll() is not None:
            raise RuntimeError(f"service exited before socket appeared: {socket_path}")
        time.sleep(0.05)
    raise RuntimeError(f"timeout waiting for socket: {socket_path}")


def stop_service(process: subprocess.Popen | None):
    if process is None:
        return "", ""
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            process.kill()
    stdout = process.stdout.read() if process.stdout is not None else ""
    stderr = process.stderr.read() if process.stderr is not None else ""
    return stdout, stderr


def print_service_output(name: str, stdout: str, stderr: str) -> None:
    if stdout:
        print(f"[{name}:stdout]\n{stdout}")
    if stderr:
        print(f"[{name}:stderr]\n{stderr}", file=sys.stderr)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TurboBus optimizer offload benchmark")
    parser.add_argument("--target-gpu", type=int, required=True)
    parser.add_argument("--min-relays", type=int, default=1)
    parser.add_argument("--allow-missing-fabric", action="store_true")
    parser.add_argument("--allow-missing-pcie", action="store_true")
    parser.add_argument("--max-sessions-per-relay", type=int, default=1)
    parser.add_argument("--daemon-max-inflight-chunks", type=int, default=8)
    parser.add_argument("--daemon-profile-max-age-seconds", type=float, default=0.0)
    parser.add_argument("--chunk-bytes", type=int, default=4 * 1024 * 1024)
    parser.add_argument("--profile-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--bucket-count", type=int, default=4)
    parser.add_argument("--active-buckets", type=int, default=None)
    parser.add_argument("--bucket-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--storage-layout", default="packed")
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--compute-delay-ms", type=float, default=1.0)
    parser.add_argument("--policy", default="daemon-default")
    parser.add_argument("--run-id", default="optimizer-offload")
    parser.add_argument("--job-id", default=None)
    parser.add_argument("--session-id", default=None)
    parser.add_argument("--cpu-buffer-id", default=None)
    parser.add_argument("--gpu-buffer-id", default=None)
    parser.add_argument("--intent-prefix", default="optimizer-offload")
    parser.add_argument("--wait-timeout-seconds", type=float, default=0.0)
    parser.add_argument("--daemon-socket-path", default=None)
    parser.add_argument("--worker-socket-path", default=None)
    parser.add_argument("--start-services", action="store_true")
    parser.add_argument("--workload-kind", default=WorkloadKind.OPTIMIZER_STATE.value)
    return parser


def validate_args(args) -> None:
    if args.bucket_count <= 0:
        raise ValueError("bucket_count must be positive")
    if args.bucket_bytes <= 0:
        raise ValueError("bucket_bytes must be positive")
    if args.active_buckets is not None and args.active_buckets <= 0:
        raise ValueError("active_buckets must be positive")
    if args.active_buckets is not None and args.active_buckets > args.bucket_count:
        raise ValueError("active_buckets must not exceed bucket_count")
    if args.chunk_bytes <= 0:
        raise ValueError("chunk_bytes must be positive")
    if args.profile_bytes <= 0:
        raise ValueError("profile_bytes must be positive")
    if args.iterations < 0:
        raise ValueError("iterations must be non-negative")
    if args.warmup < 0:
        raise ValueError("warmup must be non-negative")
    if args.target_gpu < 0:
        raise ValueError("target_gpu must be non-negative")
    if args.job_id is None:
        args.job_id = benchmark_job_id(args.run_id)
    if args.session_id is None:
        args.session_id = f"session-{uuid.uuid4().hex}"


def config_dict(args) -> dict[str, object]:
    return {
        "session_id": args.session_id,
        "job_id": args.job_id,
        "cpu_buffer_id": args.cpu_buffer_id,
        "gpu_buffer_id": args.gpu_buffer_id,
        "workload_kind": args.workload_kind,
        "target_gpu": args.target_gpu,
        "bucket_count": int(args.bucket_count),
        "active_buckets": active_bucket_count(args),
        "bucket_bytes": int(args.bucket_bytes),
        "storage_layout": args.storage_layout,
        "chunk_bytes": int(args.chunk_bytes),
        "warmup": int(args.warmup),
        "iterations": int(args.iterations),
        "compute_delay_ms": float(args.compute_delay_ms),
        "policy": args.policy,
        "run_id": args.run_id,
        "daemon_socket_path": args.daemon_socket_path,
        "worker_socket_path": args.worker_socket_path,
        "start_services": bool(args.start_services),
        "profile_bytes": int(args.profile_bytes),
        "daemon_max_inflight_chunks": int(args.daemon_max_inflight_chunks),
        "daemon_profile_max_age_seconds": float(args.daemon_profile_max_age_seconds),
    }


def main() -> int:
    args = build_parser().parse_args()
    validate_args(args)
    result = run_benchmark(args)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
