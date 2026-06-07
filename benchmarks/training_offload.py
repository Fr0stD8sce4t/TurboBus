from __future__ import annotations

import argparse
import contextlib
import json
import os
from pathlib import Path
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import uuid

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from daemon_support import benchmark_job_id, receipt_to_trace, receipt_trace_line
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
    manager_factory=None,
) -> dict:
    with runtime_context(
        args,
        session_factory=session_factory,
        buffer_factory=buffer_factory,
        manager_factory=manager_factory,
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
            "summary": summarize_training(samples),
        }


def run_warmup(runtime, args) -> list[dict]:
    return [
        run_iteration(runtime, args, iteration=iteration, phase="warmup")
        for iteration in range(int(args.warmup))
    ]


def run_iteration(runtime, args, *, iteration: int, phase: str) -> dict:
    names = active_bucket_names(args, iteration=iteration)
    start = time.perf_counter()
    prefetch = run_transfer(runtime.manager.prefetch_batch(names), operation="prefetch")
    compute_ms = run_compute_delay(args.compute_delay_ms)
    offload = run_transfer(runtime.manager.offload_batch(names), operation="offload")
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
    receipt = first_receipt(batch.handles)
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
    for handle in handles:
        key = id(handle)
        if key in seen:
            continue
        seen.add(key)
        receipt = getattr(handle, "receipt", None)
        if isinstance(receipt, TransferReceipt):
            unique.append(receipt)
    if len(unique) != 1:
        raise RuntimeError(f"expected one receipt for batched transfer, got {len(unique)}")
    return unique[0]


def run_compute_delay(delay_ms: float) -> float:
    delay_ms = float(delay_ms)
    if delay_ms <= 0.0:
        return 0.0
    start = time.perf_counter()
    time.sleep(delay_ms / 1000.0)
    return (time.perf_counter() - start) * 1000.0


def summarize_training(samples: list[dict]) -> dict:
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
    total = [
        sample["prefetch"]["bytes"] + sample["offload"]["bytes"]
        for sample in samples
    ]
    transfer = [sample["transfer_ms"] for sample in samples]
    return {
        "iterations": len(samples),
        "median_iteration_ms": statistics.median(
            sample["iteration_ms"] for sample in samples
        ),
        "median_transfer_ms": statistics.median(transfer),
        "median_compute_ms": statistics.median(
            sample["compute_ms"] for sample in samples
        ),
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
def runtime_context(args, *, session_factory=None, buffer_factory=None, manager_factory=None):
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
            if manager_factory is None:
                manager = make_manager(args, session, buffers)
            else:
                manager = manager_factory(args, session, buffers)
            yield RuntimeBenchmarkState(
                session=session,
                buffers=buffers,
                manager=manager,
            )
        finally:
            buffers.release()


def make_manager(args, session, buffers):
    from turbobus.adapters.training_offload import TrainingOffloadManager

    manager = TrainingOffloadManager(
        session,
        buffers.cpu_buffer,
        buffers.gpu_buffer,
        workload_kind=workload_kind(args.workload_kind),
        metadata={
            "benchmark": "training-offload",
            "policy": args.policy,
            "storage_layout": args.storage_layout,
            "bucket_count": int(args.bucket_count),
            "active_buckets": active_bucket_count(args),
            "bucket_bytes": int(args.bucket_bytes),
            "chunk_bytes": int(args.chunk_bytes),
        },
        intent_prefix=f"{args.intent_prefix}-{args.run_id}",
        wait_timeout_seconds=args.wait_timeout_seconds,
    )
    manager.add_packed_buckets(
        "bucket-",
        bucket_bytes=int(args.bucket_bytes),
        bucket_count=int(args.bucket_count),
    )
    return manager


class RuntimeBenchmarkState:
    def __init__(self, *, session, buffers, manager) -> None:
        self.session = session
        self.buffers = buffers
        self.manager = manager


class RuntimeBuffers:
    def __init__(self, *, cpu_buffer, gpu_buffer, target_tensor=None) -> None:
        self.cpu_buffer = cpu_buffer
        self.gpu_buffer = gpu_buffer
        self.target_tensor = target_tensor

    def release(self) -> None:
        releaser = getattr(self.cpu_buffer, "release", None)
        if callable(releaser):
            releaser()


class TorchRuntimeBufferFactory:
    def allocate(self, args) -> RuntimeBuffers:
        torch = require_torch()
        byte_count = total_bytes(args)
        run_id = str(args.run_id).replace("/", "-")
        cpu_buffer_id = args.cpu_buffer_id or f"training-offload-cpu-{run_id}"
        gpu_buffer_id = args.gpu_buffer_id or f"training-offload-gpu-{run_id}"

        from turbobus.client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer

        cpu_buffer = SharedPinnedCpuBuffer.allocate(
            buffer_id=cpu_buffer_id,
            job_id=args.job_id,
            size_bytes=byte_count,
            name_prefix="turbobus-training-offload",
        )
        source = torch.empty(byte_count, dtype=torch.uint8, pin_memory=True)
        source.random_(0, 256)
        cpu_buffer.write(source.numpy().tobytes())

        torch.cuda.set_device(int(args.target_gpu))
        target = torch.empty(
            byte_count,
            dtype=torch.uint8,
            device=f"cuda:{int(args.target_gpu)}",
        )
        gpu_buffer = CudaIpcDeviceBuffer.from_device_pointer(
            buffer_id=gpu_buffer_id,
            job_id=args.job_id,
            device_index=int(args.target_gpu),
            size_bytes=byte_count,
            device_ptr=target.data_ptr(),
        )
        return RuntimeBuffers(
            cpu_buffer=cpu_buffer,
            gpu_buffer=gpu_buffer,
            target_tensor=target,
        )


class ProductionRuntimeSessionFactory:
    @contextlib.contextmanager
    def open(self, args):
        daemon_process = None
        worker_process = None
        tmpdir = None
        session = None
        try:
            tmpdir = tempfile.TemporaryDirectory(prefix="turbobus-training-offload-")
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
        raise RuntimeError("training offload benchmark requires PyTorch") from exc
    if not torch.cuda.is_available():
        raise RuntimeError("training offload benchmark requires CUDA")
    return torch


def start_daemon_process(args, daemon_socket: str) -> subprocess.Popen:
    return start_service(
        [
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
    )


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
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def wait_for_socket(
    socket_path: str,
    process: subprocess.Popen,
    *,
    timeout_seconds: float = 30.0,
) -> None:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None
    while time.time() < deadline:
        returncode = process.poll()
        if returncode is not None:
            stdout, stderr = process.communicate(timeout=1.0)
            raise RuntimeError(
                f"service exited before socket became ready: rc={returncode} "
                f"stdout={stdout!r} stderr={stderr!r}"
            )
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            client.connect(socket_path)
            return
        except OSError as exc:
            last_error = exc
            time.sleep(0.05)
        finally:
            client.close()
    raise RuntimeError(
        f"socket did not become ready: {socket_path}; last_error={last_error}"
    )


def stop_service(process: subprocess.Popen | None) -> tuple[str, str]:
    if process is None:
        return "", ""
    if process.poll() is None:
        process.terminate()
        try:
            return process.communicate(timeout=5.0)
        except subprocess.TimeoutExpired:
            process.kill()
            return process.communicate(timeout=5.0)
    return process.communicate(timeout=1.0)


def print_service_output(service: str, stdout: str, stderr: str) -> None:
    if stdout.strip() or stderr.strip():
        print(
            f"{service}_service_output",
            f"stdout={stdout.strip()!r}",
            f"stderr={stderr.strip()!r}",
            flush=True,
        )


def write_json(path: str, result: dict) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


def write_text(path: str, text: str) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text + "\n", encoding="utf-8")


def compact_summary(result: dict) -> str:
    config = result["config"]
    summary = result["summary"]
    lines = [
        "TRAINING_OFFLOAD_SUMMARY_BEGIN",
        (
            "training_config "
            f"session_id={config['session_id']} job_id={config['job_id']} "
            f"cpu_buffer_id={config['cpu_buffer_id']} "
            f"gpu_buffer_id={config['gpu_buffer_id']} "
            f"workload_kind={config['workload_kind']} "
            f"target_gpu={config['target_gpu']} "
            f"bucket_count={config['bucket_count']} "
            f"active_buckets={config['active_buckets']} "
            f"bucket_bytes={config['bucket_bytes']} "
            f"storage_layout={config['storage_layout']} "
            f"chunk_bytes={config['chunk_bytes']} "
            f"iterations={config['iterations']} policy={config['policy']} "
            f"daemon_socket_path={config['daemon_socket_path']}"
        ),
        (
            "training_summary "
            f"median_iteration_ms={summary['median_iteration_ms']:.3f} "
            f"median_transfer_ms={summary['median_transfer_ms']:.3f} "
            f"median_compute_ms={summary['median_compute_ms']:.3f} "
            f"median_gib_s={summary['median_gib_per_second']:.3f}"
        ),
    ]
    for operation in ("prefetch", "offload"):
        side = summary[operation]
        lines.append(
            "training_transfer "
            f"op={operation} median_ms={side['median_transfer_ms']:.3f} "
            f"bytes={side['bytes']} bytes_completed={side['bytes_completed']} "
            f"direct_chunks={side['direct_chunks']} "
            f"relay_chunks={side['relay_chunks']} "
            f"direct_bytes={side['direct_bytes']} relay_bytes={side['relay_bytes']} "
            f"executed={side['executed']} verified={side['verified']} "
            f"content_match={side['content_match']}"
        )
    for sample in result["samples"]:
        lines.append(
            "training_sample "
            f"iteration={sample['iteration']} "
            f"iteration_ms={sample['iteration_ms']:.3f} "
            f"transfer_ms={sample['transfer_ms']:.3f} "
            f"compute_ms={sample['compute_ms']:.3f} "
            f"prefetch_decision_id={sample['prefetch']['decision_id']} "
            f"prefetch_topology_snapshot_id={sample['prefetch']['topology_snapshot_id']} "
            f"prefetch_ticket_id={sample['prefetch']['ticket_id']} "
            f"offload_decision_id={sample['offload']['decision_id']} "
            f"offload_topology_snapshot_id={sample['offload']['topology_snapshot_id']} "
            f"offload_ticket_id={sample['offload']['ticket_id']}"
        )
        lines.append(
            receipt_trace_line(
                receipt_from_trace(sample["prefetch"]["receipt"]),
                prefix="training_prefetch_receipt",
            )
        )
        lines.append(
            receipt_trace_line(
                receipt_from_trace(sample["offload"]["receipt"]),
                prefix="training_offload_receipt",
            )
        )
    lines.append("TRAINING_OFFLOAD_SUMMARY_END")
    return "\n".join(lines)


def receipt_from_trace(trace: dict):
    return TransferReceipt(
        receipt_id=trace["receipt_id"],
        ticket_id=trace["ticket_id"],
        intent_id=trace["intent_id"],
        decision_id=trace["decision_id"],
        topology_snapshot_id=trace["topology_snapshot_id"],
        job_id=trace["job_id"],
        session_id=trace["session_id"],
        state=trace["state"],
        bytes_total=trace["bytes_total"],
        bytes_completed=trace["bytes_completed"],
        started_at=trace.get("started_at", 0.0),
        completed_at=trace.get("completed_at"),
        path_stats=tuple(trace.get("path_stats", ())),
        error=trace.get("error"),
        metadata=trace.get("metadata", {}),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Move training-state buckets through TurboBus runtime session"
    )
    parser.add_argument("--job-id", default=benchmark_job_id("training-offload"))
    parser.add_argument("--target-gpu", type=int, required=True)
    parser.add_argument("--daemon-socket-path")
    parser.add_argument("--worker-socket-path")
    parser.add_argument("--start-services", action="store_true")
    parser.add_argument("--min-relays", type=int, default=1)
    parser.add_argument("--max-sessions-per-relay", type=int, default=1)
    parser.add_argument("--profile-bytes", type=int, default=256 * 1024 * 1024)
    parser.add_argument(
        "--workload-kind",
        choices=[WorkloadKind.TRAINING_STATE.value, WorkloadKind.OPTIMIZER_STATE.value],
        default=WorkloadKind.TRAINING_STATE.value,
    )
    parser.add_argument("--bucket-count", type=int, default=8)
    parser.add_argument("--active-buckets", type=int)
    parser.add_argument("--bucket-bytes", type=int, default=32 * 1024 * 1024)
    parser.add_argument("--storage-layout", choices=["packed"], default="packed")
    parser.add_argument("--chunk-bytes", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--warmup", type=int, default=0)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--compute-delay-ms", type=float, default=0.0)
    parser.add_argument("--policy", default="runtime-session")
    parser.add_argument("--run-id", default=str(uuid.uuid4()))
    parser.add_argument("--intent-prefix", default="training-offload")
    parser.add_argument("--wait-timeout-seconds", type=float)
    parser.add_argument("--daemon-max-inflight-chunks", type=int, default=8)
    parser.add_argument("--daemon-profile-max-age-seconds", type=float, default=3600.0)
    parser.add_argument("--json-output")
    parser.add_argument("--summary-output")
    parser.add_argument("--no-copy-summary", action="store_true")
    return parser


def validate_args(args) -> None:
    if not args.start_services and (not args.daemon_socket_path or not args.worker_socket_path):
        raise ValueError(
            "without --start-services, --daemon-socket-path and --worker-socket-path are required"
        )
    if args.bucket_count <= 0:
        raise ValueError("--bucket-count must be positive")
    if args.bucket_bytes <= 0:
        raise ValueError("--bucket-bytes must be positive")
    if args.chunk_bytes <= 0:
        raise ValueError("--chunk-bytes must be positive")
    active_buckets = active_bucket_count(args)
    if active_buckets <= 0 or active_buckets > args.bucket_count:
        raise ValueError("--active-buckets must be between 1 and --bucket-count")
    if args.warmup < 0:
        raise ValueError("--warmup must be non-negative")
    if args.iterations <= 0:
        raise ValueError("--iterations must be positive")
    if args.compute_delay_ms < 0.0:
        raise ValueError("--compute-delay-ms must be non-negative")
    if args.profile_bytes <= 0:
        raise ValueError("--profile-bytes must be positive")
    if args.daemon_max_inflight_chunks <= 0:
        raise ValueError("--daemon-max-inflight-chunks must be positive")
    args.session_id = None
    args.cpu_buffer_id = None
    args.gpu_buffer_id = None


def main() -> None:
    args = build_parser().parse_args()
    validate_args(args)
    result = run_benchmark(args)
    if args.json_output:
        write_json(args.json_output, result)
        print("json_output", args.json_output)
    summary = compact_summary(result)
    if args.summary_output:
        write_text(args.summary_output, summary)
        print("summary_output", args.summary_output)
    if not args.no_copy_summary:
        print(summary)


if __name__ == "__main__":
    main()
