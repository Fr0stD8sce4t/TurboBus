from __future__ import annotations

import argparse
import contextlib
import faulthandler
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


def trace_event(name: str, **fields) -> None:
    if os.environ.get("TURBOBUS_BENCHMARK_TRACE") != "1":
        return
    details = " ".join(f"{key}={value}" for key, value in sorted(fields.items()))
    print(f"turbobus_benchmark_stage name={name} {details}".rstrip(), flush=True)


def total_bytes(args) -> int:
    return int(args.bucket_count) * int(args.bucket_bytes)


def bucket_names(args, *, iteration: int) -> list[str]:
    return [f"bucket-{index}" for index in range(int(args.bucket_count))]


def run_benchmark(
    args,
    *,
    session_factory=None,
    buffer_factory=None,
    loader_factory=None,
) -> dict:
    with runtime_context(
        args,
        session_factory=session_factory,
        buffer_factory=buffer_factory,
        loader_factory=loader_factory,
    ) as runtime:
        warmup_samples = run_warmup(runtime, args)
        samples = [
            run_load_iteration(runtime, args, iteration=iteration, phase="measure")
            for iteration in range(int(args.iterations))
        ]
        return {
            "config": config_dict(args),
            "warmup_samples": warmup_samples,
            "samples": samples,
            "summary": summarize_load(samples),
        }


def run_warmup(runtime, args) -> list[dict]:
    return [
        run_load_iteration(runtime, args, iteration=iteration, phase="warmup")
        for iteration in range(int(args.warmup))
    ]


def run_load_iteration(runtime, args, *, iteration: int, phase: str) -> dict:
    loader = runtime.loader
    names = bucket_names(args, iteration=iteration)
    trace_event("model_load_batch_start", phase=phase, iteration=iteration, buckets=len(names))
    start = time.perf_counter()
    batch = loader.load_batch(names)
    trace_event("model_load_batch_submitted", phase=phase, iteration=iteration)
    batch.wait()
    trace_event("model_load_batch_waited", phase=phase, iteration=iteration)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    receipt = first_receipt(batch.handles)
    trace = receipt_to_trace(receipt)
    gib_per_second = (
        (int(trace["bytes_total"]) / (1024**3)) / (elapsed_ms / 1000.0)
        if elapsed_ms > 0.0
        else 0.0
    )
    return {
        "iteration": int(iteration),
        "phase": phase,
        "bucket_names": names,
        "load_ms": elapsed_ms,
        "load_gib_per_second": gib_per_second,
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
        raise RuntimeError(f"expected one receipt for batched load, got {len(unique)}")
    return unique[0]


def summarize_load(samples: list[dict]) -> dict:
    if not samples:
        return {
            "iterations": 0,
            "median_load_ms": 0.0,
            "median_gib_per_second": 0.0,
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
    evidence = summarize_receipt_evidence(samples)
    return {
        "iterations": len(samples),
        "median_load_ms": statistics.median(sample["load_ms"] for sample in samples),
        "median_gib_per_second": statistics.median(
            sample["load_gib_per_second"] for sample in samples
        ),
        "bytes": int(statistics.median(sample["bytes"] for sample in samples)),
        "bytes_completed": int(
            statistics.median(sample["bytes_completed"] for sample in samples)
        ),
        "direct_bytes": int(
            statistics.median(sample["direct_bytes"] for sample in samples)
        ),
        "relay_bytes": int(statistics.median(sample["relay_bytes"] for sample in samples)),
        "direct_chunks": int(
            statistics.median(sample["direct_chunks"] for sample in samples)
        ),
        "relay_chunks": int(
            statistics.median(sample["relay_chunks"] for sample in samples)
        ),
        "decision_ids": sorted({sample["decision_id"] for sample in samples}),
        "topology_snapshot_ids": sorted(
            {sample["topology_snapshot_id"] for sample in samples}
        ),
        "ticket_ids": sorted({sample["ticket_id"] for sample in samples}),
        "receipt_ids": sorted(
            {
                str(sample["receipt"].get("receipt_id", ""))
                for sample in samples
                if sample["receipt"].get("receipt_id")
            }
        ),
        "fallback_reasons": sorted(
            {sample["fallback_reason"] for sample in samples if sample["fallback_reason"]}
        ),
        **evidence,
    }


def summarize_receipt_evidence(samples: list[dict]) -> dict[str, object]:
    metadata = [receipt_metadata(sample) for sample in samples]
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
        "source_buffer_id": args.source_buffer_id,
        "destination_buffer_id": args.destination_buffer_id,
        "workload_kind": WorkloadKind.MODEL_WEIGHTS.value,
        "target_gpu": args.target_gpu,
        "bucket_count": int(args.bucket_count),
        "bucket_bytes": int(args.bucket_bytes),
        "storage_layout": args.storage_layout,
        "chunk_bytes": int(args.chunk_bytes),
        "warmup": int(args.warmup),
        "iterations": int(args.iterations),
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
def runtime_context(args, *, session_factory=None, buffer_factory=None, loader_factory=None):
    if session_factory is None:
        session_factory = ProductionRuntimeSessionFactory()
    if buffer_factory is None:
        buffer_factory = TorchRuntimeBufferFactory()
    trace_event("runtime_context_open_start")
    with session_factory.open(args) as session:
        trace_event("runtime_context_session_opened")
        trace_event("runtime_context_allocate_buffers_start")
        buffers = buffer_factory.allocate(args)
        trace_event("runtime_context_allocate_buffers_done")
        try:
            trace_event("runtime_context_register_cuda_start")
            session.register_cuda_buffer(buffers.gpu_buffer)
            trace_event("runtime_context_register_cuda_done")
            trace_event("runtime_context_open_session_start")
            args.session_id = session.open_session()
            trace_event("runtime_context_open_session_done", session_id=args.session_id)
            trace_event("runtime_context_register_cpu_start")
            session.register_cpu_buffer(buffers.cpu_buffer)
            trace_event("runtime_context_register_cpu_done")
            args.source_buffer_id = buffers.cpu_buffer.buffer_id
            args.destination_buffer_id = buffers.gpu_buffer.buffer_id
            trace_event("runtime_context_make_loader_start")
            if loader_factory is None:
                loader = make_loader(args, session, buffers)
            else:
                loader = loader_factory(args, session, buffers)
            trace_event("runtime_context_make_loader_done")
            yield RuntimeBenchmarkState(
                session=session,
                buffers=buffers,
                loader=loader,
            )
        finally:
            trace_event("runtime_context_release_buffers_start")
            buffers.release()
            trace_event("runtime_context_release_buffers_done")


def make_loader(args, session, buffers):
    from turbobus.adapters.model_loading import ModelWeightLoader

    loader = ModelWeightLoader(
        session,
        buffers.cpu_buffer,
        buffers.gpu_buffer,
        metadata={
            "benchmark": "model-loading",
            "policy": args.policy,
            "storage_layout": args.storage_layout,
            "bucket_count": int(args.bucket_count),
            "bucket_bytes": int(args.bucket_bytes),
            "chunk_bytes": int(args.chunk_bytes),
        },
        intent_prefix=f"model-load-{args.run_id}",
        wait_timeout_seconds=args.wait_timeout_seconds,
    )
    loader.add_packed_buckets(
        "bucket-",
        bucket_bytes=int(args.bucket_bytes),
        bucket_count=int(args.bucket_count),
    )
    return loader


class RuntimeBenchmarkState:
    def __init__(self, *, session, buffers, loader) -> None:
        self.session = session
        self.buffers = buffers
        self.loader = loader


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
        trace_event("torch_buffers_require_torch_start")
        torch = require_torch()
        trace_event("torch_buffers_require_torch_done")
        byte_count = total_bytes(args)
        run_id = str(args.run_id).replace("/", "-")
        cpu_buffer_id = args.source_buffer_id or f"model-load-cpu-{run_id}"
        gpu_buffer_id = args.destination_buffer_id or f"model-load-gpu-{run_id}"

        from turbobus.client import CudaIpcDeviceBuffer, SharedPinnedCpuBuffer

        cpu_buffer = None
        try:
            trace_event("torch_buffers_shared_cpu_allocate_start", bytes=byte_count)
            cpu_buffer = SharedPinnedCpuBuffer.allocate(
                buffer_id=cpu_buffer_id,
                job_id=args.job_id,
                size_bytes=byte_count,
                name_prefix="turbobus-model-load",
            )
            trace_event(
                "torch_buffers_shared_cpu_allocate_done",
                shm_name=cpu_buffer.shared_memory_name,
            )
            trace_event("torch_buffers_source_tensor_start", bytes=byte_count)
            source = torch.empty(byte_count, dtype=torch.uint8, pin_memory=True)
            source.random_(0, 256)
            trace_event("torch_buffers_source_tensor_done")
            trace_event("torch_buffers_cpu_write_start", bytes=byte_count)
            cpu_buffer.write(source.numpy().tobytes())
            trace_event("torch_buffers_cpu_write_done")

            trace_event("torch_buffers_set_device_start", target_gpu=args.target_gpu)
            torch.cuda.set_device(int(args.target_gpu))
            trace_event("torch_buffers_set_device_done", target_gpu=args.target_gpu)
            trace_event("torch_buffers_target_tensor_start", bytes=byte_count)
            target = torch.empty(
                byte_count,
                dtype=torch.uint8,
                device=f"cuda:{int(args.target_gpu)}",
            )
            trace_event("torch_buffers_target_tensor_done", ptr=target.data_ptr())
            trace_event("torch_buffers_cuda_ipc_export_start")
            gpu_buffer = CudaIpcDeviceBuffer.from_device_pointer(
                buffer_id=gpu_buffer_id,
                job_id=args.job_id,
                device_index=int(args.target_gpu),
                size_bytes=byte_count,
                device_ptr=target.data_ptr(),
            )
            trace_event("torch_buffers_cuda_ipc_export_done")
            return RuntimeBuffers(
                cpu_buffer=cpu_buffer,
                gpu_buffer=gpu_buffer,
                target_tensor=target,
            )
        except Exception:
            if cpu_buffer is not None:
                cpu_buffer.release()
            raise


class ProductionRuntimeSessionFactory:
    @contextlib.contextmanager
    def open(self, args):
        daemon_process = None
        worker_process = None
        tmpdir = None
        session = None
        try:
            tmpdir = tempfile.TemporaryDirectory(prefix="turbobus-model-load-")
            daemon_socket = args.daemon_socket_path or os.path.join(tmpdir.name, "daemon.sock")
            worker_socket = args.worker_socket_path or os.path.join(tmpdir.name, "worker.sock")
            args.daemon_socket_path = daemon_socket
            args.worker_socket_path = worker_socket

            if args.start_services:
                trace_event("production_start_daemon_start", socket=daemon_socket)
                daemon_process = start_daemon_process(args, daemon_socket)
                wait_for_socket(daemon_socket, daemon_process)
                trace_event("production_start_daemon_done", socket=daemon_socket)
                trace_event("production_start_worker_start", socket=worker_socket)
                worker_process = start_worker_process(args, daemon_socket, worker_socket)
                wait_for_socket(worker_socket, worker_process)
                trace_event("production_start_worker_done", socket=worker_socket)

            from turbobus.runtime_options import RuntimeOptions
            from turbobus.runtime_session import TurboBusRuntimeSession

            trace_event("production_open_runtime_session_start")
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
            trace_event("production_open_runtime_session_done")
            yield session
        finally:
            if session is not None:
                trace_event("production_close_runtime_session_start")
                session.close()
                trace_event("production_close_runtime_session_done")
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
        raise RuntimeError("model loading benchmark requires PyTorch") from exc
    if not torch.cuda.is_available():
        raise RuntimeError("model loading benchmark requires CUDA")
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
        "MODEL_LOAD_SUMMARY_BEGIN",
        (
            "model_load_config "
            f"session_id={config['session_id']} job_id={config['job_id']} "
            f"source_buffer_id={config['source_buffer_id']} "
            f"destination_buffer_id={config['destination_buffer_id']} "
            f"target_gpu={config['target_gpu']} "
            f"bucket_count={config['bucket_count']} "
            f"bucket_bytes={config['bucket_bytes']} "
            f"storage_layout={config['storage_layout']} "
            f"chunk_bytes={config['chunk_bytes']} "
            f"iterations={config['iterations']} policy={config['policy']} "
            f"daemon_socket_path={config['daemon_socket_path']}"
        ),
        (
            "model_load_summary "
            f"median_load_ms={summary['median_load_ms']:.3f} "
            f"median_gib_s={summary['median_gib_per_second']:.3f} "
            f"bytes={summary['bytes']} "
            f"bytes_completed={summary['bytes_completed']} "
            f"direct_bytes={summary['direct_bytes']} "
            f"relay_bytes={summary['relay_bytes']} "
            f"direct_chunks={summary['direct_chunks']} "
            f"relay_chunks={summary['relay_chunks']} "
            f"executed={summary['executed']} "
            f"verified={summary['verified']} "
            f"content_match={summary['content_match']}"
        ),
    ]
    for sample in result["samples"]:
        lines.append(
            "model_load_sample "
            f"iteration={sample['iteration']} "
            f"load_ms={sample['load_ms']:.3f} "
            f"gib_s={sample['load_gib_per_second']:.3f} "
            f"decision_id={sample['decision_id']} "
            f"topology_snapshot_id={sample['topology_snapshot_id']} "
            f"ticket_id={sample['ticket_id']}"
        )
        lines.append(
            receipt_trace_line(
                receipt_from_trace(sample["receipt"]),
                prefix="model_load_receipt",
            )
        )
    lines.append("MODEL_LOAD_SUMMARY_END")
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
        description="Load model-weight buckets through TurboBus runtime session"
    )
    parser.add_argument("--job-id", default=benchmark_job_id("model-loading"))
    parser.add_argument("--target-gpu", type=int, required=True)
    parser.add_argument("--daemon-socket-path")
    parser.add_argument("--worker-socket-path")
    parser.add_argument("--start-services", action="store_true")
    parser.add_argument("--allow-missing-fabric", action="store_true", default=True)
    parser.add_argument("--allow-missing-pcie", action="store_true")
    parser.add_argument("--min-relays", type=int, default=1)
    parser.add_argument("--max-sessions-per-relay", type=int, default=1)
    parser.add_argument("--profile-bytes", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--bucket-count", type=int, default=8)
    parser.add_argument("--bucket-bytes", type=int, default=32 * 1024 * 1024)
    parser.add_argument("--storage-layout", choices=["packed"], default="packed")
    parser.add_argument("--chunk-bytes", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--warmup", type=int, default=0)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--policy", default="runtime-session")
    parser.add_argument("--run-id", default=str(uuid.uuid4()))
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
    if args.warmup < 0:
        raise ValueError("--warmup must be non-negative")
    if args.iterations <= 0:
        raise ValueError("--iterations must be positive")
    if args.profile_bytes <= 0:
        raise ValueError("--profile-bytes must be positive")
    if args.daemon_max_inflight_chunks <= 0:
        raise ValueError("--daemon-max-inflight-chunks must be positive")
    args.session_id = None
    args.source_buffer_id = None
    args.destination_buffer_id = None


def main() -> None:
    faulthandler.enable(all_threads=True)
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
