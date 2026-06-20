from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

WORKLOADS = ("model-loading", "training-offload", "optimizer-offload")
STATE_OFFLOAD_WORKLOAD_KINDS = {
    "training-offload": "training_state",
    "optimizer-offload": "optimizer_state",
}
PHASE6_REPORT_SCHEMA = "phase6_unified_v1"
WORKLOAD_KIND_BY_WORKLOAD = {
    "model-loading": "model_weights",
    **STATE_OFFLOAD_WORKLOAD_KINDS,
}
REQUIRED_UNIFIED_METRIC_FIELDS = (
    "report_schema",
    "workload",
    "policy",
    "job_id",
    "session_id",
    "workload_kind",
    "cpu_buffer_id",
    "gpu_buffer_id",
    "receipt_ids",
    "decision_ids",
    "topology_snapshot_ids",
    "ticket_ids",
    "transfer_bytes",
    "bytes_completed",
    "direct_bytes",
    "relay_bytes",
    "direct_chunks",
    "relay_chunks",
    "transfer_ms",
    "performance_ms",
    "fallback_reason",
    "executed",
    "verified",
    "verified_bytes",
    "content_match",
    "correctness_status",
)


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value).split(",") if item.strip()]


def selected_workloads(value: str) -> list[str]:
    items = parse_csv(value)
    if not items or items == ["all"]:
        return list(WORKLOADS)
    unknown = [item for item in items if item not in WORKLOADS]
    if unknown:
        raise ValueError(f"unknown workloads: {unknown}")
    return items


def output_paths(output_dir: Path, workload: str) -> dict[str, Path]:
    safe = workload.replace("-", "_")
    return {
        "json": output_dir / f"{safe}.json",
        "summary": output_dir / f"{safe}_summary.txt",
    }


def state_offload_workload_kind(workload: str) -> str:
    try:
        return STATE_OFFLOAD_WORKLOAD_KINDS[workload]
    except KeyError as exc:
        raise ValueError(f"unsupported state offload workload: {workload}") from exc


def read_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def as_float(value, default: float = 0.0) -> float:
    try:
        if value in (None, "", "NA"):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(value, default: int = 0) -> int:
    try:
        if value in (None, "", "NA"):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def join_values(values) -> str:
    if values in (None, ""):
        return ""
    if isinstance(values, (list, tuple, set)):
        return ",".join(str(item) for item in values if item not in (None, ""))
    return str(values)


def join_metric_values(*values) -> str:
    joined: list[str] = []
    for value in values:
        text = join_values(value)
        if not text:
            continue
        joined.extend(item for item in str(text).split(",") if item)
    return ",".join(joined)


def fallback_reason_value(*values) -> str:
    return join_metric_values(*values) or "none"


def correctness_status(
    transfer_bytes: int,
    bytes_completed: int,
    *,
    executed: bool,
    verified: bool,
    verified_bytes: int,
    content_match: bool,
) -> str:
    if (
        int(transfer_bytes) == int(bytes_completed)
        and bool(executed)
        and bool(verified)
        and int(verified_bytes) == int(transfer_bytes)
        and bool(content_match)
    ):
        return "complete"
    return "incomplete"


def _gib_per_second_value(byte_count: int, elapsed_ms: float) -> float:
    if byte_count <= 0 or elapsed_ms <= 0:
        return 0.0
    return float(byte_count) / (1024.0**3) / (float(elapsed_ms) / 1000.0)


def collect_model_metrics(result: dict) -> list[dict[str, object]]:
    summary = result.get("summary", {}) or {}
    if not summary:
        return []
    config = result.get("config", {}) or {}
    transfer_bytes = as_int(summary.get("bytes"))
    bytes_completed = as_int(summary.get("bytes_completed"))
    verified_bytes = as_int(summary.get("verified_bytes"))
    executed = bool(summary.get("executed", False))
    verified = bool(summary.get("verified", False))
    content_match = bool(summary.get("content_match", False))
    transfer_ms = as_float(summary.get("median_load_ms"))
    return [
        {
            "report_schema": PHASE6_REPORT_SCHEMA,
            "workload": "model-loading",
            "policy": str(config.get("policy", "")),
            "job_id": str(config.get("job_id", "")),
            "session_id": str(config.get("session_id", "")),
            "cpu_buffer_id": str(config.get("source_buffer_id", "")),
            "gpu_buffer_id": str(config.get("destination_buffer_id", "")),
            "source_buffer_id": str(config.get("source_buffer_id", "")),
            "destination_buffer_id": str(config.get("destination_buffer_id", "")),
            "workload_kind": str(config.get("workload_kind", "")),
            "iterations": as_int(summary.get("iterations")),
            "ttft_proxy_ms": transfer_ms,
            "transfer_ms": transfer_ms,
            "performance_ms": transfer_ms,
            "throughput_gib_s": as_float(summary.get("median_gib_per_second")),
            "transfer_bytes": transfer_bytes,
            "bytes_completed": bytes_completed,
            "direct_bytes": as_int(summary.get("direct_bytes")),
            "relay_bytes": as_int(summary.get("relay_bytes")),
            "direct_chunks": as_int(summary.get("direct_chunks")),
            "relay_chunks": as_int(summary.get("relay_chunks")),
            "receipt_ids": join_values(summary.get("receipt_ids")),
            "decision_ids": join_values(summary.get("decision_ids")),
            "topology_snapshot_ids": join_values(summary.get("topology_snapshot_ids")),
            "ticket_ids": join_values(summary.get("ticket_ids")),
            "fallback_reason": fallback_reason_value(summary.get("fallback_reasons")),
            "executed": executed,
            "verified": verified,
            "verified_bytes": verified_bytes,
            "content_match": content_match,
            "verification_sources": join_values(summary.get("verification_sources")),
            "verification_methods": join_values(summary.get("verification_methods")),
            "correctness_status": correctness_status(
                transfer_bytes,
                bytes_completed,
                executed=executed,
                verified=verified,
                verified_bytes=verified_bytes,
                content_match=content_match,
            ),
        }
    ]


def transfer_side_summary(summary: dict, side: str) -> dict:
    return summary.get(side, {}) or {}


def collect_training_metrics(
    result: dict,
    *,
    workload: str = "training-offload",
) -> list[dict[str, object]]:
    summary = result.get("summary", {}) or {}
    if not summary:
        return []
    config = result.get("config", {}) or {}
    prefetch = transfer_side_summary(summary, "prefetch")
    offload = transfer_side_summary(summary, "offload")
    direct_bytes = as_int(prefetch.get("direct_bytes")) + as_int(offload.get("direct_bytes"))
    relay_bytes = as_int(prefetch.get("relay_bytes")) + as_int(offload.get("relay_bytes"))
    direct_chunks = as_int(prefetch.get("direct_chunks")) + as_int(offload.get("direct_chunks"))
    relay_chunks = as_int(prefetch.get("relay_chunks")) + as_int(offload.get("relay_chunks"))
    transfer_bytes = as_int(prefetch.get("bytes")) + as_int(offload.get("bytes"))
    bytes_completed = as_int(prefetch.get("bytes_completed")) + as_int(offload.get("bytes_completed"))
    verified_bytes = as_int(prefetch.get("verified_bytes")) + as_int(offload.get("verified_bytes"))
    executed = bool(prefetch.get("executed", False)) and bool(offload.get("executed", False))
    verified = bool(prefetch.get("verified", False)) and bool(offload.get("verified", False))
    content_match = bool(prefetch.get("content_match", False)) and bool(
        offload.get("content_match", False)
    )
    transfer_ms = as_float(summary.get("median_transfer_ms"))
    return [
        {
            "report_schema": PHASE6_REPORT_SCHEMA,
            "workload": workload,
            "policy": str(config.get("policy", "")),
            "job_id": str(config.get("job_id", "")),
            "session_id": str(config.get("session_id", "")),
            "cpu_buffer_id": str(config.get("cpu_buffer_id", "")),
            "gpu_buffer_id": str(config.get("gpu_buffer_id", "")),
            "source_buffer_id": str(config.get("cpu_buffer_id", "")),
            "destination_buffer_id": str(config.get("gpu_buffer_id", "")),
            "workload_kind": str(config.get("workload_kind", "")),
            "iterations": as_int(summary.get("iterations")),
            "iteration_ms": as_float(summary.get("median_iteration_ms")),
            "transfer_ms": transfer_ms,
            "performance_ms": transfer_ms,
            "compute_ms": as_float(summary.get("median_compute_ms")),
            "throughput_gib_s": as_float(summary.get("median_gib_per_second")),
            "transfer_bytes": transfer_bytes,
            "bytes_completed": bytes_completed,
            "direct_bytes": direct_bytes,
            "relay_bytes": relay_bytes,
            "direct_chunks": direct_chunks,
            "relay_chunks": relay_chunks,
            "receipt_ids": join_metric_values(
                prefetch.get("receipt_ids"),
                offload.get("receipt_ids"),
            ),
            "decision_ids": join_values(
                [*prefetch.get("decision_ids", ()), *offload.get("decision_ids", ())]
            ),
            "topology_snapshot_ids": join_values(
                [
                    *prefetch.get("topology_snapshot_ids", ()),
                    *offload.get("topology_snapshot_ids", ()),
                ]
            ),
            "ticket_ids": join_values(
                [*prefetch.get("ticket_ids", ()), *offload.get("ticket_ids", ())]
            ),
            "prefetch_decision_ids": join_values(prefetch.get("decision_ids")),
            "offload_decision_ids": join_values(offload.get("decision_ids")),
            "prefetch_receipt_ids": join_values(prefetch.get("receipt_ids")),
            "offload_receipt_ids": join_values(offload.get("receipt_ids")),
            "fallback_reason": fallback_reason_value(
                [*prefetch.get("fallback_reasons", ()), *offload.get("fallback_reasons", ())]
            ),
            "executed": executed,
            "verified": verified,
            "verified_bytes": verified_bytes,
            "content_match": content_match,
            "verification_sources": join_metric_values(
                prefetch.get("verification_sources"),
                offload.get("verification_sources"),
            ),
            "verification_methods": join_metric_values(
                prefetch.get("verification_methods"),
                offload.get("verification_methods"),
            ),
            "correctness_status": correctness_status(
                transfer_bytes,
                bytes_completed,
                executed=executed,
                verified=verified,
                verified_bytes=verified_bytes,
                content_match=content_match,
            ),
        }
    ]


def collect_workload_metrics(workload: str, paths: dict[str, Path]) -> tuple[object, list[dict]]:
    data = read_json(paths["json"], {})
    if workload == "model-loading":
        return data, collect_model_metrics(data)
    if workload == "training-offload":
        return data, collect_training_metrics(data, workload=workload)
    if workload == "optimizer-offload":
        return data, collect_training_metrics(data, workload=workload)
    raise ValueError(f"unsupported workload: {workload}")


def workload_validation_errors(data_path: Path, metrics: list[dict]) -> list[str]:
    errors = []
    if not data_path.exists():
        errors.append("missing_output_file")
    if not metrics:
        errors.append("missing_paper_metrics")
        return errors
    missing_trace = [
        metric["workload"]
        for metric in metrics
        if not metric.get("decision_ids")
        or not metric.get("topology_snapshot_ids")
        or not metric.get("ticket_ids")
    ]
    if missing_trace:
        errors.append("missing_daemon_trace")
    errors.extend(unified_report_validation_errors(metrics))
    return errors


def unified_report_validation_errors(metrics: list[dict]) -> list[str]:
    errors = []
    for metric in metrics:
        for field in REQUIRED_UNIFIED_METRIC_FIELDS:
            if field not in metric or metric.get(field) in (None, ""):
                errors.append(f"missing_{field}")
        if int(metric.get("bytes_completed", 0) or 0) != int(metric.get("transfer_bytes", 0) or 0):
            errors.append("bytes_not_fully_completed")
        if not bool(metric.get("executed", False)):
            errors.append("missing_execution_evidence")
        if not bool(metric.get("verified", False)):
            errors.append("missing_verification_evidence")
        if int(metric.get("verified_bytes", 0) or 0) != int(metric.get("transfer_bytes", 0) or 0):
            errors.append("verified_bytes_mismatch")
        if not bool(metric.get("content_match", False)):
            errors.append("missing_content_match")
        if metric.get("correctness_status") != "complete":
            errors.append("invalid_correctness_status")
    return sorted(set(errors), key=errors.index)


def phase6_workload_validation_errors(workload: str, data_path: Path, metrics: list[dict]) -> list[str]:
    errors = workload_validation_errors(data_path, metrics)
    if not metrics:
        return errors
    expected_kind = {
        "model-loading": "model_weights",
        **STATE_OFFLOAD_WORKLOAD_KINDS,
    }.get(workload)
    for metric in metrics:
        for field in ("job_id", "session_id", "workload_kind"):
            if not metric.get(field):
                errors.append(f"missing_{field}")
        if workload == "model-loading":
            for field in ("source_buffer_id", "destination_buffer_id"):
                if not metric.get(field):
                    errors.append(f"missing_{field}")
        if workload in STATE_OFFLOAD_WORKLOAD_KINDS:
            for field in ("cpu_buffer_id", "gpu_buffer_id"):
                if not metric.get(field):
                    errors.append(f"missing_{field}")
        if expected_kind is not None and metric.get("workload_kind") != expected_kind:
            errors.append(f"invalid_{workload.replace('-', '_')}_workload_kind")
    return sorted(set(errors), key=errors.index)


def workload_status(returncode: int, validation_errors: list[str]) -> str:
    if returncode != 0:
        return "failed"
    if "invalid_output" in validation_errors:
        return "invalid-output"
    if "missing_output_file" in validation_errors:
        return "missing-output"
    if validation_errors:
        return "missing-metrics"
    return "ok"


def workload_failed(status: str) -> bool:
    return status != "ok"


def metric_line(metric: dict) -> str:
    ordered = [
        "workload",
        "report_schema",
        "policy",
        "job_index",
        "job_id",
        "session_id",
        "workload_kind",
        "source_buffer_id",
        "destination_buffer_id",
        "cpu_buffer_id",
        "gpu_buffer_id",
        "iterations",
        "ttft_proxy_ms",
        "iteration_ms",
        "transfer_ms",
        "performance_ms",
        "compute_ms",
        "throughput_gib_s",
        "transfer_bytes",
        "bytes_completed",
        "direct_bytes",
        "relay_bytes",
        "direct_chunks",
        "relay_chunks",
        "receipt_ids",
        "decision_ids",
        "topology_snapshot_ids",
        "ticket_ids",
        "executed",
        "verified",
        "verified_bytes",
        "content_match",
        "verification_sources",
        "verification_methods",
        "prefetch_decision_ids",
        "prefetch_receipt_ids",
        "offload_decision_ids",
        "offload_receipt_ids",
        "save_decision_ids",
        "save_topology_snapshot_ids",
        "save_ticket_ids",
        "save_ms",
        "restore_ms",
        "save_layer_count",
        "save_layer_ranges",
        "restore_layers",
        "restore_ranges",
        "prompt_tokens",
        "shared_prefix",
        "fallback_reason",
        "correctness_status",
        "log_path",
    ]
    fields = ["paper_metric"]
    for name in ordered:
        value = metric.get(name)
        if value in (None, ""):
            continue
        if isinstance(value, float):
            fields.append(f"{name}={value:.3f}")
        else:
            fields.append(f"{name}={value}")
    return " ".join(fields)


def compact_summary(result: dict) -> str:
    config = result["config"]
    lines = [
        "PAPER_VALIDATION_SUMMARY_BEGIN",
        (
            "paper_validation_config "
            f"job_id={config['job_id']} "
            f"workloads={','.join(config['workloads'])} "
            f"policy={config['policy']} "
            f"report_source={config['report_source']} "
            f"output_dir={config['output_dir']}"
        ),
    ]
    for workload in result["workloads"]:
        errors = ",".join(workload.get("validation_errors", []))
        lines.append(
            "paper_workload "
            f"workload={workload['workload']} status={workload['status']} "
            f"returncode={workload['returncode']} summary={workload['summary_path']} "
            f"json={workload['data_path']} validation_errors={errors}"
        )
        for metric in workload["metrics"]:
            lines.append(metric_line(metric))
    lines.append("PAPER_VALIDATION_SUMMARY_END")
    return "\n".join(lines)


def write_json(path: Path, result: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text + "\n", encoding="utf-8")


def run_validation(args) -> dict:
    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = REPO_ROOT / output_dir

    workloads = selected_workloads(args.workloads)
    result = {
        "config": {
            "job_id": args.job_id,
            "workloads": workloads,
            "policy": args.policy,
            "run_id": args.run_id,
            "output_dir": str(output_dir),
            "report_source": "existing_production_benchmark_json",
            "runs_benchmarks": False,
            "fake_receipt": False,
            "synthetic_evidence": False,
            "dry_run": False,
        },
        "workloads": [],
    }

    for workload in workloads:
        paths = output_paths(output_dir, workload)
        data_path = paths["json"]
        validation_errors = []
        try:
            data, metrics = collect_workload_metrics(workload, paths)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            data = {}
            metrics = []
            validation_errors.append("invalid_output")
            validation_errors.append(type(exc).__name__)
        validation_errors.extend(phase6_workload_validation_errors(workload, data_path, metrics))
        status = workload_status(0, validation_errors)
        result["workloads"].append(
            {
                "workload": workload,
                "status": status,
                "returncode": 0,
                "command": None,
                "stdout": "",
                "stderr": "",
                "summary_path": str(paths["summary"]),
                "data_path": str(data_path),
                "validation_errors": validation_errors,
                "data": data,
                "metrics": metrics,
            }
        )
        if workload_failed(status) and not args.keep_going:
            break
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a TurboBus paper report from existing production benchmark "
            "evidence without running benchmarks"
        )
    )
    parser.add_argument(
        "--workloads",
        default="all",
        help="Comma-separated: all, model-loading, training-offload, optimizer-offload",
    )
    parser.add_argument("--job-id", default="paper-validation")
    parser.add_argument("--policy", default="daemon-default")
    parser.add_argument("--run-id", default="paper-validation")
    parser.add_argument("--keep-going", action="store_true")
    parser.add_argument("--output-dir", default="benchmarks/results/paper_validation")
    parser.add_argument("--json-output")
    parser.add_argument("--summary-output")
    parser.add_argument("--no-copy-summary", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = run_validation(args)
    summary = compact_summary(result)
    if args.json_output:
        output = Path(args.json_output)
        if not output.is_absolute():
            output = REPO_ROOT / output
        write_json(output, result)
        print("paper_validation json_output", output)
    if args.summary_output:
        output = Path(args.summary_output)
        if not output.is_absolute():
            output = REPO_ROOT / output
        write_text(output, summary)
        print("paper_validation summary_output", output)
    if not args.no_copy_summary:
        print(summary)
    if any(workload_failed(item["status"]) for item in result["workloads"]):
        sys.exit(1)


if __name__ == "__main__":
    main()
