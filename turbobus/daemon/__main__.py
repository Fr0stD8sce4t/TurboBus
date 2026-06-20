from __future__ import annotations

import argparse

from .startup import DaemonStartupConfig, DaemonStartupError, create_production_daemon


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TurboBus daemon socket service")
    parser.add_argument("--topology-provider", default="cuda-nvml")
    parser.add_argument("--target-gpu", type=int, default=None)
    parser.add_argument("--min-relays", type=int, default=1)
    parser.add_argument(
        "--allow-missing-fabric",
        action="store_true",
        help="Start even when the production provider cannot report GPU fabric links.",
    )
    parser.add_argument(
        "--allow-missing-pcie",
        action="store_true",
        help="Start even when the production provider cannot report PCIe paths.",
    )
    parser.add_argument("--max-sessions-per-relay", type=int, default=1)
    parser.add_argument("--max-inflight-chunks-per-relay", type=int, default=8)
    parser.add_argument("--min-pool-bytes", type=int, default=12 * 1024 * 1024)
    parser.add_argument("--min-chunks-for-relay", type=int, default=2)
    parser.add_argument("--relay-min-effective-bw-gbps", type=float, default=0.0)
    parser.add_argument("--relay-min-direct-ratio", type=float, default=0.0)
    parser.add_argument("--session-timeout-seconds", type=float, default=0.0)
    parser.add_argument("--profile-max-age-seconds", type=float, default=0.0)
    parser.add_argument("--require-root", action="store_true")
    parser.add_argument("--socket-group", default=None)
    parser.add_argument("--socket-mode", default="0600")
    parser.add_argument("--max-sessions-per-uid", type=int, default=16)
    parser.add_argument("--max-jobs-per-uid", type=int, default=64)
    parser.add_argument("--max-buffers-per-uid", type=int, default=4096)
    parser.add_argument("--max-buffer-bytes-per-uid", type=int, default=0)
    parser.add_argument(
        "--socket-path",
        required=True,
        help="Unix socket path for the daemon control plane",
    )
    return parser


def startup_config_from_args(args) -> DaemonStartupConfig:
    return DaemonStartupConfig(
        topology_provider=args.topology_provider,
        target_gpu=args.target_gpu,
        min_relay_count=args.min_relays,
        require_fabric=not args.allow_missing_fabric,
        require_pcie=not args.allow_missing_pcie,
        max_sessions_per_relay=args.max_sessions_per_relay,
        max_inflight_chunks_per_relay=args.max_inflight_chunks_per_relay,
        min_pool_bytes=args.min_pool_bytes,
        min_chunks_for_relay=args.min_chunks_for_relay,
        relay_min_effective_bw_gbps=args.relay_min_effective_bw_gbps,
        relay_min_direct_ratio=args.relay_min_direct_ratio,
        session_timeout_seconds=args.session_timeout_seconds,
        profile_max_age_seconds=args.profile_max_age_seconds,
        require_root=args.require_root,
        socket_group=args.socket_group,
        socket_mode=args.socket_mode,
        max_sessions_per_uid=args.max_sessions_per_uid,
        max_jobs_per_uid=args.max_jobs_per_uid,
        max_buffers_per_uid=args.max_buffers_per_uid,
        max_buffer_bytes_per_uid=args.max_buffer_bytes_per_uid,
    )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        daemon = create_production_daemon(startup_config_from_args(args))
    except DaemonStartupError as exc:
        parser.exit(2, f"turbobus daemon startup failed: {exc}\n")
    try:
        daemon.serve_forever(args.socket_path)
    except RuntimeError as exc:
        parser.exit(2, f"turbobus daemon startup failed: {exc}\n")


if __name__ == "__main__":
    main()
