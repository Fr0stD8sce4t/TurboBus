# TurboBus

TurboBus is being rebuilt as a paper-reproduction system for pooling idle PCIe
bandwidth in multi-GPU servers.

The target system is centered on:

- a privileged per-node daemon;
- daemon-owned topology discovery and profile bootstrap;
- session, job, and buffer registration through one runtime session API;
- daemon-issued direct, relay, and mixed pooled chunked transfers;
- worker/backend execution of exact `ExecutionTicket` plans;
- application isolation, relay leases, cleanup, and runtime load feedback;
- framework adapters for vLLM, model loading, and training offload.

## Repository Map

- `cpp/`: native CUDA transfer engine, profiler, planner structs, and pybind
  module.
- `turbobus/`: Python runtime session API, daemon control plane, worker data
  plane, scheduler, topology discovery, and framework adapters.
- `docs/`: active roadmap, current next step, progress, and design notes.
- `benchmarks/`: workload and evaluation scripts used after the system path is
  complete.
- `test/`: Python and native tests used after the active implementation pass
  reaches validation work.

## Current Direction

The full reproduction route is in `docs/TURBOBUS_ROADMAP.md`.
The active implementation target is in `docs/NEXT_STEPS.md`.

Current code work focuses on closing the real H2D / D2H execution path:
`TransferIntent` submission must drive daemon scheduling, daemon-issued
`ExecutionTicket` objects, direct/relay/mixed pooled worker or backend
execution, status updates, cleanup, and real `TransferReceipt` evidence.

Historical phase-by-phase and artifact-only plans are retired and must not be
used as current implementation guidance.
