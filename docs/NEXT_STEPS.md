# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G52 topology and fabric abstraction hardening.

G51 is complete: training-state offload now consumes TransferReceipt plus
daemon recovery evidence for H2D/D2H movement through `TurboBusRuntimeSession`
without exposing physical route choices. The current target is topology and
fabric abstraction hardening.

## Current Code Work

- `turbobus/topology*`: daemon-owned topology and fabric discovery objects.
- `turbobus/profiling/bootstrap.py`: topology/profile import into daemon
  scheduling inputs.
- `turbobus/scheduler/daemon.py`: fabric-aware direct, relay, and mixed pooled
  scheduling metadata.
- `turbobus/daemon/server.py`: production topology ownership and rejection of
  synthetic production topology.
- `docs/PROGRESS.md`: current completed state and deferred validation risk.

Round rules:

- Start each round with `git status`, then read `AGENTS.md`,
  `docs/TURBOBUS_ROADMAP.md`, `docs/NEXT_STEPS.md`, and `docs/PROGRESS.md`.
- Choose the single round target from this file first and `docs/PROGRESS.md`
  second.
- Finish one full system closure per round, not local bug-style fixes.
- Treat a round as complete only when the system gains one independently
  describable production capability on the current target path.
- Current stage only advances code functionality. Do not run functional
  validation, benchmark, example, paper validation, server validation, multi-GPU
  execution, new tests, mock gates, fake receipts, synthetic evidence, or
  dry-run deliverables.
- Update this file and `docs/PROGRESS.md` after each completed closure.
- Keep only active and next work here. Do not append completed history.

## Next Entry

Implement G52 as one complete production capability: topology and fabric
abstraction should carry daemon-discovered PCIe, NUMA, and scale-up fabric
capabilities into scheduler/profile metadata for direct, relay, and mixed pooled
plans without allowing synthetic production topology.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction code-function queue.

Remaining auto-advance target queue: G52, G53, G54.
