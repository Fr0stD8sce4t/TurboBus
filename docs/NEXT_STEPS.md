# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G54 production safety boundary enhancement.

G53 is complete: backend execution now exposes an exact-plan submission
interface for daemon-issued direct, relay, and mixed pooled plans. CUDA remains
the current production backend, while worker and direct execution no longer
submit native CUDA plans through separate route-specific branches. The current
target is production safety boundary enhancement.

## Current Code Work

- `turbobus/runtime_session.py`: single production runtime-session authority.
- `turbobus/intent_executor.py`: TransferIntent to daemon-issued execution path.
- `turbobus/direct_fallback.py`: direct-only exact-plan backend execution.
- `turbobus/worker/cuda_executor.py`: ticketed worker/backend execution.
- `turbobus/daemon/server.py`: daemon plan, ticket, status, and receipt boundary.
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

Implement G54 as one complete production capability: production-looking runtime
and backend execution paths should stay bounded by `TurboBusRuntimeSession`,
daemon-issued tickets, and exact daemon-issued plans so applications and
adapters cannot choose physical routes or bypass daemon scheduling.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction code-function queue.

Remaining auto-advance target queue: G54.
