# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G44 adaptive scheduling policy.

G43 is complete: the daemon now exposes a production runtime telemetry snapshot
for queued, admitted, delayed, running, active, recent-terminal, relay-load,
job, session, and worker-feedback state. The current target is to make the
scheduler consume that telemetry through a clearer adaptive policy model.

## Current Code Work

- `turbobus/scheduler/daemon.py`: scheduler cost and relay filtering policy.
- `turbobus/scheduler/load_feedback.py`: runtime load view derived from daemon
  telemetry.
- `turbobus/daemon/server.py`: daemon telemetry source passed into scheduling.
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

Implement G44 as one complete production capability: scheduler decisions should
use daemon runtime telemetry to adapt direct, relay, and mixed pooled path
weights without allowing applications or adapters to choose physical routes.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction code-function queue.

Remaining auto-advance target queue: G44, G45, G46, G47, G48, G49, G50, G51,
G52, G53, G54.
