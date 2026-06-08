# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G34 scheduler cost model strengthening.

Scheduler planning must use imported profile measurements, runtime pressure,
relay admission state, and workload priority as one explicit cost model for
direct, relay-only, and mixed pooled decisions without letting applications
select physical routes.

## Current Code Work

- `turbobus/scheduler/daemon.py`: cost model metadata, candidate path scoring,
  runtime pressure handling, and fallback reasons.
- `turbobus/planner_engine.py`: direct, relay, and mixed pooled path weighting
  consumed by daemon scheduler.
- `turbobus/planner_types.py`: planner path cost metadata carried into
  scheduling decisions.
- `turbobus/scheduler/load_feedback.py`: queued/running/active pressure used by
  scheduler cost calculations.

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

After G34 is complete, continue automatically to G35 as the only current
target.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction code-function queue.

Remaining auto-advance target queue:

- G34 scheduler cost model strengthening.
- G35 runtime feedback strengthening.
- G36 multi-tenant isolation strengthening.
- G37 vLLM KV code integration strengthening.
- G38 model loading code integration strengthening.
- G39 training offload code integration strengthening.
- G40 validation code entry recovery.
- G41 benchmark code recovery.
- G42 paper report code recovery.
