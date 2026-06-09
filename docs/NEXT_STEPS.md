# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G45 worker resident async execution.

G44 is complete: scheduler decisions now derive adaptive direct/relay/mixed
weights from daemon runtime telemetry and record the adaptive policy in
decision metadata. The current target is to strengthen resident worker async
execution for daemon-issued tickets.

## Current Code Work

- `turbobus/worker/lifecycle.py`: worker async execution pool, submit/wait,
  cleanup, and result reporting.
- `turbobus/worker/cuda_executor.py`: CUDA worker handle lifecycle and terminal
  state.
- `turbobus/worker/models.py`: worker request/result contracts.
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

Implement G45 as one complete production capability: worker execution should
keep daemon-issued ticket work in a resident async pool with clear submit,
wait, cancellation/failure cleanup, and receipt-facing evidence.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction code-function queue.

Remaining auto-advance target queue: G45, G46, G47, G48, G49, G50, G51, G52,
G53, G54.
