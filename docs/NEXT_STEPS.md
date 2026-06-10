# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Runtime option config propagation is complete for the current production pass.

`RuntimeOptions` now preserves worker runtime cache entries, terminal history
entries, and relay staging clearing policy across JSON/profile ingestion and
worker startup.

## Current Code Work

- `turbobus/runtime_options.py`: runtime control option ingestion and native
  propagation.
- `turbobus/worker/process.py`: worker startup CLI runtime option wiring.
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

No further code-function target is active in this queue. The remaining work is
deferred validation and evaluation around real execution evidence, not part of
the current no-validation code-function pass.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction code-function queue.

Remaining auto-advance target queue: none.
