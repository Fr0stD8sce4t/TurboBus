# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G42 paper report code recovery.

Paper report code entries must be restored only as production evidence
consumers. They may consume benchmark summaries, real `TransferReceipt`
validation summaries, adapter lifecycle evidence, and daemon or worker
completion evidence, but must not run paper validation, create synthetic
evidence, add dry-run deliverables, manufacture receipts, or define core
architecture.

## Current Code Work

- paper-report-facing code entries under `benchmarks/`: production evidence
  consumers only.
- `turbobus/runtime/evidence.py`: shared validation entry for real receipts
  and adapter lifecycle evidence.
- `benchmarks/paper_validation.py`: report aggregation must consume existing
  production evidence only.

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

After G42 is complete, stop auto-advance.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction code-function queue.

Remaining auto-advance target queue:

- G42 paper report code recovery.
