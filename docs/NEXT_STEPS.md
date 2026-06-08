# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G41 benchmark code recovery.

Benchmark code entries must be restored only as production evidence consumers.
They may consume real `TransferReceipt`, adapter lifecycle evidence, and daemon
or worker completion evidence, but must not run benchmarks, create synthetic
evidence, add dry-run deliverables, manufacture receipts, or define core
architecture.

## Current Code Work

- benchmark-facing code entries under `benchmarks/`: production evidence
  consumers only.
- `turbobus/runtime/evidence.py`: shared validation entry for real receipts
  and adapter lifecycle evidence.
- `turbobus/api.py` and package exports: public receipt-consumption boundary.

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

After G41 is complete, continue automatically to G42 as the only current
target.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction code-function queue.

Remaining auto-advance target queue:

- G41 benchmark code recovery.
- G42 paper report code recovery.
