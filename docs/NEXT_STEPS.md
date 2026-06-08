# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G40 validation code entry recovery.

Validation code entries must be restored only as code paths that consume real
`TransferReceipt` and daemon/worker completion evidence from production
runtime flows. They must not run functional validation, create synthetic
evidence, add dry-run deliverables, or manufacture receipts.

## Current Code Work

- `turbobus/runtime/validation.py`: receipt and reproduction-evidence
  validators.
- validation-facing code entry files under `turbobus/`: production evidence
  consumers only.
- `turbobus/offload/lifecycle.py`: adapter lifecycle evidence derived from
  real receipts.

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

After G40 is complete, continue automatically to G41 as the only current
target.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction code-function queue.

Remaining auto-advance target queue:

- G40 validation code entry recovery.
- G41 benchmark code recovery.
- G42 paper report code recovery.
