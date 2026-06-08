# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G30 real-execution validation and evaluation entry recovery.

Validation and evaluation entry points may return only around real
daemon/worker/backend `TransferReceipt` objects and their
`reproduction_evidence` view. They must reject fake receipts, synthetic
evidence, JSON-only artifacts, and dry-run output as reproduction proof.

## Current Code Work

- `turbobus/runtime/validation.py`: receipt-level reproduction evidence
  validation for real execution, path mode, completion, cleanup, buffer
  lifetime, and daemon-owned scheduling policy.
- `turbobus/daemon/receipts.py`: stable receipt evidence schema consumed by
  validation and later evaluation code.
- Existing validation or evaluation-facing modules may consume only real
  receipts from the production daemon/worker/backend path.

Round rules:

- Start each round with `git status`, then read `AGENTS.md`,
  `docs/TURBOBUS_ROADMAP.md`, `docs/NEXT_STEPS.md`, and `docs/PROGRESS.md`.
- Choose the single round target from this file first and `docs/PROGRESS.md`
  second.
- Finish one full system closure per round, not local bug-style fixes.
- Treat a round as complete only when the system gains one independently
  describable production capability on the current target path.
- Do not advance benchmark, example, paper-validation, server-validation, new
  test, dry-run, fake receipt, synthetic evidence, or replacement verification
  entry work before G30.
- During G30, validation and evaluation may consume only real executed
  daemon/worker/backend evidence.
- Update this file and `docs/PROGRESS.md` after each completed closure.
- Keep only active and next work here. Do not append completed history.

## Next Entry

After G30 is complete, stop if no further user-provided auto-advance target is
available.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction system queue.

Remaining auto-advance target queue:

- G30 real-execution validation and evaluation entry recovery.
