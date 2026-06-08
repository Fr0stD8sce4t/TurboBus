# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G30 real-execution validation and evaluation entry recovery is complete.

The active G19-G30 auto-advance queue has no remaining target. Do not start a
new implementation target until the next user-provided queue or current target
is supplied.

## Current Code Work

- `turbobus/runtime/validation.py`: production validation entry consumes only
  terminal `TransferReceipt` objects, verifies their real daemon/worker/backend
  `reproduction_evidence`, and returns a normalized
  `turbobus.real_execution_validation.v1` view for later evaluation.
- `turbobus/api.py`: terminal receipt consumption now rejects receipts that do
  not pass the real-execution evidence gate.

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

Stop. No further user-provided auto-advance target is available in this queue.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction system queue.

Remaining auto-advance target queue: none.
