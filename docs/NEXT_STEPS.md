# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G29 unified reproduction evidence model.

The production receipt model must expose one stable evidence view for direct,
relay, mixed pooled execution, failure cleanup, buffer lifetime, CUDA IPC
lifecycle, and framework adapter lifecycles. Evidence must come from real
daemon/worker/backend `TransferReceipt` completion or explicit failure, not
from fake receipts, synthetic topology, JSON artifacts, or dry-run output.

## Current Code Work

- `turbobus/daemon/receipts.py`: unified `TransferReceipt` metadata,
  completion contract, buffer lifetime evidence, CUDA IPC lifecycle, and
  path-level evidence view.
- `turbobus/daemon/server.py`: completion evidence normalization, merge, archive,
  cleanup retention, and runtime feedback preservation.
- `turbobus/intent_executor.py`: worker/backend completion evidence propagation
  into receipts.
- `turbobus/offload/lifecycle.py`: adapter lifecycle evidence derived from real
  receipts and runtime buffer bindings.
- `turbobus/runtime/validation.py`: runtime receipt contract validation around
  real completion evidence.

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
  entry work during the current system-body pass.
- Update this file and `docs/PROGRESS.md` after each completed closure.
- Keep only active and next work here. Do not append completed history.

## Next Entry

After G29 is complete, continue automatically to G30 as the only current target.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction system queue.

Remaining auto-advance target queue:

- G29 unified reproduction evidence model.
- G30 real-execution validation and evaluation entry recovery.
