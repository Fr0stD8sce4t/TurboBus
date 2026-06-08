# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G17 training-state offload closure.

Training-state offload paths must register real CPU/GPU buffers through
`TurboBusRuntimeSession`, submit training-state `TransferIntent` work, and
consume daemon `TransferReceipt` objects without exposing direct, relay, pool,
target-GPU, or relay-GPU policy to adapter or application code.

## Current Code Work

- `turbobus/adapters/training_offload.py`: optimizer/checkpoint/state bucket
  registration, offload/restore submission, wait, and receipt lifecycle state.
- `turbobus/runtime_session.py`: production adapter construction and receipt
  consumption entry points for training-state workloads.
- `turbobus/offload/store.py`: shared named-block transfer path used by
  framework adapters.

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

After G17 is complete, advance to G18 unified auditable receipt closure.

## Auto-Advance Policy

Auto-advance is active for the system-body queue.

Remaining auto-advance target queue:

- G17 training-state offload closure.
- G18 unified auditable receipt closure.

In auto-advance mode:

- keep exactly one current active target at a time;
- after each completed target, rewrite this file and `docs/PROGRESS.md` so the
  next queued target becomes the only current active target;
- carry forward the no-benchmark, no-example, no-test, no-fake-evidence, and
  daemon-issued-plan constraints;
- stop when the queue is complete, external environment blocks the target, a
  real architecture choice needs user review, or continuing would leave the
  system-body scope.
