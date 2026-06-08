# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

System-body auto-advance queue complete through G18.

No new implementation target is active in the current queue. Benchmark, example,
paper validation, server validation, new tests, fake evidence, synthetic
evidence, and dry-run deliverables remain deferred.

## Current Code Work

- No active code work remains in the G14-G18 auto-advance queue.
- `turbobus/offload/lifecycle.py` owns shared framework-facing lifecycle
  evidence derived from daemon `TransferReceipt` objects.
- Framework adapters should keep using `TurboBusRuntimeSession`,
  `TransferIntent`, and daemon `TransferReceipt` paths without adapter-side
  route, relay, pool, target-GPU, or relay-GPU policy.

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

Stop auto-advance for the current system-body queue. A new queue should be
defined before starting validation, benchmark, example, paper-validation,
server-validation, or new-test work.

## Auto-Advance Policy

Auto-advance queue complete.

Remaining auto-advance target queue:

- None.
