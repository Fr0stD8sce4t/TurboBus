# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Auto-advance queue G1 through G6 is complete. There is no active target in the
current auto-advance queue.

## Exit Criteria

- G1 long-lived asynchronous data-plane closure is present in worker production
  code.
- G2 mixed pooled worker/backend execution is present in production code.
- G3 unified scheduling-model closure is present across daemon-issued plans,
  direct-only fallback, worker/backend completion, and receipt evidence.
- G4 dynamic feedback-loop closure is present across daemon runtime snapshots
  and scheduler load feedback.
- G5 daemon admission-loop closure is present across production admission state,
  delayed promotion, worker authorization, terminal cleanup, lease expiry, and
  scheduler/runtime feedback.
- G6 multi-tenant isolation closure is present across daemon peer ownership,
  worker authorization, transfer status updates, cleanup retention, staging
  records, lease ownership, and archived receipt access.

## Current Code Work

- No active G1-G6 code target remains in the auto-advance queue.
- Future work must start from a new user-approved system target and must still
  respect the current no-benchmark/no-test/no-fake-evidence constraints unless
  the active plan explicitly moves into validation work.

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
- State assumptions when they matter, prefer the simplest correct change, and
  keep edits surgical to the active target.
- Update this file and `docs/PROGRESS.md` after each completed closure.
- Keep only active and next work here. Do not append completed history.

## Next Entry

Auto-advance for G1 through G6 is complete. Stop here unless the user provides a
new active system-body target.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.

## Auto-Advance Policy

Auto-advance for the current goal run has completed the requested G1-G6 queue.

Remaining auto-advance target queue:

- None.

In auto-advance mode:

- keep exactly one current active target at a time;
- after each completed target, rewrite this file and `docs/PROGRESS.md` so the
  next queued target becomes the only current active target;
- for each queued target, carry forward the same system contracts from
  `AGENTS.md` and the same no-benchmark/no-test/no-fake-evidence constraints
  from this file;
- stop when the queue is complete, external environment blocks the target, a
  real architecture choice needs user review, or continuing would require
  benchmark, example, paper-validation, server-validation, new test, fake
  receipt, synthetic evidence, dry-run, or replacement verification work.
