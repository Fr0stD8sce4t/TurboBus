# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G5: close the daemon admission loop so transfer planning, relay admission,
delayed admission, worker authorization, terminal cleanup, and promoted work
all share one production admission state machine.

## Exit Criteria

- Daemon admission decisions are recorded as production state and are refreshed
  when relay leases, staging records, terminal transfers, or promoted delayed
  transfers change availability.
- Delayed transfers are promoted by daemon-owned admission state, not by
  application, benchmark, example, dry-run, fake, or synthetic paths.
- Worker authorization consumes the current admission state and exact daemon
  ticket contract before staging records are registered.
- Terminal completion, failure, cancellation, lease expiry, and cleanup update
  admission state and unblock eligible queued work.
- The closure stays in daemon/scheduler/runtime production code and preserves
  daemon-issued plans as the only production transfer-plan source.

## Current Code Work

- `turbobus/daemon/server.py`
- `turbobus/scheduler/daemon.py`
- `turbobus/scheduler/load_feedback.py`
- `turbobus/daemon/receipts.py`
- `turbobus/worker/lifecycle.py`
- `turbobus/worker/validation.py`

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

Start at `daemon/server.py` around `_admission_for_decision_locked`,
`_validate_transfer_admission_locked`, `_promote_delayed_transfers_locked`,
worker authorization, terminal cleanup, and lease expiry. Then follow only the
production admission state into scheduler feedback or worker validation where
needed.

After the current target closes in auto-advance mode, the next queued target is:

- G6 multi-tenant isolation hardening.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.

## Auto-Advance Policy

Auto-advance is enabled for the current goal run because the user explicitly
started TurboBus Auto-Advance Mode.

Remaining auto-advance target queue:

1. G5 daemon admission loop.
2. G6 multi-tenant isolation hardening.

In auto-advance mode:

- keep exactly one current active target at a time;
- after each completed target, rewrite this file and `docs/PROGRESS.md` so the
  next queued target becomes the only current active target;
- for each queued target, carry forward the same system contracts from
  `AGENTS.md` and the same no-benchmark/no-test/no-fake-evidence constraints
  from this file;
- continue only while the next queued target is still system-body work;
- stop when the queue is complete, external environment blocks the target, a
  real architecture choice needs user review, or continuing would require
  benchmark, example, paper-validation, server-validation, new test, fake
  receipt, synthetic evidence, dry-run, or replacement verification work.
