# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close the remaining scheduler/runtime feedback production path so scheduler
load accounting uses real queued/running/active transfer state, terminal
completion source, and runtime feedback from the daemon-issued execution path.

## Exit Criteria

- Scheduler-facing load and runtime feedback state must come from real
  daemon-issued queued/running/terminal transfer records, not partial local
  bookkeeping.
- Direct-only, relay-only, and mixed execution terminal feedback must feed one
  scheduler-visible runtime accounting contract.
- The closure stays in daemon/runtime/scheduler production code and does not
  add benchmark-owned, example-owned, or dry-run wrapper paths.

## Current Code Work

- `turbobus/daemon/server.py`
- `turbobus/scheduler.py`
- `turbobus/runtime/daemon_view.py`
- `turbobus/runtime_session.py`
- `turbobus/intent_executor.py`

Round rules:

- Start each round with `git status`, then read `AGENTS.md`,
  `docs/TURBOBUS_ROADMAP.md`, `docs/NEXT_STEPS.md`, and `docs/PROGRESS.md`.
- Choose the single round target from this file first and `docs/PROGRESS.md`
  second.
- Finish one full system closure per round, not local bug-style fixes.
- State assumptions when they matter, prefer the simplest correct change, and
  keep edits surgical to the active target.
- Update this file and `docs/PROGRESS.md` after each completed closure.
- Keep only active and next work here. Do not append completed history.

## Next Entry

Start at `daemon/server.py` around queued/running/terminal transfer records and
runtime feedback, then move through `scheduler.py`, `runtime/daemon_view.py`,
`runtime_session.py`, and `intent_executor.py` to close one real scheduler
accounting path.

After the current target closes, the next round should finish exactly one of
these:

- one full production system-body closure for the next remaining scheduler
  feedback or ownership-hardening gap.
- one full validation/evaluation preparation closure only if system-body
  implementation no longer blocks it.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
