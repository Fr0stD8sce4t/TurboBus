# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full live scheduler runtime-load-feedback lifecycle in the daemon
control path.

## Exit Criteria

- Scheduler admission and path selection consume live queued, running, and
  active transfer state from real daemon/runtime records instead of static or
  stale summaries.
- Runtime feedback from execution, completion source, and relay use stays on
  the production daemon path and becomes visible to later scheduling.
- The closure stays in production daemon/scheduler/runtime code and does not
  add benchmark-owned, example-owned, or dry-run wrapper paths.

## Current Code Work

- `turbobus/daemon/server.py`
- `turbobus/scheduler/load_feedback.py`
- `turbobus/runtime/daemon_view.py`
- `turbobus/worker/lifecycle.py`
- `turbobus/schema.py`

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

Start at `daemon/server.py` around transfer queue/runtime-state updates, then
move through `scheduler/load_feedback.py`, `runtime/daemon_view.py`,
`worker/lifecycle.py`, and `schema.py` to close one real live scheduler
feedback loop.

After the current target closes, the next round should finish exactly one of
these:

- one full production system-body closure for the next remaining runtime,
  worker, execution, or ownership-hardening gap.
- one full validation/evaluation preparation closure only if system-body
  implementation no longer blocks it.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
