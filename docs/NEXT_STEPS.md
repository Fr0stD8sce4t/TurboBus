# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full scheduler/runtime feedback lifecycle from real execution state
through daemon relay admission and path selection.

## Exit Criteria

- Scheduler decisions consume real queued, running, active, and terminal
  transfer state instead of stale or synthetic summaries.
- Relay admission, delayed promotion, and path selection react to worker/backend
  runtime feedback on the production path.
- The closure stays in daemon/runtime scheduling code and does not use
  benchmark-only or local substitute control loops.

## Current Code Work

- `turbobus/daemon/server.py`
- `turbobus/scheduler/`
- `turbobus/intent_executor.py`
- `turbobus/runtime/daemon_view.py`
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

Start at `daemon/server.py`, then follow runtime-state aggregation and
relay-admission inputs through `scheduler/`, `intent_executor.py`, and
`runtime/daemon_view.py`.

After the current target closes, the next round should finish exactly one of
these:

- one full cross-job isolation and ownership hardening closure.
- one full runtime-session-facing adapter expansion closure only if scheduler
  feedback no longer blocks the main system body.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
