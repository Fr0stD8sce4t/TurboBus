# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full scheduler/runtime load-feedback path on top of the now tighter
runtime-session-owned production entry.

## Exit Criteria

- Scheduler decisions consume more real queued/running/active transfer state.
- Relay load, active execution, and completion ownership feed back into daemon
  scheduling state through one clearer path.
- Load accounting does not depend on benchmark-only or synthetic control paths.

## Current Code Work

- `turbobus/daemon/server.py`
- `turbobus/runtime_session.py`
- `turbobus/intent_executor.py`
- `turbobus/daemon/receipts.py`
- `turbobus/daemon/dispatch.py`

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

Start at `daemon/server.py`, `runtime_session.py`, `intent_executor.py`, and
`daemon/receipts.py`.

After the current target closes, the next round should finish exactly one of
these:

- one full cross-job isolation and ownership closure.
- one full framework adapter closure after the core system path is stable.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
