# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full cross-job isolation and ownership path for shared daemon-issued
transfer execution.

## Exit Criteria

- Shared relay use stays bound to the correct job, session, buffer, and peer
  ownership through submit, execute, cleanup, and receipt.
- Daemon and worker cleanup paths stop one job from consuming or retiring
  another job's transfer state.
- Isolation closure lives on the production path and does not depend on
  benchmark-only or synthetic control paths.

## Current Code Work

- `turbobus/daemon/server.py`
- `turbobus/daemon/dispatch.py`
- `turbobus/runtime_session.py`
- `turbobus/worker/lifecycle.py`
- `turbobus/worker/validation.py`

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

Start at `daemon/server.py`, `daemon/dispatch.py`, `runtime_session.py`, and
the worker ownership checks in `worker/lifecycle.py` and
`worker/validation.py`.

After the current target closes, the next round should finish exactly one of
these:

- one full framework adapter closure after the core system path is stable.
- one full server-backed validation closure after the system body is complete.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
