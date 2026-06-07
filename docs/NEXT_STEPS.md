# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full daemon/worker production startup lifecycle from
runtime-session-owned startup through authenticated execution and cleanup.

## Exit Criteria

- `TurboBusRuntimeSession` remains the only production startup entry for daemon
  and worker connectivity.
- Production startup binds daemon-issued authorization to real worker execution
  without synthetic topology or local substitute startup paths.
- Startup, authorization, execution, and cleanup failures return explicit
  daemon/worker evidence instead of hidden local fallback behavior.

## Current Code Work

- `turbobus/runtime_session.py`
- `turbobus/daemon/server.py`
- `turbobus/daemon/dispatch.py`
- `turbobus/worker/server.py`
- `turbobus/worker/lifecycle.py`
- `turbobus/native_runtime.py`

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

Start at `runtime_session.py`, then follow startup/bootstrap and worker routing
through `daemon/server.py`, `daemon/dispatch.py`, `worker/server.py`,
`worker/lifecycle.py`, and `native_runtime.py`.

After the current target closes, the next round should finish exactly one of
these:

- one full runtime-session-facing adapter expansion closure for another real
  workload family.
- one full scheduler/topology feedback closure only if startup no longer blocks
  the main system body.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
