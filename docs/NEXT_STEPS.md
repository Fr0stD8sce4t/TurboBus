# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full server/runtime production-startup hardening path on the single
`TurboBusRuntimeSession` production entry.

## Exit Criteria

- Production startup converges on `TurboBusRuntimeSession` for daemon socket
  client, worker client, and runtime-owned bootstrap instead of scattered
  production-looking entry points.
- The runtime path opens the production control plane with the identities,
  sockets, and startup state needed for real intent submission and ticketed
  execution.
- Startup hardening does not reintroduce old runtime/planner compatibility
  APIs, manual relay control, or synthetic production fallbacks.

## Current Code Work

- `turbobus/runtime_session.py`
- `turbobus/daemon/server.py`
- `turbobus/daemon/dispatch.py`
- `turbobus/worker/lifecycle.py`
- `turbobus/native_runtime.py`
- `turbobus/profiling/bootstrap.py`

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

Start at `runtime_session.py`, then follow the production startup path through
`daemon/server.py`, `daemon/dispatch.py`, `worker/lifecycle.py`,
`native_runtime.py`, and `profiling/bootstrap.py`.

After the current target closes, the next round should finish exactly one of
these:

- one full scheduler/load-accounting closure driven by real
  queued/running/active transfer state.
- one full adapter expansion closure for another workload family only if the
  startup path no longer blocks the main system body.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
