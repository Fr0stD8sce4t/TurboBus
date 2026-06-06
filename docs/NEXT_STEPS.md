# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full scheduler/load-accounting path driven by real
queued/running/active transfer state.

## Exit Criteria

- Scheduler decisions consume real queued, running, active, and recent terminal
  transfer state instead of stale or partial bookkeeping.
- Runtime feedback from daemon-issued execution affects relay load, busy relay
  view, and admission/scheduling behavior on the same production path.
- The closure stays inside daemon/runtime scheduling ownership and does not
  shift route choice or load policy into adapters, benchmarks, or examples.

## Current Code Work

- `turbobus/daemon/server.py`
- `turbobus/scheduler/`
- `turbobus/scheduler/load_feedback.py`
- `turbobus/schema.py`
- `turbobus/runtime/daemon_view.py`

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

Start at `daemon/server.py`, then follow the live runtime-state and scheduling
path through `scheduler/`, `scheduler/load_feedback.py`, `schema.py`, and
`runtime/daemon_view.py`.

After the current target closes, the next round should finish exactly one of
these:

- one full cross-job isolation and ownership hardening closure on shared relay
  use.
- one full adapter expansion closure for another workload family only if the
  scheduler/runtime path no longer blocks the main system body.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
