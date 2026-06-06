# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full runtime-session-owned execution and cleanup path as the single
production entry.

## Exit Criteria

- One `TurboBusRuntimeSession` path owns startup, intent submission, execution
  wait, cleanup handoff, and final `TransferReceipt` use.
- Runtime session stays the only production-facing path for daemon-issued
  transfer execution.
- Runtime-session-owned execution does not leave production-looking duplicate
  paths outside the session boundary.

## Current Code Work

- `turbobus/runtime_session.py`
- `turbobus/daemon/server.py`
- `turbobus/intent_executor.py`
- `turbobus/worker/lifecycle.py`
- `turbobus/daemon/receipts.py`

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

Start at `runtime_session.py`, `daemon/server.py`, `intent_executor.py`, and
`worker/lifecycle.py`.

After the current target closes, the next round should finish exactly one of
these:

- one full scheduler/runtime load-feedback closure.
- one full cross-job isolation and ownership closure.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
