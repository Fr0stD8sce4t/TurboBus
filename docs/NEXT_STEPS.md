# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full relay-only execution path as a daemon-owned production closure.

## Exit Criteria

- One relay-only `TransferIntent` runs through daemon scheduling, worker
  execution, cleanup, and final `TransferReceipt`.
- Relay-only success and failure both report daemon-owned terminal evidence.
- Relay-only completion uses the same receipt contract shape as direct and
  mixed execution.

## Current Code Work

- `turbobus/intent_executor.py`
- `turbobus/worker/lifecycle.py`
- `turbobus/daemon/server.py`
- `turbobus/daemon/receipts.py`
- `turbobus/runtime_session.py`

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

Start at `intent_executor.py`, `worker/lifecycle.py`, `daemon/server.py`, and
`daemon/receipts.py`.

After the current target closes, the next round should finish exactly one of
these:

- one full runtime-session-owned execution and cleanup closure;
- one full scheduler/runtime load-feedback closure.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
