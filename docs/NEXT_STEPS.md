# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full registered buffer lifecycle from production registration through
execution, cleanup, and receipt.

## Exit Criteria

- Shared pinned CPU buffers and CUDA IPC GPU buffers stay bound to one daemon
  plan from registration/open through cleanup and receipt evidence.
- Success, failure, and cleanup do not leave buffer-open / buffer-close state
  implicit or adapter-owned.
- The closure stays on runtime/daemon/worker/data-plane production boundaries
  and does not introduce local substitute entrypoints.

## Current Code Work

- `turbobus/runtime_session.py`
- `turbobus/buffer_registration.py`
- `turbobus/client.py`
- `turbobus/worker/resources.py`
- `turbobus/worker/lifecycle.py`
- `turbobus/daemon/server.py`

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

Start at `runtime_session.py`, then follow buffer registration and resource
opening through `buffer_registration.py`, `client.py`, `worker/resources.py`,
`worker/lifecycle.py`, and `daemon/server.py`.

After the current target closes, the next round should finish exactly one of
these:

- one full daemon/worker production startup hardening closure only if buffer
  lifetime no longer blocks the main system body.
- one full runtime-session-facing adapter expansion closure for another real
  workload family only if buffer lifetime no longer blocks the main system
  body.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
