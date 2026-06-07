# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full `TurboBusRuntimeSession` production startup and execution
authority path across runtime-managed daemon/worker sockets, session/job
registration, buffer registration, intent submission, and receipt consumption.

## Exit Criteria

- `TurboBusRuntimeSession` is the clear production entry for managed
  daemon/worker startup, runtime control connection, session/job registration,
  buffer registration, transfer submission, and receipt consumption.
- Production-looking alternate startup or execution paths no longer carry the
  same end-to-end responsibility outside the runtime session boundary.
- The closure stays in system-body runtime/daemon/worker code and does not add
  benchmark-owned, example-owned, or dry-run wrapper paths.

## Current Code Work

- `turbobus/runtime_session.py`
- `turbobus/runtime/session_state.py`
- `turbobus/daemon/client.py`
- `turbobus/daemon/startup.py`
- `turbobus/daemon/server.py`
- `turbobus/worker/process.py`
- `turbobus/worker/socket_client.py`

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

Start at `runtime_session.py` around managed production socket startup,
runtime-owned clients, and close semantics, then move through
`runtime/session_state.py`, `daemon/client.py`, `daemon/startup.py`,
`worker/process.py`, and `worker/socket_client.py` to close one real runtime
session authority path.

After the current target closes, the next round should finish exactly one of
these:

- one full production system-body closure for the next remaining execution,
  scheduler-feedback, or ownership-hardening gap.
- one full validation/evaluation preparation closure only if system-body
  implementation no longer blocks it.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
