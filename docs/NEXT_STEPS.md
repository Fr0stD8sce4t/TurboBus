# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close the remaining runtime-session production startup path so
`TurboBusRuntimeSession` is the single production authority for daemon/worker
socket startup, session registration, buffer registration, intent submission,
receipt consumption, and shutdown on the real execution path.

## Exit Criteria

- `TurboBusRuntimeSession` must own one end-to-end production startup and
  shutdown path for daemon socket, worker socket, session/job registration,
  buffer registration, transfer submission, receipt wait, and service teardown.
- No production-looking caller should need to preassemble daemon/worker control
  flow outside `TurboBusRuntimeSession`.
- The closure stays in runtime/daemon/worker production code and does not add
  benchmark-owned, example-owned, or dry-run wrapper paths.

## Current Code Work

- `turbobus/runtime_session.py`
- `turbobus/runtime/session_state.py`
- `turbobus/runtime/daemon_view.py`
- `turbobus/daemon/server.py`
- `turbobus/daemon/client.py`
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

Start at `runtime_session.py` around production open/close, attached vs owned
service paths, and session registration responsibilities, then move through
`runtime/session_state.py`, `runtime/daemon_view.py`, `daemon/client.py`,
`worker/process.py`, and `worker/socket_client.py` to close one real runtime
authority path.

After the current target closes, the next round should finish exactly one of
these:

- one full production system-body closure for the next remaining scheduler
  feedback or ownership-hardening gap.
- one full validation/evaluation preparation closure only if system-body
  implementation no longer blocks it.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
