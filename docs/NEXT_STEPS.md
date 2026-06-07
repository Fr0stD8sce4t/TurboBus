# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full `TurboBusRuntimeSession`-managed production startup and
shutdown lifecycle through `open_managed_production_socket()`.

## Exit Criteria

- `TurboBusRuntimeSession` becomes the owning production entry for managed
  daemon and worker startup, readiness, session open, and shutdown evidence.
- The managed socket path closes one real lifecycle from runtime startup
  through transfer-capable session use to owned service shutdown and cleanup.
- The closure stays in production runtime/daemon/worker code and does not add
  benchmark-owned, example-owned, or dry-run wrapper paths.

## Current Code Work

- `turbobus/runtime_session.py`
- `turbobus/daemon/startup.py`
- `turbobus/runtime/session_state.py`
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

Start at `runtime_session.py` around `open_managed_production_socket()`, then
move through `daemon/startup.py`, `runtime/session_state.py`,
`worker/process.py`, and `worker/socket_client.py` to close one real
runtime-owned managed startup and shutdown lifecycle.

After the current target closes, the next round should finish exactly one of
these:

- one full production system-body closure for the next remaining runtime,
  scheduler, worker, or execution path gap.
- one full validation/evaluation preparation closure only if system-body
  implementation no longer blocks it.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
