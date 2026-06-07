# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close the production buffer-lifetime path for daemon-issued transfers so shared
pinned CPU buffers and CUDA IPC GPU buffers stay registered, opened, retained,
cleaned up, and released correctly across success, failure, and session close.

## Exit Criteria

- Runtime-owned registered CPU/GPU buffers must carry one production lifetime
  contract from session registration through daemon-issued execution and final
  cleanup.
- Buffer retention and release evidence must agree across daemon archive,
  worker resource cleanup, runtime session shutdown, and final receipt views.
- The closure stays in daemon/runtime/worker/buffer code and does not add
  benchmark-owned, example-owned, or dry-run wrapper paths.

## Current Code Work

- `turbobus/runtime_session.py`
- `turbobus/buffer_registration.py`
- `turbobus/daemon/server.py`
- `turbobus/worker/resources.py`
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

Start at `worker/resources.py` and `worker/lifecycle.py` around opened CPU/GPU
resource tracking and failure cleanup, then move through `daemon/server.py`,
`daemon/receipts.py`, `buffer_registration.py`, and `runtime_session.py` to
close one real registered-buffer lifetime path.

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
