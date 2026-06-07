# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full registered buffer lifetime lifecycle across runtime session,
daemon, worker resource binding, execution cleanup, and receipt retention.

## Exit Criteria

- Shared pinned CPU buffers and CUDA IPC GPU buffers stay on one production
  lifecycle from registration through worker/backend use, cleanup, and receipt
  evidence.
- Success, failure, and session-close paths release the same registered buffer
  state instead of splitting ownership between ad hoc local/runtime paths.
- The closure stays in production runtime/daemon/worker code and does not add
  benchmark-owned, example-owned, or dry-run wrapper paths.

## Current Code Work

- `turbobus/runtime_session.py`
- `turbobus/buffer_registration.py`
- `turbobus/worker/resources.py`
- `turbobus/daemon/server.py`
- `turbobus/worker/lifecycle.py`
- `turbobus/schema.py`

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

Start at `runtime_session.py` around registered buffer cleanup and session
close, then move through `buffer_registration.py`, `worker/resources.py`,
`daemon/server.py`, and `worker/lifecycle.py` to close one real registered
buffer lifetime path.

After the current target closes, the next round should finish exactly one of
these:

- one full production system-body closure for the next remaining runtime,
  worker, execution, or ownership-hardening gap.
- one full validation/evaluation preparation closure only if system-body
  implementation no longer blocks it.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
