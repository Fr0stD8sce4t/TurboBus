# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one cross-job ownership and isolation production path so authenticated
session/job/buffer ownership survives daemon-issued relay sharing, cleanup,
receipt retention, and terminal runtime-state retention.

## Exit Criteria

- Cleanup, archived receipt lookup, terminal feedback retention, and ownership
  validation must agree on the same authenticated owner contract.
- Shared relay use must not weaken which peer may query, complete, clean up, or
  retain transfer-owned state after live transfer records retire.
- The closure stays in daemon/runtime/worker production code and does not add
  benchmark-owned, example-owned, or dry-run wrapper paths.

## Current Code Work

- `turbobus/daemon/server.py`
- `turbobus/worker/validation.py`
- `turbobus/worker/lifecycle.py`
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

Start at `daemon/server.py` around archived transfer ownership, cleanup scope,
receipt access, and retained terminal feedback, then move through
`worker/validation.py`, `worker/lifecycle.py`, and `runtime_session.py` to
close one real authenticated ownership path.

After the current target closes, the next round should finish exactly one of
these:

- one full production system-body closure for the next remaining ownership or
  runtime-startup gap.
- one full validation/evaluation preparation closure only if system-body
  implementation no longer blocks it.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
