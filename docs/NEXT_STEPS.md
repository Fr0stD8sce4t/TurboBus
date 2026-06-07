# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full cross-job ownership and cleanup-isolation lifecycle in the
daemon and worker control path.

## Exit Criteria

- Cleanup for live, missing, and archived session/job/buffer/reservation targets
  stays bound to the owning peer or daemon-owned identity.
- Worker-issued cleanup and daemon-issued cleanup use the same ownership scope,
  so shared relay use does not widen cross-job cleanup authority.
- The closure stays in production daemon/worker/runtime code and does not add
  benchmark-owned, example-owned, or dry-run wrapper paths.

## Current Code Work

- `turbobus/daemon/server.py`
- `turbobus/worker/validation.py`
- `turbobus/worker/lifecycle.py`
- `turbobus/runtime_session.py`
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

Start at `daemon/server.py` around cleanup ownership checks for live and
retired targets, then move through `worker/validation.py` and
`worker/lifecycle.py` to close one real owner-bound cleanup path. Touch
`runtime_session.py` and `schema.py` only if the production cleanup contract
needs it.

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
