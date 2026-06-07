# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close the remaining relay-lease ownership lifecycle across daemon lease
validation, worker cleanup scope, and retired reservation cleanup.

## Exit Criteria

- Relay lease validation and release stay bound to the owning session/job/buffer
  identities on the production path.
- Worker-issued reservation cleanup stays inside the daemon-issued lease scope,
  including retired reservation or staging cleanup after lease teardown.
- The closure stays in daemon/worker/control-plane ownership code and does not
  add compatibility shims or benchmark-owned policy.

## Current Code Work

- `turbobus/daemon/server.py`
- `turbobus/daemon/dispatch.py`
- `turbobus/worker/lifecycle.py`
- `turbobus/worker/validation.py`

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

Start at `daemon/server.py`, then follow relay lease ownership, reservation
cleanup authorization, and retired reservation validation through
`daemon/dispatch.py`, `worker/lifecycle.py`, and `worker/validation.py`.

After the current target closes, the next round should finish exactly one of
these:

- one full runtime-session-facing adapter expansion closure for another real
  workload family.
- one full validation/evaluation preparation closure only if system-body
  implementation no longer blocks it.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
