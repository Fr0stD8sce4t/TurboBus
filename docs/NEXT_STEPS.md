# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full cross-job isolation and ownership hardening path on shared
relay use.

## Exit Criteria

- Job, session, buffer, lease, and cleanup ownership stay bound to the correct
  peer or daemon-owned identity during shared relay use.
- Shared relay execution and cleanup cannot drift into cross-job leakage when
  delayed admission, promotion, or terminal cleanup happen out of order.
- The closure stays on daemon/runtime/worker ownership boundaries and does not
  push isolation policy into adapters, benchmarks, or examples.

## Current Code Work

- `turbobus/daemon/server.py`
- `turbobus/daemon/peer_auth.py`
- `turbobus/worker/lifecycle.py`
- `turbobus/worker/validation.py`
- `turbobus/schema.py`
- `turbobus/runtime/daemon_view.py`

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

Start at `daemon/server.py`, then follow ownership and cleanup flow through
`daemon/peer_auth.py`, `worker/lifecycle.py`, `worker/validation.py`,
`schema.py`, and `runtime/daemon_view.py`.

After the current target closes, the next round should finish exactly one of
these:

- one full adapter expansion closure for another workload family only if the
  ownership path no longer blocks the main system body.
- one full native direct/relay/mixed data-path hardening closure only if
  ownership no longer blocks the main system body.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
