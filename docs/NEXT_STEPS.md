# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full daemon-issued `TransferIntent -> SchedulingDecision ->
ExecutionTicket -> worker/backend execution -> TransferReceipt` lifecycle for
mixed direct + relay execution, with unified completion, failure, cleanup, and
runtime feedback evidence.

## Exit Criteria

- A daemon-issued mixed plan must execute all direct chunks and relay chunks on
  the real worker/backend path and return one valid `TransferReceipt`.
- Completion, failure, cleanup, and runtime feedback for mixed execution must
  agree on one daemon-issued transfer contract instead of splitting between
  partial paths.
- The closure stays in system-body daemon/runtime/worker code and does not add
  benchmark-owned, example-owned, or dry-run wrapper paths.

## Current Code Work

- `turbobus/intent_executor.py`
- `turbobus/direct_fallback.py`
- `turbobus/daemon/server.py`
- `turbobus/worker/lifecycle.py`
- `turbobus/worker/resources.py`
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

Start at `intent_executor.py` around mixed direct + relay execution and failure
paths, then move through `direct_fallback.py`, `daemon/server.py`,
`worker/lifecycle.py`, `worker/resources.py`, and `runtime_session.py` to
close one real mixed execution lifecycle.

After the current target closes, the next round should finish exactly one of
these:

- one full production system-body closure for the next remaining runtime
  authority, scheduler-feedback, or ownership-hardening gap.
- one full validation/evaluation preparation closure only if system-body
  implementation no longer blocks it.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
