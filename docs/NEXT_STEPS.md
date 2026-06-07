# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G4: close the dynamic feedback loop so daemon runtime state reflects real
queued, running, active, completed, failed, and worker/backend execution
signals before scheduler decisions are made.

## Exit Criteria

- Daemon runtime feedback uses live transfer status, worker lifecycle, active
  staging records, relay leases, completion source, and failure evidence from
  production state.
- Scheduler input receives one coherent runtime-state view before planning and
  no longer depends on stale or admission-only counters when current transfer
  pressure is available.
- Runtime feedback distinguishes queued, running, active, completed, failed,
  direct, relay, and mixed pooled work without benchmark-owned, example-owned,
  test-owned, dry-run, fake, or synthetic evidence.
- Worker/backend completion and failure updates change the daemon feedback used
  by later scheduling decisions.
- The closure stays in daemon/scheduler/runtime production code and preserves
  the daemon-issued plan and exact-ticket execution contract.

## Current Code Work

- `turbobus/daemon/server.py`
- `turbobus/scheduler/daemon.py`
- `turbobus/runtime_feedback.py`
- `turbobus/intent_executor.py`
- `turbobus/worker/lifecycle.py`
- `turbobus/worker/cuda_executor.py`

Round rules:

- Start each round with `git status`, then read `AGENTS.md`,
  `docs/TURBOBUS_ROADMAP.md`, `docs/NEXT_STEPS.md`, and `docs/PROGRESS.md`.
- Choose the single round target from this file first and `docs/PROGRESS.md`
  second.
- Finish one full system closure per round, not local bug-style fixes.
- Treat a round as complete only when the system gains one independently
  describable production capability on the current target path.
- Do not advance benchmark, example, paper-validation, server-validation, new
  test, dry-run, fake receipt, synthetic evidence, or replacement verification
  entry work during the current system-body pass.
- State assumptions when they matter, prefer the simplest correct change, and
  keep edits surgical to the active target.
- Update this file and `docs/PROGRESS.md` after each completed closure.
- Keep only active and next work here. Do not append completed history.

## Next Entry

Start at `daemon/server.py` around runtime state snapshot construction,
transfer status updates, worker lifecycle updates, completion evidence storage,
and scheduler invocation. Then follow the runtime-state payload into
`scheduler/daemon.py` and `runtime_feedback.py` only as needed to make the
feedback consumed by scheduling come from current production state.

After the current target closes in auto-advance mode, the next queued target is:

- G5 daemon admission loop.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.

## Auto-Advance Policy

Auto-advance is enabled for the current goal run because the user explicitly
started TurboBus Auto-Advance Mode.

Remaining auto-advance target queue:

1. G4 dynamic feedback loop.
2. G5 daemon admission loop.
3. G6 multi-tenant isolation hardening.

In auto-advance mode:

- keep exactly one current active target at a time;
- after each completed target, rewrite this file and `docs/PROGRESS.md` so the
  next queued target becomes the only current active target;
- for each queued target, carry forward the same system contracts from
  `AGENTS.md` and the same no-benchmark/no-test/no-fake-evidence constraints
  from this file;
- continue only while the next queued target is still system-body work;
- stop when the queue is complete, external environment blocks the target, a
  real architecture choice needs user review, or continuing would require
  benchmark, example, paper-validation, server-validation, new test, fake
  receipt, synthetic evidence, dry-run, or replacement verification work.
