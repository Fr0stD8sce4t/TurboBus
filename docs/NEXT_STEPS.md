# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G8 buffer ownership lifecycle closure.

The daemon must own a production-visible buffer ownership ledger that binds
registered buffers to job/session ownership and active transfer leases. Active
daemon-issued tickets must prevent premature buffer cleanup, and terminal
cleanup must leave receipt-visible ownership evidence.

## Current Code Work

- `turbobus/daemon/server.py`: buffer registration, transfer planning, lease
  ownership, cleanup rejection/retention, receipt archive.
- `turbobus/schema.py`: shared ownership evidence fields only if the existing
  schema cannot carry the ledger through metadata.
- `turbobus/runtime_session.py`: runtime buffer registration and cleanup
  consumption without adding application-side route or relay control.
- `turbobus/worker/resources.py`: worker-side resource evidence only if needed
  to close the daemon ownership ledger.

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

After G8 is complete, advance to G9 CUDA IPC metadata and span validation.

## Auto-Advance Policy

Auto-advance is active for the new system-body queue approved after G1 through
G6.

Remaining auto-advance target queue:

- G8 buffer ownership lifecycle closure.
- G9 CUDA IPC metadata and span validation.
- G10 worker asynchronous execution pool.
- G11 scheduler cost-model upgrade.
- G12 admission priority queue.
- G13 runtime feedback metrics closure.
- G14 vLLM KV lifecycle closure.
- G15 prefix store productionization.
- G16 model loading takeover.
- G17 training-state offload closure.
- G18 unified auditable receipt closure.

In auto-advance mode:

- keep exactly one current active target at a time;
- after each completed target, rewrite this file and `docs/PROGRESS.md` so the
  next queued target becomes the only current active target;
- for each queued target, carry forward the same system contracts from
  `AGENTS.md` and the same no-benchmark/no-test/no-fake-evidence constraints
  from this file;
- stop when the queue is complete, external environment blocks the target, a
  real architecture choice needs user review, or continuing would require
  benchmark, example, paper-validation, server-validation, new test, fake
  receipt, synthetic evidence, dry-run, or replacement verification work.
