# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G10 worker asynchronous execution pool.

Worker execution must keep daemon-issued transfers in a long-lived asynchronous
pool instead of collapsing worker execution into one synchronous submit/wait
path. The pool must preserve ticket authority, resource cleanup, status
reporting, and receipt evidence for queued, running, completed, and failed
worker transfers.

## Current Code Work

- `turbobus/worker/lifecycle.py`: worker submit, running status, wait, cleanup,
  and daemon status reporting.
- `turbobus/worker/cuda_executor.py`: asynchronous CUDA worker handles and
  inflight/terminal transfer ownership.
- `turbobus/worker/models.py`: worker result and lifecycle state envelopes.
- `turbobus/daemon/server.py`: daemon transfer status and runtime feedback
  evidence for async worker state if needed.

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

After G10 is complete, advance to G11 scheduler cost-model upgrade.

## Auto-Advance Policy

Auto-advance is active for the new system-body queue approved after G1 through
G6.

Remaining auto-advance target queue:

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
