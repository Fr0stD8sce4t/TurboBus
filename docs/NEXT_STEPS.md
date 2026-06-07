# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G9 CUDA IPC metadata and span validation.

CUDA IPC GPU buffer registrations must carry enough production metadata for the
worker to validate the opened allocation span before execution. The worker must
reject out-of-range device views before submitting daemon-issued plans.

## Current Code Work

- `turbobus/client.py`: CUDA IPC device buffer registration metadata.
- `turbobus/backends/cuda.py`: CUDA IPC mapping export/open details.
- `turbobus/worker/resources.py`: worker-side CUDA IPC open and span checks.
- `turbobus/daemon/server.py`: daemon buffer snapshot and ownership evidence if
  needed to carry CUDA IPC metadata through tickets and receipts.

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

After G9 is complete, advance to G10 worker asynchronous execution pool.

## Auto-Advance Policy

Auto-advance is active for the new system-body queue approved after G1 through
G6.

Remaining auto-advance target queue:

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
