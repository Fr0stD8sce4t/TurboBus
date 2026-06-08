# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G15 prefix store productionization.

Prefix storage must become a production lifecycle boundary for framework KV
state. Saved prefixes need ownership, capacity, eviction, cleanup, and receipt
evidence handling that remains bound to `TurboBusRuntimeSession` and does not
expose route, relay, pool, direct, or target-GPU policy to adapters or
applications.

## Current Code Work

- `turbobus/adapters/vllm_prefix_store.py`: prefix ownership, lookup, eviction,
  lifecycle evidence, and cleanup semantics.
- `turbobus/adapters/vllm_kv_connector.py`: connector save/load path that stores
  and consumes prefix lifecycle state through runtime-session-backed transfers.
- `turbobus/adapters/vllm_backing_pool.py`: CPU backing reuse and release path
  for saved or evicted prefixes.

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
- Update this file and `docs/PROGRESS.md` after each completed closure.
- Keep only active and next work here. Do not append completed history.

## Next Entry

After G15 is complete, advance to G16 model loading takeover.

## Auto-Advance Policy

Auto-advance is active for the system-body queue.

Remaining auto-advance target queue:

- G15 prefix store productionization.
- G16 model loading takeover.
- G17 training-state offload closure.
- G18 unified auditable receipt closure.

In auto-advance mode:

- keep exactly one current active target at a time;
- after each completed target, rewrite this file and `docs/PROGRESS.md` so the
  next queued target becomes the only current active target;
- carry forward the no-benchmark, no-example, no-test, no-fake-evidence, and
  daemon-issued-plan constraints;
- stop when the queue is complete, external environment blocks the target, a
  real architecture choice needs user review, or continuing would leave the
  system-body scope.
