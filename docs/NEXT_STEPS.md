# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G18 unified auditable receipt closure.

All framework-facing production adapters must expose auditable lifecycle state
that is derived from daemon `TransferReceipt` objects. The final closure must
make receipt evidence consistent across offload, model loading, training state,
and vLLM KV paths without adding benchmark, example, dry-run, fake receipt, or
synthetic validation entry points.

## Current Code Work

- `turbobus/offload/handles.py`: shared receipt handle validation and wait
  semantics.
- `turbobus/offload/store.py`: shared named-block TransferIntent submission,
  wait, block state, and receipt consumption path.
- `turbobus/adapters/model_loading.py`: model-weight lifecycle evidence.
- `turbobus/adapters/training_offload.py`: training-state lifecycle evidence.
- `turbobus/adapters/vllm_kv_connector.py`: vLLM KV lifecycle evidence.

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

After G18 is complete, stop auto-advance for the system-body queue and leave
benchmark, example, paper validation, server validation, and new tests deferred.

## Auto-Advance Policy

Auto-advance is active for the system-body queue.

Remaining auto-advance target queue:

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
