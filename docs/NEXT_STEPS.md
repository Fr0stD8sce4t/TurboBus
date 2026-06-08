# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G14 vLLM KV lifecycle closure.

vLLM KV save and restore must register real KV CPU/GPU buffers through
`TurboBusRuntimeSession`, submit KV-cache `TransferIntent` objects, and consume
daemon `TransferReceipt` objects without exposing route, relay, pool, direct, or
target-GPU policy to the adapter or application.

## Current Code Work

- `turbobus/adapters/vllm.py`: vLLM KV block and range lifecycle through
  runtime-session-backed KV slot adapters.
- `turbobus/adapters/vllm_integration.py`: vLLM runner/cache binding, CPU
  backing allocation, restore/save lifecycle, and receipt consumption.
- `turbobus/adapters/vllm_kv_connector.py`: vLLM connector entry point,
  request lifecycle, prefix save/load flow, and runtime-session ownership.
- `turbobus/offload/store.py`: shared offload block registration and
  TransferIntent submission path used by vLLM KV adapters.

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

After G14 is complete, advance to G15 prefix store productionization.

## Auto-Advance Policy

Auto-advance is active for the system-body queue.

Remaining auto-advance target queue:

- G14 vLLM KV lifecycle closure.
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
