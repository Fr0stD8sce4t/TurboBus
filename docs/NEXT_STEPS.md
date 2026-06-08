# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G26 vLLM real lifecycle closure.

vLLM KV cache save and restore must run through `TurboBusRuntimeSession`,
submit only `TransferIntent`, consume only `TransferReceipt`, and record a
stable workload lifecycle that binds request ids, block ids, runtime buffers,
receipt ids, ticket ids, byte counts, and path split evidence without exposing
direct, relay, pool, target GPU, or relay GPU choice to the adapter.

## Current Code Work

- `turbobus/adapters/vllm_integration.py`: real vLLM KV cache observation,
  block-id mapping, CPU backing ownership, and runtime-session adapter binding.
- `turbobus/adapters/vllm_kv_connector.py`: prefix save/restore lifecycle
  records, receipt trace aggregation, and backing-pool cleanup evidence.
- `turbobus/adapters/vllm.py`: KV block/range conversion into runtime-owned
  transfer contexts.
- `turbobus/offload/lifecycle.py`: adapter lifecycle evidence derived from
  real `TransferReceipt` objects.
- `turbobus/runtime_session.py`: vLLM adapter construction through the single
  production runtime-session entry.

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

After G26 is complete, continue automatically to G27 as the only current target.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction system queue.

Remaining auto-advance target queue:

- G26 vLLM real lifecycle closure.
- G27 model loading real integration closure.
- G28 training offload real integration closure.
- G29 unified reproduction evidence model.
- G30 real-execution validation and evaluation entry recovery.
