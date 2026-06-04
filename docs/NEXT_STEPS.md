# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

The next task is to inspect real workload closure so model-loading, training
offload, inference KV, vLLM KV, and lower-level vLLM paths keep using the
unified runtime session without application-side physical route control.

## Exit Criteria

- Workload adapters construct transfer work through `TurboBusRuntimeSession`
  and `OffloadStore` intent submission instead of daemon, worker, or backend
  shortcuts.
- Adapter state keys, handles, and events preserve daemon runtime session,
  job, intent, ticket, and receipt identity.
- Applications and adapters still submit only `TransferIntent` and consume
  `TransferReceipt`.
- No benchmark, paper-validation, experiment, server-validation, compatibility
  shim, or export layer code is added during this pass.

## Current Code Work

- Inspect `turbobus/offload_store.py`, `turbobus/adapters/model_loading.py`,
  `turbobus/adapters/training_offload.py`,
  `turbobus/adapters/inference.py`, `turbobus/adapters/vllm.py`,
  `turbobus/adapters/vllm_integration.py`, and
  `turbobus/adapters/vllm_kv_connector.py` for runtime-session handoff.
- Keep offload and vLLM adapter handoff owned by `TurboBusRuntimeSession`; vLLM
  connector prefix state uses the daemon runtime session id while preserving
  the connector engine id only as metadata.
- Keep the old `client_transfer.py`, `turbobus/worker/helper.py`,
  `turbobus/daemon/protocol.py`, and `turbobus/worker_managed.py` files
  deleted. Do not recreate compatibility export layers.
- Continue code implementation and refactoring without adding server test
  commands or using server validation as the current entry point.

## Next Entry

Continue the code implementation pass by inspecting real workload closure
through the unified runtime session. Keep the work focused on system code;
defer tests, benchmarks, paper-validation, experiments, and server validation
until the full system implementation pass is complete.
