# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

The next task is to tighten the unified runtime session handoff to upper
system components so offload and adapter paths keep using `TransferIntent` and
`TransferReceipt` without reopening physical route choices.

## Exit Criteria

- Offload and adapter-facing runtime entry points submit intent through
  `TurboBusRuntimeSession` and consume daemon receipts.
- No upper layer chooses direct, relay, pool, target GPU, or relay GPU outside
  daemon/session setup.
- Runtime session ownership of submitted intents and closed-session rejection
  remains enforced.
- No benchmark, paper-validation, experiment, server-validation, compatibility
  shim, or export layer code is added during this pass.

## Current Code Work

- Inspect `turbobus/offload_store.py`, `turbobus/adapters/vllm_kv_connector.py`,
  lower-level vLLM adapter code, and `turbobus/runtime_session.py` for any
  remaining application-side physical route control or daemon-bypass transfer
  construction.
- Keep daemon ticket, receipt, and cleanup state consistent: worker lease
  release keeps completed receipts available, while session/job/buffer cleanup
  retires the full transfer record.
- Keep the old `client_transfer.py`, `turbobus/worker/helper.py`,
  `turbobus/daemon/protocol.py`, and `turbobus/worker_managed.py` files
  deleted. Do not recreate compatibility export layers.
- Continue code implementation and refactoring without adding server test
  commands or using server validation as the current entry point.

## Next Entry

Continue the code implementation pass by inspecting offload and adapter runtime
session handoff. Keep the work focused on system code; defer tests, benchmarks,
paper-validation, experiments, and server validation until the full system
implementation pass is complete.
