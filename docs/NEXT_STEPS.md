# TurboBus Next Steps

This is the only active per-round forward plan. Keep it short and replace
completed state instead of appending history.

## Current Main Target

Real H2D / D2H execution path closure before experiments.

Current code target: framework adapter closure through `TurboBusRuntimeSession`.
Mixed direct-plus-relay completion evidence, buffer lifecycle evidence,
production worker startup evidence, and scheduler load feedback are now
preserved through backend/worker completion, daemon receipts, runtime feedback,
and runtime-session cleanup.

## Exit Criteria

- Buffer registration and cleanup keep shared pinned CPU and CUDA IPC GPU
  ownership scoped to the session, job, and transfer.
- Receipt metadata and runtime feedback preserve buffer open, close, cleanup,
  and release evidence from real worker/backend completion or explicit failure.
- Offload, inference, model-loading, training, and vLLM adapters submit H2D/D2H
  `TransferIntent` through `TurboBusRuntimeSession` and consume
  `TransferReceipt`.
- Offload, inference, model-loading, training, and vLLM adapters remain on
  `TurboBusRuntimeSession` and do not receive direct/relay/pool/target/relay
  policy controls.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

Focus on the production transfer boundary:

- `turbobus/daemon/server.py`
- `turbobus/intent_executor.py`
- `turbobus/runtime_session.py`
- `turbobus/adapters/`
- `turbobus/offload/`

The main implementation gap is now adapter closure: framework-facing code
should register real buffers through `TurboBusRuntimeSession`, submit only
transfer intent, and consume receipts without seeing route, relay, target, or
pool controls.

## Next Entry

Start at offload and vLLM adapter paths that still bypass or wrap around
`TurboBusRuntimeSession`. Do not add server-validation commands, benchmark
adapters, or application-side route controls.
