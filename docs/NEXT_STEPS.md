# TurboBus Next Steps

This is the only active per-round forward plan. Keep it short and replace
completed state instead of appending history.

## Current Main Target

Real H2D / D2H execution path closure before experiments.

Current code target: production worker/socket closure for daemon-issued mixed
pooled transfer execution. The in-process runtime path now splits direct and
relay assignments from one daemon plan, executes direct chunks through backend
exact-plan code, executes relay chunks through worker authorization and cleanup,
and reports one merged daemon completion.

## Exit Criteria

- Worker socket execution can use the same deferred-terminal mixed pooled path
  as the in-process `WorkerTransferClient`.
- Daemon terminal status and receipt metadata include merged real completion
  or explicit failure evidence for every planned byte.
- Runtime feedback observes queued/running/active direct and relay paths from
  daemon state, not static plan output alone.
- Buffer registration and cleanup keep shared pinned CPU and CUDA IPC GPU
  ownership scoped to the session, job, and transfer.
- Offload, inference, model-loading, training, and vLLM adapters remain on
  `TurboBusRuntimeSession` and do not receive direct/relay/pool/target/relay
  policy controls.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

Focus on the production transfer boundary:

- `turbobus/intent_executor.py`
- `turbobus/direct_fallback.py`
- `turbobus/daemon/server.py`
- `turbobus/worker/lifecycle.py`
- `turbobus/worker/cuda_executor.py`
- `turbobus/runtime_session.py`
- `cpp/src/executor_cuda.cu`

The main implementation gap is now the production worker socket boundary:
socket worker requests still need a deferred-terminal mode so relay workers can
return relay completion and cleanup evidence without independently completing
the whole transfer before `WorkerIntentTransferExecutor` merges direct and relay
evidence.

## Next Entry

Start at the worker socket request/envelope path and carry the existing
deferred-terminal mixed pooled mode through `WorkerServiceSocketClient`,
`WorkerServiceEndpoint`, and `WorkerTransferService` without adding
application-side route controls.
