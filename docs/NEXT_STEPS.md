# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

The current task is to continue hardening production data movement now that
the runtime/native boundary has clear owning modules. The next pass should
focus on the daemon-issued plan path from `TurboBusRuntimeSession` through
direct fallback and `CudaWorkerExecutor`, making sure both paths execute only
validated `ExecutionTicket` plans and report real backend completion evidence.

## Exit Criteria

- Direct fallback and `CudaWorkerExecutor` reject unticketed or mismatched
  daemon plans before invoking the CUDA backend.
- Runtime session profile bootstrap writes daemon profile data through
  `put_profile` and does not create mock profile data.
- Existing runtime/session/adapters continue to submit `TransferIntent` and
  consume `TransferReceipt`.
- No benchmark, paper-validation, experiment, or compatibility shim code is
  added during this pass.

## Current Code Work

- Start from `turbobus/direct_fallback.py`, `turbobus/worker/cuda_executor.py`,
  and `turbobus/runtime_session.py`.
- Keep the old `client_transfer.py` file deleted. Do not recreate it as a
  compatibility export layer.
- Do not add mock native backends, fake correctness gates, benchmark helpers,
  or paper-validation code while validating this path.

## Next Entry

Trace a daemon-issued `ExecutionTicket` from `TurboBusRuntimeSession` through
direct fallback and worker CUDA execution.
