# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

The current task is to harden the runtime/native boundary for production data
movement. Python runtime code should keep profile bootstrap, tensor validation,
native plan conversion, and CUDA backend execution clearly separated so worker
and runtime session paths execute only daemon-issued plans.

## Exit Criteria

- Native extension loading, profile conversion, tensor validation, and transfer
  plan conversion have clear owning modules or helpers.
- `CudaWorkerExecutor` and direct fallback execute daemon-issued
  `ExecutionTicket` plans without choosing physical paths.
- Runtime session profile bootstrap still writes daemon profile data through
  `put_profile` and does not create mock profile data.
- Existing runtime/session/adapters continue to submit `TransferIntent` and
  consume `TransferReceipt`.
- No benchmark, paper-validation, experiment, or compatibility shim code is
  added during this pass.

## Current Code Work

- Start from `turbobus/runtime_engine.py`, `turbobus/backends/cuda.py`, and
  `turbobus/worker/cuda_executor.py`.
- Keep the old `client_transfer.py` file deleted. Do not recreate it as a
  compatibility export layer.
- Do not add mock native backends, fake correctness gates, benchmark helpers,
  or paper-validation code while validating this path.

## Next Entry

Trace native profile bootstrap and daemon-issued plan execution from
`TurboBusRuntimeSession` through CUDA backend and worker executor code.
