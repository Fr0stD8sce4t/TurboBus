# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The daemon-first path remains the production route:
`TransferIntent` submission, daemon scheduling, daemon-issued
`ExecutionTicket`, worker/backend completion, and `TransferReceipt`
consumption through `TurboBusRuntimeSession`.

Server validation, benchmark work, paper validation, experiments, and new test
code remain deferred until the full system implementation pass is complete.

## Completed This Round

- Production socket runtime sessions can now resolve daemon and worker socket
  paths from `RuntimeOptions` while wiring runtime, execution, profile, and
  worker clients into one `TurboBusRuntimeSession`.
- Worker socket startup now builds a CUDA worker executor and data-plane
  resource binder with the same backend/runtime options path used by the
  runtime session, while still executing only daemon-issued tickets.
- vLLM production connector now passes socket paths and chunk size through
  `RuntimeOptions` and delays daemon session opening until real CUDA buffer
  registration binds the target GPU.
- Added no test, experiment, benchmark, paper-validation, server-validation, or
  compatibility export-layer code.

## Validation

- `python -m py_compile turbobus/runtime_session.py
  turbobus/runtime_options.py turbobus/worker/process.py
  turbobus/worker/lifecycle.py turbobus/worker/cuda_executor.py
  turbobus/adapters/vllm_kv_connector.py turbobus/adapters/vllm_config.py
  turbobus/adapters/vllm.py turbobus/offload/context.py` passed.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests, examples, and benchmarks still contain old production-path
  assumptions; current-stage constraints defer migration until system
  implementation is complete.

## Next Main Target

Continue with one concrete implementation boundary: H2D/D2H system main path
closure.
