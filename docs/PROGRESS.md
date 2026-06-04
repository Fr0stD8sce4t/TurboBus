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

- `fetch_h2d()` and `offload_d2h()` now default to the session's configured
  chunk size instead of a hardcoded 16 MiB path, and still submit daemon
  `TransferIntent` objects through `TurboBusRuntimeSession`.
- Offload and vLLM adapter contexts now inherit `chunk_bytes` into intent
  policy hints by default, so daemon plan chunking follows the same runtime
  configuration across the main H2D/D2H path.
- vLLM production KV slot wiring now carries chunk size through the same
  runtime options path while delaying daemon session opening until real CUDA
  buffer registration binds the target GPU.
- Added no test, experiment, benchmark, paper-validation, server-validation, or
  compatibility export-layer code.

## Validation

- `python -m py_compile turbobus/runtime_session.py
  turbobus/runtime_options.py turbobus/adapters/vllm.py
  turbobus/adapters/vllm_kv_connector.py turbobus/adapters/vllm_config.py
  turbobus/offload/context.py turbobus/offload/store.py turbobus/worker/process.py
  turbobus/worker/lifecycle.py turbobus/worker/cuda_executor.py` passed.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests, examples, and benchmarks still contain old production-path
  assumptions; current-stage constraints defer migration until system
  implementation is complete.

## Next Main Target

Continue with one concrete implementation boundary: runtime feedback into
scheduler load accounting.
