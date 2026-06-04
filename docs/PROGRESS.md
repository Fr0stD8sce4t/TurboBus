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

- Daemon plan and authorization payloads now carry the cached profile entry
  used for that session's execution path.
- Worker request parsing preserves the daemon profile entry in data-plane
  metadata.
- CUDA worker execution and direct fallback now install the daemon profile
  into the backend runtime before data movement.
- Added no test, experiment, benchmark, paper-validation, server-validation, or
  compatibility export-layer code.

## Validation

- `python -m py_compile turbobus/daemon/receipts.py turbobus/daemon/server.py
  turbobus/worker/models.py turbobus/worker/cuda_executor.py
  turbobus/direct_fallback.py` passed.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests, examples, and benchmarks still contain old production-path
  assumptions; current-stage constraints defer migration until system
  implementation is complete.

## Next Main Target

Continue with one concrete implementation boundary: adapter
submission/receipt consumption through `TurboBusRuntimeSession`.
