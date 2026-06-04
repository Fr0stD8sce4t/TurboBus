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

- Runtime sessions can now allocate runtime-owned shared pinned CPU buffers and
  register them through the daemon before transfer intent submission.
- Runtime buffer registration now records daemon-visible session id,
  ownership, and runtime buffer kind metadata while preserving worker-usable
  shared-memory and CUDA IPC metadata.
- Runtime sessions can clean up individual registered buffers through daemon
  cleanup; runtime-owned shared pinned CPU buffers are released locally on
  explicit buffer cleanup or session close.
- Added no test, experiment, benchmark, paper-validation, server-validation, or
  compatibility export-layer code.

## Validation

- `python -m py_compile turbobus/runtime_session.py
  turbobus/runtime/buffers.py turbobus/buffer_registration.py
  turbobus/runtime/session_state.py turbobus/offload/context.py
  turbobus/adapters/vllm.py turbobus/adapters/vllm_kv_connector.py` passed.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled buffer use, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests, examples, and benchmarks still contain old production-path
  assumptions; current-stage constraints defer migration until system
  implementation is complete.

## Next Main Target

Continue with one concrete implementation boundary: daemon/worker production
startup.
