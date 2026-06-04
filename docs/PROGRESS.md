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

- Profile bootstrap now feeds daemon cache entries into worker CUDA execution
  using the same daemon entry shape returned by `put_profile` and `get_profile`.
- Direct fallback now preserves daemon planning metadata from the planned
  payload when executing a daemon-issued direct `ExecutionTicket`, so backend
  profile installation can see `planning.profile_entry`.
- Worker and direct execution now install the daemon profile entry directly
  instead of wrapping it as a nested profile object.
- Added no test, experiment, benchmark, paper-validation, server-validation, or
  compatibility export-layer code.

## Validation

- `python -m py_compile turbobus/direct_fallback.py
  turbobus/worker/cuda_executor.py turbobus/profiling/bootstrap.py
  turbobus/profiling/daemon_format.py` passed.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled profile use, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests, examples, and benchmarks still contain old production-path
  assumptions; current-stage constraints defer migration until system
  implementation is complete.

## Next Main Target

Continue with one concrete implementation boundary: shared buffer lifecycle
cleanup.
