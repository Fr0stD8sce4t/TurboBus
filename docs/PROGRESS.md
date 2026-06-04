# TurboBus Progress

## Current State

Current main target: isolation and authority hardening.

The daemon-first path remains the production route:
`TransferIntent` submission, daemon scheduling, daemon-issued
`ExecutionTicket`, worker/backend completion, and `TransferReceipt`
consumption through `TurboBusRuntimeSession`.

Server validation, benchmark work, paper validation, experiments, and new test
code remain deferred until the full system implementation pass is complete.

## Completed This Round

- Scheduler load feedback now includes live running-transfer pressure, so
  relay fairness fallback reacts to actual concurrent execution instead of only
  byte totals.
- Added no test, experiment, benchmark, paper-validation, server-validation,
  or compatibility export-layer code.

## Validation

- Pending: `python -m py_compile turbobus/scheduler/load_feedback.py` and
  `git diff --check`.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests, examples, and benchmarks still contain old production-path
  assumptions; current-stage constraints defer migration until system
  implementation is complete.

## Next Main Target

Continue with one concrete implementation boundary: shared buffer and lease
retirement hardening.
