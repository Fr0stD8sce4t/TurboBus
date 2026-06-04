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

- Transfer retirement now archives receipt-facing intent, status, decision,
  ticket, and ownership data before active runtime state is dropped, so
  `transfer_status()` and `wait_transfer_receipt()` still work after cleanup
  retires the live queue records.
- Added no test, experiment, benchmark, paper-validation, server-validation,
  or compatibility export-layer code.

## Validation

- `python -m py_compile turbobus/daemon/server.py
  turbobus/scheduler/load_feedback.py` passed.
- `git diff --check` passed with a CRLF warning on `turbobus/daemon/server.py`.

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
