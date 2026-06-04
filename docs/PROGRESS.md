# TurboBus Progress

## Current State

Current main target: session/job/buffer registration into the TransferIntent
execution path.

The daemon-first path remains the production route:
`TransferIntent` submission, daemon scheduling, daemon-issued
`ExecutionTicket`, worker/backend completion, and `TransferReceipt`
consumption through `TurboBusRuntimeSession`.

Server validation, benchmark work, paper validation, experiments, and new test
code remain deferred until the full system implementation pass is complete.

## Completed This Round

- Offload adapter submissions now re-confirm the runtime session and pending
  buffer registrations before submitting a transfer intent, so the adapter
  path stays tied to the live runtime session.
- Added no test, experiment, benchmark, paper-validation, server-validation,
  or compatibility export-layer code.

## Validation

- `python -m py_compile turbobus/offload/store.py` passed.
- `git diff --check` passed.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests, examples, and benchmarks still contain old production-path
  assumptions; current-stage constraints defer migration until system
  implementation is complete.

## Next Main Target

Continue with one concrete implementation boundary: `WorkerIntentTransferExecutor`
and the daemon-issued worker path.
