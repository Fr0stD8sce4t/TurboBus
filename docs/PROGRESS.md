# TurboBus Progress

## Current State

Current main target: real H2D / D2H execution path closure.

The daemon-first path remains the production route:
`TransferIntent` submission, daemon scheduling, daemon-issued
`ExecutionTicket`, worker/backend completion, and `TransferReceipt`
consumption through `TurboBusRuntimeSession`.

Server validation, benchmark work, paper validation, experiments, and new test
code remain deferred until the full system implementation pass is complete.

## Completed This Round

- `TurboBusRuntimeSession` now owns adapter transfer context creation for the
  offload and inference adapters that feed `fetch_h2d()` / `offload_d2h()`.
- Kept the round free of new test, experiment, benchmark, paper-validation,
  server-validation, or compatibility export-layer code.

## Validation

- Not run yet for this turn.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Older benchmark and example surfaces still use `TurboBusClient` and have not
  been migrated to the runtime-session-first API yet.
- Existing tests, examples, and benchmarks still contain old production-path
  assumptions; current-stage constraints defer migration until system
  implementation is complete.

## Next Main Target

Continue with one concrete implementation boundary: real H2D / D2H execution
path closure.
