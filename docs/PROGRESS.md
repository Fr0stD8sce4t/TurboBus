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

- Runtime receipt validation now checks ticket, transfer, and plan-generation
  binding in receipt metadata.
- Complete receipts continue to require worker/backend execution and verified
  byte evidence.
- Failed or canceled receipts now require worker/backend execution source,
  execution evidence, error, ticket evidence, transfer evidence, and
  plan-generation evidence before `TurboBusRuntimeSession` returns them.
- Added no test, experiment, benchmark, paper-validation, server-validation, or
  compatibility export-layer code.

## Validation

- `python -m py_compile` passed for runtime validation, runtime session, intent
  executor, direct fallback, daemon receipts, and schema modules.
- Searches confirmed runtime receipt validation is called from
  `TurboBusRuntimeSession` and now includes failed/canceled evidence checks.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests, examples, and benchmarks still contain old production-path
  assumptions; current-stage constraints defer migration until system
  implementation is complete.

## Next Main Target

Continue with one concrete implementation boundary: runtime session production
startup or adapter submission/receipt consumption through
`TurboBusRuntimeSession`.
