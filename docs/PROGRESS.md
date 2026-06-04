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

- Worker failed completion envelopes now require daemon FAILED status evidence.
- Worker failed completion envelopes now require cleanup evidence for the
  daemon-issued lease set and staging release.
- Runtime intent execution now returns the daemon failed `TransferReceipt` for
  confirmed worker/backend failure instead of converting that receipt path into
  an exception.
- Added no test, experiment, benchmark, paper-validation, server-validation, or
  compatibility export-layer code.

## Validation

- `python -m py_compile` passed for intent execution support, intent executor,
  runtime session, worker models, worker lifecycle, and daemon server modules.
- Searches confirmed the worker failed completion validator and runtime
  failed-receipt return path are present in the intended modules.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests, examples, and benchmarks still contain old production-path
  assumptions; current-stage constraints defer migration until system
  implementation is complete.

## Next Main Target

Continue with one concrete implementation boundary: runtime receipt validation,
runtime session production startup, or adapter submission/receipt consumption
through `TurboBusRuntimeSession`.
