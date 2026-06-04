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

- Added a production socket opener on `TurboBusRuntimeSession` that requires
  both daemon and worker socket paths.
- vLLM KV connector now uses the production socket opener, so it does not
  silently fall back to the in-process worker path.
- vLLM TurboBus config now requires non-empty daemon and worker socket paths.
- Added no test, experiment, benchmark, paper-validation, server-validation, or
  compatibility export-layer code.

## Validation

- `python -m py_compile` passed for runtime session, vLLM config, vLLM KV
  connector, worker socket/process modules, and daemon startup modules.
- Searches confirmed the production socket opener is defined and vLLM KV
  connector uses it with a required worker socket path.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests, examples, and benchmarks still contain old production-path
  assumptions; current-stage constraints defer migration until system
  implementation is complete.

## Next Main Target

Continue with one concrete implementation boundary: adapter submission/receipt
consumption through `TurboBusRuntimeSession` or profile bootstrap closure.
