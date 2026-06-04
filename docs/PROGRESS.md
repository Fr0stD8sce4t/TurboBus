# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The production path is being kept on the daemon-first route:
`TransferIntent` submission, daemon scheduling, daemon-issued
`ExecutionTicket`, worker/backend completion, and `TransferReceipt`
consumption. `TurboBusRuntimeSession` remains the public runtime entry for
session, job, buffer, profile bootstrap, intent submission, worker execution,
receipt wait, and cleanup wiring.

Server validation, benchmark work, paper validation, experiments, and new test
code remain deferred until the full system implementation pass is complete.

## Completed This Round

- Closed the public client/runtime boundary that let applications bypass
  `TurboBusRuntimeSession`.
- Removed the old direct `TurboBusClient` public API and deleted the
  `turbobus/api` compatibility entry files instead of preserving a re-export
  layer.
- Runtime receipt waiting now happens inside `TurboBusRuntimeSession` with
  runtime-owned daemon response parsing and receipt evidence validation.
- Offload and vLLM receipt evidence checks now import from runtime validation,
  not from the removed public client package.
- Added no test, experiment, benchmark, paper-validation, server-validation, or
  compatibility export-layer code.

## Validation

- `python -m py_compile` passed for the updated public package, runtime session,
  runtime validation, offload handle, vLLM connector, daemon server, and intent
  executor modules.
- Import/export checks confirmed `TurboBusRuntimeSession` remains exported and
  `TurboBusClient` is no longer exported from `turbobus`.
- Searches found no production `turbobus` module importing `TurboBusClient` or
  `turbobus.api`.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests still contain old production-path assumptions. Current-stage
  constraints defer test migration until the system implementation pass is
  complete.
- The closure audit still needs to continue across worker completion, cleanup,
  runtime receipt validation, scheduler feedback, and adapter boundaries for
  remaining compatibility drift or public bypasses.
- Benchmarks, examples, and tests still reference the removed old public client;
  current-stage constraints defer their migration until system implementation
  is complete.
- Tests still import old worker and daemon package-level internals; current
  constraints defer test migration until system implementation is complete.
- Tests still import old `turbobus.profile` helpers; current constraints defer
  test migration until system implementation is complete.

## Next Main Target

Continue the system-code closure audit with the next complete implementation
boundary that still mixes responsibilities or exposes route-control bypasses.
