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

- Closed the delayed-admission execution gap in the daemon-first intent path.
- Re-submitting an existing `TransferIntent` now asks the daemon to promote
  delayed admission and returns the current execution payload, including the
  daemon-issued ticket, active reservations, lease tokens, receipt, and plan.
- `WorkerIntentTransferExecutor` now performs a bounded delayed-admission retry
  through the runtime daemon view and then executes only the admitted daemon
  payload.
- Added no test, experiment, benchmark, paper-validation, server-validation, or
  compatibility export-layer code.

## Validation

- `python -m py_compile` passed for the updated daemon server, intent executor,
  runtime daemon view, runtime options, and runtime session modules.
- Searches found no remaining use of the removed initial-response lease-token
  helper.
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
- Tests still import old worker and daemon package-level internals; current
  constraints defer test migration until system implementation is complete.
- Tests still import old `turbobus.profile` helpers; current constraints defer
  test migration until system implementation is complete.

## Next Main Target

Continue the system-code closure audit with the next complete implementation
boundary that still mixes responsibilities or exposes route-control bypasses.
