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

- Tightened public worker and daemon package boundaries as one system API pass.
- Removed worker lifecycle client, service, error, and authorization parser
  exports from `turbobus.worker`; those remain in their owning lifecycle module.
- Kept worker package-level access focused on service process, socket client,
  endpoint, and transport entry points.
- Removed daemon role-client and manual daemon constructor exports from
  `turbobus.daemon`; production owners now import daemon role clients directly
  from `turbobus.daemon.client`.
- Kept `TurboBusRuntimeSession` and worker process startup wired to the owning
  daemon-client module rather than package-level shortcuts.
- Added no test, experiment, benchmark, paper-validation, server-validation, or
  compatibility export-layer code.

## Validation

- `python -m py_compile` passed for the updated daemon, worker, runtime
  session, and top-level import modules.
- Searches found no production import from `turbobus.worker` package-level
  worker internals.
- Searches found no production daemon role-client import through
  `turbobus.daemon` package-level shortcuts.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests still contain old production-path assumptions. Current-stage
  constraints defer test migration until the system implementation pass is
  complete.
- The closure audit still needs to continue across daemon, worker, scheduler,
  runtime, and adapter boundaries for remaining compatibility drift or public
  bypasses.
- Tests still import old worker and daemon package-level internals; current
  constraints defer test migration until system implementation is complete.

## Next Main Target

Continue the system-code closure audit with the next complete implementation
boundary that still mixes responsibilities or exposes route-control bypasses.
