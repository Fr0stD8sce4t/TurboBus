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

- Closed the worker cleanup to scheduler-feedback boundary for daemon-issued
  relay plans.
- Daemon reservation cleanup responses now report the cleaned reservation id,
  cleaned id set, cleanup kind, and cleanup mode when resources were actually
  released.
- Worker completion validation now requires cleanup evidence to cover every
  lease id carried by the worker lifecycle, not only the primary lease.
- This keeps pooled or multi-relay worker completion from being accepted while
  leaving relay reservations active and visible as busy to later scheduling.
- Added no test, experiment, benchmark, paper-validation, server-validation, or
  compatibility export-layer code.

## Validation

- `python -m py_compile` passed for the updated daemon server, worker lifecycle,
  worker models, intent executor, and intent execution support modules.
- Searches found no use of a nonexistent `WorkerTransferAuthorizationRequest`
  `lease_ids` field; cleanup coverage is taken from the worker completion
  envelope.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests still contain old production-path assumptions. Current-stage
  constraints defer test migration until the system implementation pass is
  complete.
- The closure audit still needs to continue across scheduler feedback, worker
  failure handling, runtime receipt validation, and adapter boundaries for
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
