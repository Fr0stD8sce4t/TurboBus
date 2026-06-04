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

- Implemented worker RUNNING status reporting before daemon-issued backend
  execution starts.
- Worker lifecycle now reports a daemon `running` status after authorization
  and staging allocation, before CUDA/backend execution.
- Worker lifecycle and completion envelopes now carry the running update and
  daemon response so runtime/scheduler feedback can observe active execution
  instead of seeing only submitted then complete.
- Completion validation checks that the running response belongs to the same
  transfer and reached daemon state `running`.
- Added no test, experiment, benchmark, paper-validation, server-validation, or
  compatibility export-layer code.

## Validation

- `python -m py_compile` passed for the updated worker lifecycle, worker models,
  intent execution support, intent executor, daemon server, worker socket
  client, and worker transport modules.
- Searches confirmed the new running update/response fields are confined to the
  worker execution and completion-validation path.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests still contain old production-path assumptions. Current-stage
  constraints defer test migration until the system implementation pass is
  complete.
- The closure audit still needs to continue across worker failure handling,
  scheduler load accounting, runtime receipt validation, and adapter boundaries for
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
