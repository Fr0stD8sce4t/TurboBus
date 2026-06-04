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

- Split profile bootstrap support as one runtime/backend boundary.
- Moved simple profile result models to `turbobus/profiling/models.py`.
- Moved daemon profile serialization, cache freshness, validation, and
  daemon-entry reconstruction to `turbobus/profiling/daemon_format.py`.
- Moved CUDA profile collection, daemon `put_profile`, and bootstrap cache
  flow to `turbobus/profiling/bootstrap.py`.
- Updated `TurboBusRuntimeSession` to call the owning bootstrap module.
- Deleted `turbobus/profile.py` instead of keeping a compatibility export
  layer.
- Added no test, experiment, benchmark, paper-validation, server-validation, or
  compatibility export-layer code.

## Validation

- `python -m py_compile` passed for the new profiling modules and directly
  related runtime/backend modules.
- Searches found no production reference to the removed `turbobus.profile`
  module.
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
- Tests still import old `turbobus.profile` helpers; current constraints defer
  test migration until system implementation is complete.

## Next Main Target

Continue the system-code closure audit with the next complete implementation
boundary that still mixes responsibilities or exposes route-control bypasses.
