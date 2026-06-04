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

- Split runtime-session support responsibilities into owning modules under
  `turbobus/runtime/`.
- Moved daemon execution view, buffer registration fingerprinting, runtime
  buffer intent validation, receipt validation, role-client resolution, and
  session cleanup state out of `turbobus/runtime_session.py`.
- Kept `turbobus/runtime_session.py` focused on the public
  `TurboBusRuntimeSession` flow: session open, buffer registration, profile
  bootstrap, intent execution, receipt validation, and close.
- Preserved the rule that custom object sessions without a daemon socket path
  must provide explicit runtime, profile, and execution daemon clients.
- Added no test, experiment, benchmark, paper-validation, server-validation, or
  compatibility export-layer code.

## Validation

- `python -m py_compile` passed for the updated runtime-session modules and
  directly related runtime entry files.
- Searches found no old private runtime helper names left in production code.
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

## Next Main Target

Continue the system-code closure audit with the next complete implementation
boundary that still mixes responsibilities or exposes route-control bypasses.
