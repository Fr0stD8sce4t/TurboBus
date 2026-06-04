# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The production path is being kept on the daemon-first route:
`TransferIntent` submission, daemon scheduling, daemon-issued
`ExecutionTicket`, worker/backend completion, and `TransferReceipt`
consumption. `TurboBusRuntimeSession` remains the public runtime entry for
session, job, buffer, profile bootstrap, intent submission, worker execution,
receipt wait, and cleanup wiring. The old `client_transfer.py`,
`turbobus.control`, route-shaped transfer request, manual relay reservation,
manual session relay selection, worker shortcut, transfer-mode, broad daemon
client, buffer self-registration, and pure re-export compatibility entry
points remain removed.

Server validation, benchmark work, paper validation, experiments, and new test
code remain deferred until the full system implementation pass is complete.
Current progress should continue through code reading, implementation,
refactoring, and existing minimal local checks without adding server test
commands or making server validation a current entry point.

## Completed This Round

- Audited public worker/data-plane exports during the daemon-first closure
  pass.
- Removed raw `WorkerDataPlaneRequest` and `WorkerDataPlaneCompletion` from
  the public `turbobus.worker` package entry.
- Changed production intent execution to import worker completion/lifecycle
  types from `turbobus.worker.models`, the module that owns those
  implementations.
- Kept the raw worker data-plane schema objects available only through
  `turbobus.schema` and internal worker modules that derive them from
  daemon-issued tickets.

## Validation

- `python -m py_compile turbobus\worker\__init__.py
  turbobus\intent_executor.py turbobus\worker\models.py` passed.
- `rg -n "WorkerDataPlaneRequest|WorkerDataPlaneCompletion"
  turbobus\worker\__init__.py` found no raw worker data-plane request or
  completion exports; the remaining matches are
  `WorkerDataPlaneCompletionEnvelope`.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests still contain old production-path assumptions such as removed
  transfer request, manual reservation, worker shortcut, broad daemon client,
  and compatibility entry points. Current-stage constraints defer test
  migration until the system implementation pass is complete.
- A final system-code closure audit still needs to continue looking for
  compatibility drift, application-side physical route controls, or public
  bypasses around daemon-issued execution tickets.

## Next Main Target

Continue the system-code closure audit for the daemon-first path while keeping
tests, benchmarks, paper validation, server validation, and experiments
deferred.
