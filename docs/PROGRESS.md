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

- Audited the public client/API export boundary during the daemon-first
  closure pass.
- Removed `DaemonIntentClient` from the top-level `turbobus` package export.
- Removed `DaemonIntentClient` from `turbobus.api` and renamed the remaining
  protocol type inside `turbobus.api.client` to private `_DaemonIntentClient`.
- Kept `TurboBusClient` focused on submitting `TransferIntent` objects and
  waiting for `TransferReceipt` objects.

## Validation

- `python -m py_compile turbobus\api\__init__.py turbobus\api\client.py
  turbobus\__init__.py turbobus\runtime_session.py` passed.
- `rg -n "DaemonIntentClient" turbobus\__init__.py turbobus\api` found only
  the private `_DaemonIntentClient` implementation detail in
  `turbobus\api\client.py`.
- `rg -n "def (register_session|register_buffer|cleanup|authorize_worker_transfer)"
  turbobus\api\client.py` found no runtime, buffer, cleanup, or worker
  execution methods on the public client.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests still contain old production-path assumptions such as removed
  transfer request, manual reservation, worker shortcut, broad daemon client,
  manual daemon planning, adapter policy hints, old `runtime_engine` imports,
  public worker internals, package-level worker data-plane exports, and
  duck-typed offload clients, exported daemon intent protocol helpers, and
  compatibility entry points. Current-stage constraints defer test migration
  until the system implementation pass is complete.
- A final system-code closure audit still needs to continue looking for
  compatibility drift, application-side physical route controls, or public
  bypasses around daemon-issued execution tickets.

## Next Main Target

Continue the system-code closure audit for the daemon-first path while keeping
tests, benchmarks, paper validation, server validation, and experiments
deferred.
