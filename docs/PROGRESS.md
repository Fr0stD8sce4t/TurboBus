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

- Audited daemon and scheduler planning ownership during the daemon-first
  closure pass.
- Removed the public `TurboBusDaemon.plan_transfer()` method name by making the
  existing planning implementation daemon-internal as `_plan_transfer()`.
- Kept `submit_transfer_intent()` on the daemon as the production planning
  entry; it calls `_plan_transfer()` with daemon-owned `mode="auto"`.
- Confirmed production API, runtime session, offload store, and adapters do
  not expose `PLAN_TRANSFER` or manual `plan_transfer` entry points.

## Validation

- `python -m py_compile turbobus\daemon\server.py
  turbobus\daemon\dispatch.py turbobus\scheduler\daemon.py` passed.
- `rg -n "\.plan_transfer\(|def plan_transfer\(|self\.plan_transfer\("
  turbobus` found only daemon-internal scheduler calls and the scheduler
  method itself.
- `rg -n PLAN_TRANSFER turbobus\daemon turbobus\api
  turbobus\runtime_session.py turbobus\offload_store.py turbobus\adapters`
  found no production external plan request entry.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests still contain old production-path assumptions such as removed
  transfer request, manual reservation, worker shortcut, broad daemon client,
  manual daemon planning, and compatibility entry points. Current-stage
  constraints defer test migration until the system implementation pass is
  complete.
- A final system-code closure audit still needs to continue looking for
  compatibility drift, application-side physical route controls, or public
  bypasses around daemon-issued execution tickets.

## Next Main Target

Continue the system-code closure audit for the daemon-first path while keeping
tests, benchmarks, paper validation, server validation, and experiments
deferred.
