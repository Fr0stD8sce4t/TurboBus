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

- Audited `TurboBusRuntimeSession` daemon role-client wiring during the
  daemon-first closure pass.
- Moved runtime, profile, and execution daemon-client resolution into
  `TurboBusRuntimeSession.__post_init__()` so the rule applies to direct
  construction as well as factory helpers.
- Kept socket-backed sessions able to derive role clients from the socket path.
- Kept custom object sessions required to provide explicit runtime, profile,
  and execution daemon clients instead of falling back to a broad daemon
  object.

## Validation

- `python -m py_compile turbobus\runtime_session.py
  turbobus\intent_executor.py turbobus\buffer_registration.py
  turbobus\profile.py turbobus\__init__.py` passed.
- `rg -n "TurboBusRuntimeSession\(" turbobus benchmarks examples docs` found
  no production direct construction sites that would be broken by the stricter
  initialization boundary.
- `rg -n "runtime_daemon_client is required without socket_path|execution_daemon_client is required without socket_path|profile_daemon_client is required without socket_path|__post_init__"
  turbobus\runtime_session.py` confirmed role-client enforcement now lives in
  runtime-session initialization.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests still contain old production-path assumptions such as removed
  transfer request, manual reservation, worker shortcut, broad daemon client,
  manual daemon planning, adapter policy hints, old `runtime_engine` imports,
  public worker internals, package-level worker data-plane exports, and
  compatibility entry points. Current-stage constraints defer test migration
  until the system implementation pass is complete.
- A final system-code closure audit still needs to continue looking for
  compatibility drift, application-side physical route controls, or public
  bypasses around daemon-issued execution tickets.

## Next Main Target

Continue the system-code closure audit for the daemon-first path while keeping
tests, benchmarks, paper validation, server validation, and experiments
deferred.
