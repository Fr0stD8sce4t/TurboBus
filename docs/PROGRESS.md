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

- Audited worker lifecycle ownership during the daemon-first closure
  pass.
- Made `WorkerTransferClient` keep its authorizer, executor, status reporter,
  cleanup coordinator, staging pool, and resource binder as private
  implementation state.
- Preserved the full `submit_report_cleanup_lifecycle()` worker entry as the
  production path for daemon-authorized execution.
- Confirmed production code no longer reaches into worker client internals to
  bypass or inspect partial lifecycle components.

## Validation

- `python -m py_compile turbobus\worker\lifecycle.py
  turbobus\worker\process.py turbobus\worker\transport.py
  turbobus\worker\__init__.py turbobus\runtime_session.py` passed.
- `rg -n "self\.(authorizer|status_reporter|cleanup_coordinator|executor|staging_pool|resource_binder)|\.transfer_client\.(authorizer|status_reporter|cleanup_coordinator|executor|staging_pool|resource_binder)|worker_client\.(authorizer|status_reporter|cleanup_coordinator|executor|staging_pool|resource_binder)"
  turbobus` found no production access to public worker client internals.
- `rg -n "_authorizer|_status_reporter|_cleanup_coordinator|_executor|_staging_pool|_resource_binder"
  turbobus\worker\lifecycle.py` confirmed the worker lifecycle still owns
  those components privately.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests still contain old production-path assumptions such as removed
  transfer request, manual reservation, worker shortcut, broad daemon client,
  manual daemon planning, adapter policy hints, old `runtime_engine` imports,
  public worker internals, and compatibility entry points. Current-stage
  constraints defer test migration until the system implementation pass is
  complete.
- A final system-code closure audit still needs to continue looking for
  compatibility drift, application-side physical route controls, or public
  bypasses around daemon-issued execution tickets.

## Next Main Target

Continue the system-code closure audit for the daemon-first path while keeping
tests, benchmarks, paper validation, server validation, and experiments
deferred.
