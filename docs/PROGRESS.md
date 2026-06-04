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

- Audited public API/runtime execution ownership during the daemon-first
  closure pass.
- Removed the application-provided `transfer_executor` and `execution_daemon`
  injection path from public `TurboBusClient`.
- Moved worker/backend execution ownership into `TurboBusRuntimeSession`: it
  now submits the intent to the daemon, receives the daemon scheduling payload,
  and invokes the runtime-owned `WorkerIntentTransferExecutor`.
- Added a private runtime execution daemon view so the runtime-owned executor
  can wait for intent receipts while using the execution-role daemon client for
  status, cleanup, and lease validation.
- Changed worker execution support imports to use `turbobus.worker.models`
  directly for worker envelope and lifecycle types.

## Validation

- `python -m py_compile turbobus\api\client.py turbobus\runtime_session.py
  turbobus\intent_executor.py turbobus\intent_execution_support.py` passed.
- `rg -n "transfer_executor|IntentTransferExecutor|execution_daemon"
  turbobus\api` found no public API executor injection entry.
- `rg -n "_RuntimeExecutionDaemonView|execution_view|TurboBusClient\("
  turbobus\runtime_session.py turbobus\api\client.py` found the runtime-owned
  private execution view and the pure public client construction.
- `rg -n "from \.worker import" turbobus\intent_executor.py
  turbobus\intent_execution_support.py` found no worker package model imports
  in the intent execution helpers.
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
