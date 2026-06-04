# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

The next task is to perform a system-code closure audit for the daemon-first
path before any test, benchmark, paper-validation, server-validation, or
experiment work is planned.

## Exit Criteria

- Public runtime session, intent executor, worker lifecycle, scheduler,
  offload store, and adapters have no remaining compatibility wrappers or
  application-side physical route controls.
- System modules still route work through `TransferIntent`, daemon scheduling,
  daemon-issued `ExecutionTicket`, worker/backend completion, and
  `TransferReceipt`.
- Applications and adapters still submit only `TransferIntent` and consume
  `TransferReceipt`.
- No benchmark, paper-validation, experiment, server-validation, compatibility
  shim, or export layer code is added during this pass.

## Current Code Work

- Inspect `turbobus/runtime_session.py`, `turbobus/intent_executor.py`,
  `turbobus/direct_fallback.py`, `turbobus/buffer_registration.py`,
  `turbobus/worker/lifecycle.py`, `turbobus/scheduler/daemon.py`,
  `turbobus/daemon/server.py`, `turbobus/offload_store.py`, and
  `turbobus/adapters/` for remaining route-selection or compatibility drift.
- Keep workload adapters owned by `TurboBusRuntimeSession` and preserve daemon
  runtime session, job, intent, ticket, decision, topology, and receipt
  identity in adapter-visible state.
- Keep the old `client_transfer.py`, `turbobus/worker/helper.py`,
  `turbobus/daemon/protocol.py`, `turbobus/worker_managed.py`, and old
  route-shaped transfer request files deleted. Do not recreate compatibility
  export layers.
- Continue code implementation and refactoring without adding server test
  commands or using server validation as the current entry point.

## Next Entry

Continue the code implementation pass with a system-code closure audit. Keep
the work focused on implementation and refactoring; defer tests, benchmarks,
paper-validation, experiments, and server validation until the full system
implementation pass is complete.
