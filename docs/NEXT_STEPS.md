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

- Continue inspecting production modules for remaining route-selection or
  compatibility drift after removing old client, worker, transfer-request,
  reservation, session-relay, worker-shortcut, adapter-alias, planner-helper,
  release-transfer, reschedule-transfer, app-facing execution-status, runtime
  transfer-mode, backend transfer-mode, and ordinary daemon-client profile entry
  points, plus offload block alias and worker partial-lifecycle public entry
  points, ordinary daemon-client runtime/admin operations, buffer manual daemon
  registration helpers, and pure control re-export files.
- Remove remaining broad daemon-client fallback behavior from runtime-session
  role clients; socket-backed sessions may derive role clients from socket path,
  but custom object sessions must provide explicit runtime, profile, and
  execution daemon clients.
- Keep workload adapters owned by `TurboBusRuntimeSession` and preserve daemon
  runtime session, job, intent, ticket, decision, topology, and receipt
  identity in adapter-visible state.
- Keep the old `client_transfer.py`, `turbobus/worker/helper.py`,
  `turbobus/daemon/protocol.py`, `turbobus/worker_managed.py`, and old
  route-shaped transfer request, manual relay reservation, or session relay
  selection entry files deleted. Worker execution entry points must keep the
  full authorize-execute-status-cleanup lifecycle. Do not recreate
  compatibility export layers.
- Continue code implementation and refactoring without adding server test
  commands or using server validation as the current entry point.

## Next Entry

Continue the code implementation pass with a system-code closure audit. Keep
the work focused on implementation and refactoring; defer tests, benchmarks,
paper-validation, experiments, and server validation until the full system
implementation pass is complete.
