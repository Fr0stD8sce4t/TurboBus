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
- Keep runtime-session role client resolution owned by
  `TurboBusRuntimeSession` initialization, not only by factory helpers. Direct
  object construction without a daemon socket path must still provide explicit
  runtime, profile, and execution daemon clients.
- Keep workload adapters owned by `TurboBusRuntimeSession` and preserve daemon
  runtime session, job, intent, ticket, decision, topology, and receipt
  identity in adapter-visible state.
- Keep the old `client_transfer.py`, `turbobus/worker/helper.py`,
  `turbobus/daemon/protocol.py`, `turbobus/worker_managed.py`, and old
  route-shaped transfer request, manual relay reservation, or session relay
  selection entry files deleted. Worker execution entry points must keep the
  full authorize-execute-status-cleanup lifecycle. Do not recreate
  compatibility export layers.
- Keep raw worker data-plane request and completion schema objects internal to
  the daemon-authorized worker execution path. The public `turbobus.worker`
  package should expose service and lifecycle entry points, not objects that
  let external callers construct data-plane work outside an
  `ExecutionTicket`.
- Keep worker/backend transfer executors owned by `TurboBusRuntimeSession`.
  The public `TurboBusClient` should submit intents and wait for receipts only;
  it must not accept application-provided execution hooks that can bypass the
  runtime-session worker path.
- Keep daemon planning reachable only through `TransferIntent` submission.
  Daemon-internal scheduling may still call the scheduler with a mode, but the
  daemon object should not expose a public manual planning method that lets
  callers choose direct, relay, or pool.
- Keep workload adapters from exposing `policy_hints` as an application-facing
  scheduling hook. Runtime/session code may still place system-owned hints
  such as chunk sizing into `TransferIntent`, and schema validation must keep
  rejecting physical route keys.
- Keep runtime configuration in the module that owns it. Do not recreate
  `runtime_engine.py` as a compatibility export layer after moving
  `RuntimeOptions`; import it from `turbobus.runtime_options` or the top-level
  package.
- Keep worker lifecycle internals private. `WorkerTransferClient` may own
  authorizer, executor, status reporter, cleanup coordinator, staging pool,
  and resource binder objects, but external code should use the complete
  authorize-execute-status-cleanup lifecycle entry rather than those internals.
- Keep the public `turbobus.worker` package focused on worker service and
  lifecycle entry points. Data-plane requests, worker result models, staging
  pools, resource binders, CUDA executors, and codec helpers should be imported
  only by the production modules that own that execution path, not exported as
  package-level entry points for external callers.
- Continue code implementation and refactoring without adding server test
  commands or using server validation as the current entry point.

## Next Entry

Continue the code implementation pass with a system-code closure audit. Keep
the work focused on implementation and refactoring; defer tests, benchmarks,
paper-validation, experiments, and server validation until the full system
implementation pass is complete.
