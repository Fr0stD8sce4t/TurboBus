# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Continue the system-code closure audit for the daemon-first path. The current
code pass should keep production work flowing through `TurboBusRuntimeSession`,
`TransferIntent`, daemon scheduling, daemon-issued `ExecutionTicket`,
worker/backend completion, and `TransferReceipt`.

## Exit Criteria

- Public runtime, client, worker, daemon, offload, and adapter boundaries have
  no compatibility export layers, old entry wrappers, or application-side
  physical route controls.
- Runtime session setup owns session/job/buffer registration, profile
  bootstrap, worker execution wiring, receipt validation, and cleanup state.
- Applications and adapters submit intent and consume receipt only; they do not
  choose direct, relay, pool, target GPU, or relay GPU routes.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

- Keep auditing production modules for compatibility drift or public bypasses
  around daemon-issued execution tickets.
- Keep runtime-session support split into owning modules under
  `turbobus/runtime/`; `turbobus/runtime_session.py` should remain the public
  high-level entry, not a dump of helper logic.
- Keep socket-backed runtime sessions deriving daemon role clients from the
  socket path, while direct object construction without a daemon socket must
  provide explicit runtime, profile, and execution daemon clients.
- Keep worker/backend transfer executors owned by `TurboBusRuntimeSession`.
  `TurboBusClient` should submit intents and wait for receipts only.
- Keep offload and framework adapters bound to real `TurboBusRuntimeSession`
  instances, not duck-typed clients that can bypass runtime-owned registration,
  profile bootstrap, worker execution, or cleanup.
- Keep deleted files such as `client_transfer.py`, `offload_store.py`,
  `worker_managed.py`, `turbobus/worker/helper.py`, and
  `turbobus/daemon/protocol.py` deleted; do not recreate compatibility export
  layers.

## Next Entry

Continue the code implementation pass with the next complete system boundary
that still shows compatibility drift, mixed responsibilities, or route-control
bypass risk. Defer tests, benchmarks, paper validation, experiments, and server
validation until the full system implementation pass is complete.
