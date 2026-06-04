# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

The next task is to tighten daemon/worker production cleanup and scheduling
state after runtime-session and adapter-owned transfers complete. Workers and
backends must continue to execute only daemon-issued `ExecutionTicket` plans,
and application/runtime code must continue to submit `TransferIntent` and
consume `TransferReceipt`.

## Exit Criteria

- Daemon cleanup of jobs, sessions, buffers, leases, tickets, and transfers
  preserves isolation across sessions and jobs.
- Completed, failed, canceled, or cleaned transfers cannot re-enter runtime
  scheduling or worker execution with stale ticket data.
- Terminal receipt wait remains available to the authenticated transfer owner
  without reviving cleaned scheduling state.
- No benchmark, paper-validation, experiment, server-validation, compatibility
  shim, or export layer code is added during this pass.

## Current Code Work

- Inspect `turbobus/daemon/server.py` cleanup and release paths for transfer
  retirement, archived ticket evidence, and delayed admission promotion.
- Inspect worker completion and cleanup envelopes so successful completion
  remains tied to daemon release evidence.
- Keep `TurboBusRuntimeSession` and adapters on the unified session API:
  applications submit only `TransferIntent` and consume session-owned
  `TransferReceipt`.
- Keep the old `client_transfer.py`, `turbobus/worker/helper.py`, and
  `turbobus/daemon/protocol.py` files deleted. Do not recreate compatibility
  export layers.
- Continue code implementation and refactoring without adding server test
  commands or using server validation as the current entry point.

## Next Entry

Continue the code implementation pass by inspecting daemon cleanup, release,
and delayed admission paths for stale execution-ticket or scheduling-state
reuse. Keep the work focused on system code; defer tests, benchmarks,
paper-validation, experiments, and server validation until the full system
implementation pass is complete.
