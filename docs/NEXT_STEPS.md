# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

The next task is to tighten daemon ticket, receipt, and cleanup state
consistency so completed or failed intent transfers retain one authoritative
daemon-issued ticket identity from scheduling through status update and cleanup.

## Exit Criteria

- Daemon status updates reject stale or mismatched ticket evidence for both
  worker and backend completion paths.
- Transfer receipts expose completion evidence, ticket identity, and cleanup
  state without accepting intent-only or synthetic completion.
- Cleanup removes ticket, lease, and receipt state consistently after completed
  or failed runtime-session transfers.
- No benchmark, paper-validation, experiment, server-validation, compatibility
  shim, or export layer code is added during this pass.

## Current Code Work

- Inspect `turbobus/daemon/server.py`, `turbobus/daemon/dispatch.py`,
  `turbobus/transfer_execution.py`, and `turbobus/runtime_session.py` for
  ticket, receipt, and cleanup consistency.
- Keep direct fallback and worker CUDA resource evidence bound to the
  daemon-issued ticket, transfer id, plan generation, and registered source and
  destination buffers.
- Keep the old `client_transfer.py`, `turbobus/worker/helper.py`,
  `turbobus/daemon/protocol.py`, and `turbobus/worker_managed.py` files
  deleted. Do not recreate compatibility export layers.
- Continue code implementation and refactoring without adding server test
  commands or using server validation as the current entry point.

## Next Entry

Continue the code implementation pass by inspecting daemon ticket, receipt, and
cleanup state consistency. Keep the work focused on system code; defer tests,
benchmarks, paper-validation, experiments, and server validation until the full
system implementation pass is complete.
