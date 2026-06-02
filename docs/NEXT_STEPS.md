# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

The next task is to tighten cross-job isolation and daemon authority in the
daemon/worker production path. Workers and backends must continue to execute
only daemon-issued `ExecutionTicket` plans, and application/runtime code must
continue to submit `TransferIntent` and consume `TransferReceipt`.

## Exit Criteria

- Daemon peer identity, job ownership, buffer ownership, lease, and ticket
  checks are clearly enforced on the daemon/worker socket path.
- Worker execution cannot proceed from application-selected physical paths or
  stale ticket data.
- Cleanup of jobs, buffers, leases, tickets, and transfer state preserves
  isolation across sessions and jobs.
- No benchmark, paper-validation, experiment, compatibility shim, or export
  layer code is added during this pass.

## Current Code Work

- Continue from `turbobus/daemon/server.py`, `turbobus/daemon/dispatch.py`,
  `turbobus/worker/lifecycle.py`, and `turbobus/worker/validation.py`.
- Production daemon startup now creates a daemon that requires authenticated
  socket peers, and the socket service fails before serving when the platform
  cannot provide supported Unix peer credentials.
- Intent transfer status updates now require worker/backend execution evidence
  bound to the current daemon `ExecutionTicket`, including failed and canceled
  external status reports.
- Session, job, buffer, lease, and transfer cleanup now release staging records
  and drop daemon execution tickets when transfers end in failed or canceled
  states.
- Worker service and production worker process paths now route socket requests
  through the standard authorization, execution, status, cleanup lifecycle.
- The old `turbobus/worker/helper.py` export layer has been deleted; worker
  modules import real implementation modules directly.
- Keep the old `client_transfer.py` file deleted. Do not recreate it as a
  compatibility export layer.
- Do not add mock native backends, fake correctness gates, benchmark helpers,
  or paper-validation code while validating this path.

## Next Entry

Stop local code work for this authority-hardening target until production
daemon and worker socket startup can be validated on a Linux CUDA server.
