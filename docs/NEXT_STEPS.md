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
- Focus the next pass on worker cleanup coordination after authorization or
  status-report failure, especially how cleanup requests preserve daemon ticket
  and transfer authority across reservations, transfers, and buffers.
- Keep the old `client_transfer.py` file deleted. Do not recreate it as a
  compatibility export layer.
- Do not add mock native backends, fake correctness gates, benchmark helpers,
  or paper-validation code while validating this path.

## Next Entry

Trace worker cleanup coordinator calls after authorization, execution, and
status-report failures through daemon cleanup and release handling.
