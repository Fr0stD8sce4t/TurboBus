# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

The current task is to harden isolation and authority across daemon and worker
control paths. The daemon must remain the only source of `ExecutionTicket` and
lease authority, while workers and adapters only execute or consume daemon
issued objects.

## Exit Criteria

- Peer identity checks consistently cover session, job, and buffer ownership.
- Worker authorization rejects stale, missing, or cleanup-invalidated tickets
  and leases.
- Cleanup and reschedule cannot leave executable tickets or active leases for
  transfers that are no longer admitted.
- Runtime session and adapters continue to submit `TransferIntent` and consume
  `TransferReceipt` without choosing physical paths.
- No benchmark, paper-validation, experiment, or compatibility shim code is
  added during this pass.

## Current Code Work

- Start from `turbobus/daemon/server.py`, `turbobus/worker/validation.py`, and
  `turbobus/worker/lifecycle.py`.
- Keep the old `client_transfer.py` file deleted. Do not recreate it as a
  compatibility export layer.
- Do not add mock correctness gates, fake receipts, benchmark helpers, or
  paper-validation code while validating this path.

## Next Entry

Trace daemon-issued `ExecutionTicket` validation through worker authorization,
worker execution, status reporting, cleanup, and receipt creation.
