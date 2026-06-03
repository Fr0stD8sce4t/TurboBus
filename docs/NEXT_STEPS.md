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
- Linux server startup validation with `CUDA_VISIBLE_DEVICES=0,1`,
  `--target-gpu 0`, `--min-relays 0`, and `--allow-missing-fabric` confirmed
  that production daemon and worker sockets can start and listen with
  owner-only socket files without occupying the busy NVLink pair on GPU5/GPU6.
- Do not add mock native backends, fake correctness gates, benchmark helpers,
  or paper-validation code while validating this path.

## Next Entry

With the production daemon and worker still running on the Linux server, run
only existing client APIs from a third shell:

- `TurboBusDaemonClient(...).get_inventory()` against the daemon socket.
- `WorkerServiceSocketClient(...).submit_envelope(...)` with a structurally
  valid but unauthorized worker envelope, expecting `authorization_failed`
  before staging allocation or CUDA execution.

Record the real responses, then update this file and `docs/PROGRESS.md`. Do not
add new test, benchmark, paper-validation, or fake request code.
