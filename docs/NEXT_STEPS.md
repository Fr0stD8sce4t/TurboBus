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
- Profile bootstrap should respect `RuntimeOptions.profile_cache_enabled`
  before reusing daemon cached profiles.
- Continue code work on the system path; server validation is deferred until
  after the complete system implementation pass.
- Do not add mock native backends, fake correctness gates, server-validation
  gates, benchmark helpers, or paper-validation code while validating this
  path.

## Next Entry

Continue the code implementation pass by inspecting `turbobus/runtime_session.py`,
`turbobus/profile.py`, `turbobus/offload_store.py`, and the adapter entry
points for remaining places where the unified runtime session path is not the
default system entry. Keep server-only behavior as a deferred validation risk,
not a blocker for this stage.
