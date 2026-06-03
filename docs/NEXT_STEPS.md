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

- Keep `TurboBusRuntimeSession.open()` as the public system entry without
  application-side relay selection; it must register session, job, and buffers
  before submitting `TransferIntent`.
- Keep model loading, training offload, inference KV, vLLM connector, vLLM KV,
  and lower-level vLLM integration paths on the runtime-session API so callers
  do not assemble daemon clients, transfer contexts, or buffer registration
  manually.
- Keep daemon receipt wait, reschedule, direct fallback, and worker backend
  execution bound to daemon-issued, fresh `ExecutionTicket` data with ticket
  evidence in status updates.
- Keep daemon cleanup paths from leaving cleaned job, buffer, or session
  transfers in the runtime scheduling view.
- Keep the old `client_transfer.py`, `turbobus/worker/helper.py`, and
  `turbobus/daemon/protocol.py` files deleted. Do not recreate them as
  compatibility export layers.
- Continue code work on the system path; server-only validation is deferred
  until after the complete system implementation pass.
- Do not add mock native backends, fake correctness gates, server-validation
  gates, benchmark helpers, or paper-validation code while validating this
  path.

## Next Entry

Continue the code implementation pass by inspecting `OffloadStore` and the
runtime-session transfer lifecycle for any remaining places where application
code can bypass session-owned buffer registration, daemon scheduling, ticket
execution, or receipt cleanup. Also remove any old pure export layer that
remains after refactoring. Keep server-only behavior as a deferred validation
risk, not a blocker for this stage.
