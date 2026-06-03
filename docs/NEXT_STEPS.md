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

- `TurboBusRuntimeSession.open()` is the public system entry and must not expose
  application-side relay selection. It should bind the target GPU from the
  registered CUDA buffer and obtain relay eligibility from daemon discovery.
- Runtime session should keep registering session, job, and buffers before
  submitting `TransferIntent`, then execute through `WorkerIntentTransferExecutor`
  and consume `TransferReceipt`.
- Model loading, training offload, and inference KV adapters should provide
  runtime-session constructors so callers do not manually assemble daemon
  clients, transfer contexts, or buffer registration.
- vLLM connector save/restore paths must record receipt traces only from real
  `TransferReceipt` handles and must fail if a transfer returns no receipt
  evidence.
- Keep the old `client_transfer.py`, `turbobus/worker/helper.py`, and
  `turbobus/daemon/protocol.py` files deleted. Do not recreate them as
  compatibility export layers.
- Continue code work on the system path; server-only validation is deferred
  until after the complete system implementation pass.
- Do not add mock native backends, fake correctness gates, server-validation
  gates, benchmark helpers, or paper-validation code while validating this
  path.

## Next Entry

Continue the code implementation pass by inspecting vLLM/offload adapter buffer
lifecycle for remaining places where callers can bypass the unified runtime
session path. Also remove any old pure export layer that remains after
refactoring. Keep server-only behavior as a deferred validation risk, not a
blocker for this stage.
