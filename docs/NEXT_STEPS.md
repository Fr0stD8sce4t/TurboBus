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
- vLLM saved-prefix lookup must be isolated by job and session so one job cannot
  restore another job's prefix when vLLM session ids collide.
- Runtime session close must clear local buffer, target, relay, profile, and
  registered-buffer state so closed sessions cannot carry stale adapter buffers
  into a new daemon session.
- vLLM connector close must release connector-owned saved prefixes, pending save
  contexts, pooled CPU backings, connector metadata, and the runtime session.
- Daemon socket receipt wait and reschedule paths must enforce authenticated
  peer ownership before exposing receipt state or replacing daemon plans.
- Worker authorization must reject daemon responses whose `ExecutionTicket` is
  already expired and must re-check ticket freshness immediately before worker
  execution starts.
- Keep the old `client_transfer.py`, `turbobus/worker/helper.py`, and
  `turbobus/daemon/protocol.py` files deleted. Do not recreate them as
  compatibility export layers.
- Continue code work on the system path; server-only validation is deferred
  until after the complete system implementation pass.
- Do not add mock native backends, fake correctness gates, server-validation
  gates, benchmark helpers, or paper-validation code while validating this
  path.

## Next Entry

Continue the code implementation pass by inspecting daemon/worker socket-path
direct/backend ticket freshness, status evidence, and cleanup enforcement for
stale execution state. Also remove any old pure export layer that remains after
refactoring. Keep server-only behavior as a deferred validation risk, not a
blocker for this stage.
