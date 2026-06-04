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
- Keep `TurboBusRuntimeSession.open_socket()` as the production socket entry:
  it owns daemon socket and optional worker socket clients, then still submits
  only `TransferIntent` and consumes `TransferReceipt`.
- Keep model loading, training offload, inference KV, vLLM connector, vLLM KV,
  and lower-level vLLM integration paths on the runtime-session API so callers
  do not assemble daemon clients, transfer contexts, or buffer registration
  manually.
- Keep daemon receipt wait, reschedule, direct fallback, and worker backend
  execution bound to daemon-issued, fresh `ExecutionTicket` data with ticket
  evidence in status updates.
- Keep completed transfer tickets archived only for receipt/release evidence,
  not available as active execution tickets after backend or worker completion.
- Keep successful worker completion cleanup bound to daemon release evidence:
  completed worker envelopes must show a real reservation release, not a
  generic cleanup or skipped cleanup response.
- Keep `OffloadStore` bound to runtime-session-owned clients, and keep closed
  runtime sessions from accepting later buffer registration, transfer submit,
  receipt wait, or profile bootstrap calls.
- Keep daemon cleanup paths from leaving cleaned job, buffer, or session
  transfers in the runtime scheduling view.
- Keep terminal receipt wait available to the authenticated transfer owner after
  job/session/buffer cleanup, without letting cleaned transfers re-enter
  scheduling or worker execution.
- Keep scheduler runtime feedback tied to live relay paths, active leases,
  reservations, and worker staging records so busy relays are not reused while
  a daemon-issued transfer still owns relay resources.
- Keep delayed admission promotion daemon-owned: resource release, cleanup, or
  lease reaping may re-run scheduler state and issue fresh `ExecutionTicket`
  data, but applications cannot select routes or promote themselves.
- Keep the old `client_transfer.py`, `turbobus/worker/helper.py`, and
  `turbobus/daemon/protocol.py` files deleted. Do not recreate them as
  compatibility export layers.
- Continue code work on the system path; server-only validation is deferred
  until after the complete system implementation pass.
- Do not add mock native backends, fake correctness gates, server-validation
  gates, benchmark helpers, or paper-validation code while validating this
  path.

## Next Entry

Continue the code implementation pass by inspecting CUDA worker executor
resource evidence and transfer metadata. Backend execution should keep using
daemon-authorized resources and should report resource, ticket, plan, and byte
evidence without restoring application-side route controls, compatibility
export layers, benchmark hooks, or server-validation gates.
