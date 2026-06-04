# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The system-level runtime path submits `TransferIntent`, uses daemon scheduling,
issues `ExecutionTicket` plans, and keeps the old `client_transfer.py` file
deleted. `TurboBusRuntimeSession.open()` is now the public system entry without
application-side relay selection: the target GPU is bound from registered CUDA
buffers and relay eligibility is discovered from the daemon before session
registration and profile bootstrap. `TurboBusRuntimeSession.open_socket()` now
owns daemon socket and optional worker socket clients for the production socket
path while keeping execution on daemon-issued `ExecutionTicket` data. Model
loading, training offload, and inference KV adapters now have runtime-session
entry points. Worker service and production process entry points route requests
through the standard lifecycle, and worker socket completion envelopes now
report `ok` only when that lifecycle reaches real worker completion.
vLLM connector save/restore tracing now requires real `TransferReceipt` handles
before it records receipt, decision, topology, or ticket ids.
vLLM saved prefixes are keyed by job id, session id, and prefix key, and the
connector binds externally created saved prefixes to its own job before storing
them.
Runtime session close now clears local buffer, target, relay, client, profile,
and registered-buffer state after daemon close succeeds, and also clears local
pending state when no daemon session was opened.
vLLM connector close now releases connector-owned saved prefixes, pending save
contexts, pooled CPU backings, connector metadata, global prefix-store entries
for the connector job/session, and its runtime session.
Daemon socket receipt wait and transfer reschedule paths now enforce
authenticated peer ownership before returning receipt state or replacing a
daemon-issued plan.
Daemon worker authorization responses now include an authorization timestamp,
and worker authorization rejects expired `ExecutionTicket` data before worker
execution can start. Direct fallback also rejects expired or malformed
daemon-issued tickets before invoking the backend, and the CUDA worker executor
re-checks daemon-authorized ticket freshness before converting a daemon plan
into a native backend plan.
Daemon job, buffer, and session cleanup now retires the affected transfer from
the runtime scheduling queue after canceling any non-terminal state, while
leaving terminal status and audit data available for control-plane inspection.
Model loading, training offload, inference KV, vLLM KV, vLLM connector
save/restore, and lower-level vLLM integration paths now construct their
workload adapters from `TurboBusRuntimeSession` instead of requiring
application code to assemble daemon clients or adapter transfer contexts.
`OffloadStore` now accepts only runtime-session-owned clients whose job and
session identity match the adapter context, and closed runtime sessions reject
later buffer registration, transfer submission, receipt wait, and profile
bootstrap calls.
Completed intent transfers now archive the execution ticket used for verified
worker/backend completion, then remove it from the active ticket map so it
cannot be reused for later execution while receipts and release checks still
have ticket evidence.
Failed or canceled intent transfers that come from worker/backend status
updates now also archive the daemon-issued ticket used by the terminal status
evidence, so their receipts keep ticket, transfer, and plan-generation binding
without leaving the ticket active for later execution.
Forced cleanup of missing job, buffer, or session targets now requires residual
transfer ownership evidence when the daemon has an authenticated peer. Unknown
cleanup targets cannot produce successful ownerless cleanup records on the
daemon socket path.
Successful worker completion cleanup now requires daemon release evidence:
daemon `release_transfer()` returns an explicit release payload, and worker
completion envelope validation rejects skipped cleanup, generic cleanup, or
missing released-reservation evidence.
Daemon release responses for completed intent transfers now include transfer id,
ticket id, plan generation, lease ids, and release time. Worker cleanup
aggregation verifies that all completed-release responses refer to the same
daemon-issued ticket and plan generation, and worker completion envelope
validation checks release evidence against the worker result metadata.
Worker data-plane resources now own the CUDA IPC device handle opened for the
daemon-authorized request. Closing the resources closes CUDA host registration,
shared CPU memory, and the device IPC handle, while the binding remains a
fallback cleanup path.
CUDA worker execution now snapshots the daemon-authorized CPU/GPU data-plane
resources while they are open and attaches that resource evidence to worker
success, bound failure, daemon completion evidence, and receipt metadata.
Direct fallback now snapshots the runtime-owned CPU/GPU endpoint resources used
for a daemon-issued direct `ExecutionTicket`, includes that resource evidence in
backend completion/failure evidence, and preserves backend expected-byte
evidence in daemon receipts.
Runtime-session intent submission now validates that custom and generated
`TransferIntent` objects use runtime-owned CPU/GPU buffers with matching
direction and byte ranges. Runtime receipt waits are limited to intents that
were successfully submitted through that session.
Adapter-owned runtime-session paths now explicitly reject closed runtime
sessions before creating offload or vLLM slot transfer contexts. vLLM connector
close now clears and reports pending load/save metadata, saved/sending/receiving
request sets, pending save parameters, prefix state, pending save backings, and
the owned runtime session through the unified session API.
Terminal receipt waits now keep authenticated transfer-owner evidence at plan
time, so a completed or canceled transfer can still be read by its owner after
job, session, or buffer cleanup removes active ownership state. Cleaned
transfers remain retired from scheduling and worker execution state.
Scheduler runtime feedback now treats active relay paths, live relay leases,
active reservations, and worker staging records as busy relay state for
admission and next-plan decisions. Runtime summaries and scheduling metadata
record those busy relays so daemon decisions are tied to live control-plane
state rather than benchmark hints.
Delayed relay admissions now promote from daemon-owned resource changes:
reservation release, cleanup, failure/cancel cleanup, and expired lease reaping
scan delayed transfers, re-run daemon scheduling, advance plan generation, and
issue fresh leases/tickets only when relay resources are available.
The old `turbobus/worker/helper.py` and `turbobus/daemon/protocol.py` export
layers have also been removed. Server-only validation is deferred until after
the system implementation pass, so it no longer blocks code work in this stage.

## Completed This Round

- Bound worker socket response `ok` to real worker lifecycle completion instead
  of treating failed authorization, failed execution, failed status reporting,
  or failed cleanup as successful socket completions.
- Updated the existing worker lifecycle/transport assertions to preserve daemon
  status and cleanup evidence while expecting failed worker lifecycles to return
  non-ok completion envelopes.

## Validation

- `python -m py_compile turbobus\worker\models.py
  turbobus\worker\codec.py turbobus\worker\endpoint.py
  turbobus\worker\lifecycle.py turbobus\worker\socket_client.py
  turbobus\worker\transport.py turbobus\worker\process.py` passed.
- `python -m unittest test.python.integration.test_worker_helper` passed.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM integration behavior, relay/pooled execution, and
  server-only behavior remain deferred until the full system implementation
  pass is complete.

## Next Main Target

Continue the code implementation pass by inspecting daemon peer identity,
cleanup, and ticket ownership on the production socket path while keeping
server validation deferred until the full system implementation pass is
complete.
