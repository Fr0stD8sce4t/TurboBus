# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The system-level runtime path submits `TransferIntent`, uses daemon scheduling,
issues `ExecutionTicket` plans, and keeps the old `client_transfer.py` file
deleted. `TurboBusRuntimeSession.open()` and `open_socket()` are the public
system entries: they own daemon clients, optional worker socket clients,
session/job/buffer registration, profile bootstrap, intent submission, and
receipt waits without application-side relay selection.

Profile bootstrap is owned by `TurboBusRuntimeSession`: runtime buffers bind
the target GPU, daemon relay discovery supplies the relay set, native profiling
collects profile data for that exact target/relay set, and daemon profile
storage rejects mismatched target or relay payloads.

Model loading, training offload, inference KV, vLLM KV, vLLM connector, and
lower-level vLLM integration paths construct their workload adapters from
`TurboBusRuntimeSession`. Adapter-owned offload handles verify receipt
job/session/intent/ticket ownership before consuming `TransferReceipt`
objects, and closed runtime sessions reject later adapter submit or wait calls.

Daemon and worker production startup paths are aligned with the unified
runtime-session route. The daemon startup path rejects synthetic topology
fixtures for production startup, the worker socket service routes envelopes
through the standard worker lifecycle, and the old worker-managed manual
target/relay client entry has been removed instead of kept as a compatibility
layer.

Direct fallback and worker CUDA execution both bind resource evidence to the
same daemon-issued ticket data used by status evidence: ticket id, transfer id,
plan generation, source buffer, destination buffer, job, and session. That
keeps shared pinned CPU buffers and CUDA IPC GPU handles tied to the
runtime-session registered buffers recorded by the daemon-issued execution.

Daemon release and cleanup now have separate state boundaries. Worker lease
release retires lease-side runtime state while keeping completed receipts
available for the runtime session to consume. Session, job, or buffer cleanup
retires the full transfer record, including active tickets, archived
completion tickets, completion source/evidence, planning state, admission
state, queue records, peer identity, intent mappings, and terminal status.

Offload, model-loading, training-offload, inference KV, vLLM slot adapter, vLLM
integration, and vLLM connector paths hand off through `TurboBusRuntimeSession`
and `OffloadStore` intent submission. vLLM connector prefix state now uses the
daemon runtime session id for saved-prefix lookup, storage, events, and cleanup;
the vLLM connector engine id is preserved only as connector metadata.

Server-only validation remains deferred until after the full system
implementation pass. Current code work should continue through code reading,
implementation, refactoring, and existing minimal local checks without adding
server test commands or server-validation gates.

Runtime load feedback is now isolated in scheduler code. `DaemonScheduler`
builds its scheduling policy view from daemon-owned runtime resource state,
and daemon relay admission uses the same busy-relay parser when checking
active relay paths, reservations, leases, and staging records.

Worker data-plane resource binding now requires the full daemon-authorized
`WorkerTransferRequest` instead of a standalone `WorkerDataPlaneRequest`.
Opening shared pinned CPU buffers and CUDA IPC device handles revalidates the
daemon-issued `ExecutionTicket`, worker authorization, data-plane request,
ticket id, transfer id, and plan generation before touching resources.

Real workload adapters keep using the unified runtime-session path. Offload
block state now exposes the last daemon intent, receipt, ticket, decision,
topology snapshot, job, session, receipt state, and error identity, and the
lower-level vLLM integration entry rejects non-runtime-session objects before
building KV adapters.

The old route-shaped Python transfer request path is removed from production
code. Runtime sessions and intent execution use `TransferIntent` ranges
directly, the daemon socket client no longer exposes `plan_transfer` or
`plan_transfer_request`, and the daemon dispatch path no longer accepts the
external `PLAN_TRANSFER` request type. Daemon-internal planning remains only as
the implementation used after `submit_transfer_intent`.

The manual relay reservation control-plane path is also removed from
production entry points. Applications and socket clients can no longer submit a
`RESERVE_TRANSFER` request or call a daemon client method that names a
`relay_gpu`; relay leases are created inside daemon scheduling after a
`TransferIntent` has been accepted.

Session relay selection is now daemon-owned. Runtime sessions register only
the target CUDA device discovered from registered buffers; the daemon chooses
eligible relay GPUs from its configured relay pool and topology inventory, then
returns the selected relay set in the session record for profile bootstrap and
receipt identity.

Worker execution is now exposed through the full daemon-authorized lifecycle.
The worker client no longer has public execute-only or execute-and-status
shortcuts that can skip lease cleanup; runtime and worker socket paths use the
authorize, execute, status report, and cleanup lifecycle.

The adapter and planner public surfaces have been tightened. Adapter modules no
longer export old aliases for the same runtime-session-backed classes, the
public transfer client exposes the explicit `submit_transfer_intent()` entry,
and module-level planner helper functions that could look like public
application planning APIs have been removed. Scheduler code still owns planning
through `PlannerEngine`.

The daemon control plane no longer exposes old manual release or reschedule
requests. `RESCHEDULE_TRANSFER` and `RELEASE_TRANSFER` request types, daemon
socket client helpers, dispatch routes, and daemon public methods were removed.
Lease release remains available only through daemon cleanup and worker
lifecycle cleanup paths.

The daemon socket client surface is now split by role. `TurboBusDaemonClient`
keeps application/runtime control operations such as registration, intent
submission, receipt waits, and daemon inventory/admin operations. Execution-only
operations such as worker authorization, status updates, lease validation, and
cleanup live on `TurboBusDaemonExecutionClient`, which is used by the runtime
executor and worker service. Target/relay profile bootstrap operations live on
`TurboBusDaemonProfileClient`, which is owned by the runtime session rather
than the ordinary app-facing daemon client.

The app-visible runtime/backend transfer-mode controls are removed from
production code. `RuntimeOptions` no longer accepts `transfer_mode`, the CUDA
backend no longer exposes `transfer_mode_value()`, and native transfer-mode
conversion helpers are gone. Native runtime initialization still enables the
pool-capable default; direct, relay, and pooled execution remain scheduler
outcomes carried in daemon-issued plans.

Offload store block operations now expose one canonical production method set:
`add()`, `remove()`, and `block()`. The old `add_block()`, `remove_block()`,
and `get_block()` aliases are removed from production code, and model-loading
and training-offload adapters call the canonical methods directly.

Worker transfer internals now keep authorization, status reporting, and cleanup
coordinators private to the worker lifecycle. `WorkerTransferClient` no longer
exposes a public authorization-only method; the production worker entry remains
the full authorize-execute-status-cleanup lifecycle.

Daemon socket clients are now split by public role. `TurboBusDaemonClient`
submits `TransferIntent` and waits for `TransferReceipt` only. Runtime
registration operations live on `TurboBusDaemonRuntimeClient`, daemon admin
operations live on `TurboBusDaemonAdminClient`, profile bootstrap operations
live on `TurboBusDaemonProfileClient`, and worker execution operations live on
`TurboBusDaemonExecutionClient`. `TurboBusRuntimeSession` owns the runtime,
profile, execution, and intent clients instead of using one broad daemon client.

Manual buffer daemon registration helpers were removed from
`SharedPinnedCpuBuffer` and `CudaIpcDeviceBuffer`; runtime buffer registration
now goes through `TurboBusRuntimeSession` and `buffer_registration.py`. The old
`turbobus.control` pure re-export package entry was deleted instead of kept as
a compatibility layer.

`TurboBusRuntimeSession.open()` no longer falls back to using one broad daemon
object for runtime registration, profile bootstrap, and execution operations.
Socket-backed sessions still derive the role clients from the daemon socket
path, while custom object sessions must provide explicit runtime, profile, and
execution daemon clients.

## Completed This Round

- Removed runtime-session broad daemon-client fallback for runtime, profile,
  and execution roles.
- Added explicit runtime, profile, and execution daemon client accessors inside
  `TurboBusRuntimeSession`.
- Kept server validation, benchmark work, paper validation, experiments, and
  new test code deferred.

## Validation

- `python -m py_compile turbobus\runtime_session.py
  turbobus\daemon\client.py turbobus\api\client.py` passed.
- Targeted `rg` checks found no remaining runtime-session `else daemon_client`
  or `or self.daemon_client` role fallback. Remaining matches are explicit
  missing-client checks.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain deferred until the full system implementation
  pass is complete.
- Existing test files still contain old `PLAN_TRANSFER` and `TransferRequest`
  references plus old manual reservation checks. Current-stage constraints
  defer test migration until the system implementation pass is complete, so
  this round only removes production-path compatibility drift.
- Existing tests may also still expect session registration to accept
  caller-provided relay lists; current-stage constraints defer test migration
  until the system implementation pass is complete.
- Existing tests may still call removed worker shortcut methods; current-stage
  constraints defer test migration until the system implementation pass is
  complete.
- Existing tests or external callers may still reference adapter aliases or
  `TurboBusClient.submit()`/`submit_transfer()`. Current-stage constraints
  prefer removing compatibility names from production code before migrating
  tests.
- Existing tests may still reference removed manual release or reschedule
  request types. Current-stage constraints defer test migration until the
  system implementation pass is complete.
- Existing tests may still instantiate `TurboBusClient` with a transfer
  executor but without an execution daemon client. Current-stage constraints
  defer test migration until the system implementation pass is complete.
- Existing tests may still refer to removed runtime/backend transfer-mode
  helper APIs or the old native `TransferHandle`. Current-stage constraints
  defer test migration until the system implementation pass is complete.
- Existing tests may still refer to removed offload block alias methods or
  worker authorization helper exports. Current-stage constraints defer test
  migration until the system implementation pass is complete.
- Existing tests may still import `turbobus.control`, use buffer
  `register_with_daemon()` helpers, or expect one broad `TurboBusDaemonClient`
  to expose registration/admin methods. Current-stage constraints defer test
  migration until the system implementation pass is complete.
- Existing tests or fake daemon objects may still call `TurboBusRuntimeSession.open()`
  without explicit runtime/profile/execution role clients. Current-stage
  constraints defer test migration until the system implementation pass is
  complete.
- A final system-code closure audit still needs to look for remaining
  compatibility drift or application-side route selection before planning
  tests, benchmarks, paper validation, server validation, or experiments.

## Next Main Target

Continue the code implementation pass with a system-code closure audit while
keeping server validation deferred until the full system implementation pass
is complete.
