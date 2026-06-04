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

## Completed This Round

- Removed the daemon client `reserve_transfer` method so public Python clients
  cannot reserve a chosen relay GPU.
- Removed the external daemon `RESERVE_TRANSFER` request type and dispatch
  branch.
- Removed the public daemon `reserve_transfer` method while preserving
  daemon-internal scheduler lease creation and worker lease validation/release.
- Kept server validation, benchmark work, paper validation, experiments, and
  new test code deferred.

## Validation

- `python -m py_compile turbobus\daemon\client.py
  turbobus\daemon\dispatch.py turbobus\daemon\server.py turbobus\schema.py`
  passed.
- `rg -n "reserve_transfer\(|RequestType\.RESERVE_TRANSFER|RESERVE_TRANSFER|ISSUE_LEASE|issue_lease"
  turbobus` found only daemon-internal `_issue_lease_token_locked`.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain deferred until the full system implementation
  pass is complete.
- Existing test files still contain old `PLAN_TRANSFER` and `TransferRequest`
  references plus old manual reservation checks. Current-stage constraints
  defer test migration until the system implementation pass is complete, so
  this round only removes production-path compatibility drift.
- A final system-code closure audit still needs to look for remaining
  compatibility drift or application-side route selection before planning
  tests, benchmarks, paper validation, server validation, or experiments.

## Next Main Target

Continue the code implementation pass with a system-code closure audit while
keeping server validation deferred until the full system implementation pass
is complete.
