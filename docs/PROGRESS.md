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

## Completed This Round

- Tightened worker resource binding so `WorkerDataPlaneResourceBinder.bind()`
  accepts only a full `WorkerTransferRequest`, not a bare data-plane request.
- Revalidated daemon-issued ticket ownership before binding shared pinned CPU
  buffers and CUDA IPC GPU handles.
- Added ticket id, plan generation, session id, and job id to worker resource
  evidence emitted by bound resources.
- Kept the old compatibility/export-layer files deleted and left server
  validation deferred.
- Updated the active plan files to move the next implementation entry to
  real workload closure through the unified runtime session.

## Validation

- `python -m py_compile turbobus\worker\resources.py
  turbobus\worker\lifecycle.py turbobus\worker\models.py
  turbobus\worker\validation.py turbobus\worker\cuda_executor.py
  turbobus\runtime_session.py` passed.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain deferred until the full system implementation
  pass is complete.
- Real workload closure still needs inspection so every adapter path stays on
  the unified runtime session and consumes real receipts.

## Next Main Target

Continue the code implementation pass by inspecting real workload closure
through the unified runtime session while keeping server validation deferred
until the full system implementation pass is complete.
