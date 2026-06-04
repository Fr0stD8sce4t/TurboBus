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

Server-only validation remains deferred until after the full system
implementation pass. Current code work should continue through code reading,
implementation, refactoring, and existing minimal local checks without adding
server test commands or server-validation gates.

## Completed This Round

- Added ticket binding to direct fallback resource evidence for successful
  backend completion and backend failure status updates.
- Added ticket binding to worker CUDA resource evidence for successful
  execution and bound-resource failure results.
- Kept the old compatibility/export-layer files deleted and left server
  validation deferred.
- Updated the active plan files to move the next implementation entry to
  daemon ticket, receipt, and cleanup state consistency.

## Validation

- `python -m py_compile turbobus\direct_fallback.py
  turbobus\worker\resources.py turbobus\worker\cuda_executor.py
  turbobus\buffer_registration.py turbobus\intent_executor.py` passed.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain deferred until the full system implementation
  pass is complete.
- Daemon ticket, receipt, and cleanup state consistency still needs inspection
  before moving to the next system-code target.

## Next Main Target

Continue the code implementation pass by inspecting daemon ticket, receipt, and
cleanup state consistency while keeping server validation deferred until the
full system implementation pass is complete.
