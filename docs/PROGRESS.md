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

Server-only validation remains deferred until after the full system
implementation pass. Current code work should continue through code reading,
implementation, refactoring, and existing minimal local checks without adding
server test commands or server-validation gates.

## Completed This Round

- Tightened daemon profile storage so profile `target_device`, relay
  `target_device`, and relay device set must match the daemon profile key.
- Added runtime-side daemon profile validation before `put_profile`, binding
  native profile output to the `TurboBusRuntimeSession` target GPU and
  daemon-discovered relay set.
- Added cached daemon profile target validation before converting it back into
  a runtime/native profile object.
- Updated active plan files to move the next implementation entry to
  direct-fallback and worker CUDA resource ownership.

## Validation

- `python -m py_compile turbobus\profile.py turbobus\daemon\profiles.py
  turbobus\runtime_session.py turbobus\runtime_engine.py
  turbobus\backends\cuda.py turbobus\daemon\server.py
  turbobus\daemon\dispatch.py` passed.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain deferred until the full system implementation
  pass is complete.
- Direct fallback and worker CUDA resource ownership still need inspection to
  keep shared pinned CPU and CUDA IPC GPU buffers tied to daemon-issued tickets.

## Next Main Target

Continue the code implementation pass by inspecting direct fallback and worker
CUDA resource ownership while keeping server validation deferred until the full
system implementation pass is complete.
