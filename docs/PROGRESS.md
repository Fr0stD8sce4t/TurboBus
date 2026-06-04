# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The system-level runtime path submits `TransferIntent`, uses daemon scheduling,
issues `ExecutionTicket` plans, and keeps the old `client_transfer.py` file
deleted. `TurboBusRuntimeSession.open()` and `open_socket()` are the public
system entries: they own daemon clients, optional worker socket clients,
session/job/buffer registration, profile bootstrap, intent submission, and
receipt waits without application-side relay selection.

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

Daemon, scheduler, worker, and backend paths keep execution bound to
daemon-issued tickets. Completed transfer tickets are archived for receipt and
release evidence, removed from active execution-ticket state, and cleanup paths
retire affected transfers from runtime scheduling while keeping terminal
receipt data available to authenticated owners.

Server-only validation remains deferred until after the full system
implementation pass. Current code work should continue through code reading,
implementation, refactoring, and existing minimal local checks without adding
server test commands or server-validation gates.

## Completed This Round

- Removed the old `turbobus/worker_managed.py` manual target/relay client path
  instead of preserving it as a compatibility entry.
- Moved the direct-fallback result shape into `turbobus/intent_executor.py` as
  `WorkerIntentTransferResult`, keeping it internal to daemon-issued
  `TransferIntent` execution.
- Inspected daemon and worker socket startup paths and kept worker socket
  execution routed through daemon authorization, standard lifecycle, status
  reporting, and cleanup.
- Updated active plan files and project guidance to remove the deleted
  worker-managed module from the current code path.

## Validation

- `python -m py_compile turbobus\intent_executor.py
  turbobus\runtime_session.py turbobus\worker\process.py
  turbobus\worker\transport.py turbobus\worker\socket_client.py
  turbobus\worker\lifecycle.py turbobus\worker\validation.py
  turbobus\worker\cuda_executor.py turbobus\daemon\__main__.py
  turbobus\daemon\startup.py` passed.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain deferred until the full system implementation
  pass is complete.
- Profile bootstrap and runtime/native profile ownership still need inspection
  to keep profile collection and daemon `put_profile` on the unified runtime
  session path.

## Next Main Target

Continue the code implementation pass by inspecting profile bootstrap and
runtime/native profile ownership while keeping server validation deferred until
the full system implementation pass is complete.
