# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The production path is being kept on the daemon-first route:
`TransferIntent` submission, daemon scheduling, daemon-issued
`ExecutionTicket`, worker/backend completion, and `TransferReceipt`
consumption. `TurboBusRuntimeSession` remains the public runtime entry for
session, job, buffer, profile bootstrap, intent submission, worker execution,
receipt wait, and cleanup wiring. The old `client_transfer.py`,
`turbobus.control`, route-shaped transfer request, manual relay reservation,
manual session relay selection, worker shortcut, transfer-mode, broad daemon
client, buffer self-registration, and pure re-export compatibility entry
points remain removed.

Server validation, benchmark work, paper validation, experiments, and new test
code remain deferred until the full system implementation pass is complete.
Current progress should continue through code reading, implementation,
refactoring, and existing minimal local checks without adding server test
commands or making server validation a current entry point.

## Completed This Round

- Audited offload and framework adapter runtime-session ownership during the
  daemon-first closure pass.
- Removed the duck-typed `TransferIntentClient` adapter boundary from
  `OffloadStore`.
- Required `OffloadStore` and its receipt handles to use real
  `TurboBusRuntimeSession` instances.
- Required vLLM slot and vLLM integration adapters to reject non-runtime-session
  objects before they register buffers or submit transfers.

## Validation

- `python -m py_compile turbobus\offload_store.py
  turbobus\adapters\model_loading.py turbobus\adapters\training_offload.py
  turbobus\adapters\inference.py turbobus\adapters\vllm.py
  turbobus\adapters\vllm_integration.py
  turbobus\adapters\vllm_kv_connector.py turbobus\runtime_session.py`
  passed.
- `rg -n "TransferIntentClient|Protocol" turbobus\offload_store.py` found no
  old duck-typed adapter client boundary.
- `rg -n "hasattr\(runtime_session" turbobus\adapters\vllm.py
  turbobus\adapters\vllm_integration.py` found no remaining vLLM
  runtime-session duck typing.
- `rg -n "TurboBusClient|DaemonIntentClient" turbobus\adapters
  turbobus\offload_store.py` found no adapter/offload dependency on the old
  public client path.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests still contain old production-path assumptions such as removed
  transfer request, manual reservation, worker shortcut, broad daemon client,
  manual daemon planning, adapter policy hints, old `runtime_engine` imports,
  public worker internals, package-level worker data-plane exports, and
  duck-typed offload clients, and compatibility entry points. Current-stage
  constraints defer test migration until the system implementation pass is
  complete.
- A final system-code closure audit still needs to continue looking for
  compatibility drift, application-side physical route controls, or public
  bypasses around daemon-issued execution tickets.

## Next Main Target

Continue the system-code closure audit for the daemon-first path while keeping
tests, benchmarks, paper validation, server validation, and experiments
deferred.
