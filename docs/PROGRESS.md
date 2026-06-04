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

- Audited workload adapter scheduling controls during the daemon-first
  closure pass.
- Removed application-facing `policy_hints` parameters from model-loading,
  training-offload, inference KV, vLLM KV slot, and vLLM integration adapters.
- Kept adapter submissions on workload metadata, priority, intent prefix, and
  runtime-session-owned buffer registration rather than application-provided
  scheduling hints.
- Left schema and offload-store policy hint validation in place so any
  lower-level `TransferIntent` path still rejects physical route keys.

## Validation

- `python -m py_compile turbobus\adapters\model_loading.py
  turbobus\adapters\training_offload.py turbobus\adapters\inference.py
  turbobus\adapters\vllm.py turbobus\adapters\vllm_integration.py
  turbobus\offload_store.py turbobus\schema.py` passed.
- `rg -n "policy_hints" turbobus\adapters` found no adapter-level
  application-facing policy hint entry.
- `rg -n "def _normalize_policy_hints|def _validate_policy_hints_no_physical|policy_hints must not choose physical paths"
  turbobus\schema.py turbobus\offload_store.py` confirmed lower-level
  physical route key rejection remains in schema and offload-store validation.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests still contain old production-path assumptions such as removed
  transfer request, manual reservation, worker shortcut, broad daemon client,
  manual daemon planning, adapter policy hints, and compatibility entry
  points. Current-stage constraints defer test migration until the system
  implementation pass is complete.
- A final system-code closure audit still needs to continue looking for
  compatibility drift, application-side physical route controls, or public
  bypasses around daemon-issued execution tickets.

## Next Main Target

Continue the system-code closure audit for the daemon-first path while keeping
tests, benchmarks, paper validation, server validation, and experiments
deferred.
