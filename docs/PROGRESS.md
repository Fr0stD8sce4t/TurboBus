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

- Audited the offload adapter support layer as one full system boundary.
- Split the old `turbobus/offload_store.py` monolith into
  `turbobus/offload/context.py`, `blocks.py`, `handles.py`, `stats.py`, and
  `store.py`.
- Updated model-loading, training-offload, inference, vLLM slot, and vLLM KV
  connector adapters to import from the new owning modules.
- Deleted `turbobus/offload_store.py` instead of keeping a compatibility
  export layer.
- Preserved the daemon-first offload path: adapters still build
  `TransferIntent`, submit through `TurboBusRuntimeSession`, validate
  `TransferReceipt` evidence, and keep physical route keys out of policy hints.

## Validation

- `python -m py_compile turbobus\offload\__init__.py
  turbobus\offload\stats.py turbobus\offload\context.py
  turbobus\offload\blocks.py turbobus\offload\handles.py
  turbobus\offload\store.py turbobus\adapters\model_loading.py
  turbobus\adapters\training_offload.py turbobus\adapters\inference.py
  turbobus\adapters\vllm.py turbobus\adapters\vllm_integration.py
  turbobus\adapters\vllm_kv_connector.py` passed.
- `rg -n "offload_store" turbobus` found no production reference to the
  removed monolith.
- `rg -n "from \.\.offload_store|from \.offload_store|from turbobus\.offload_store|import turbobus\.offload_store"
  turbobus benchmarks examples` found no old import path.
- `rg --files turbobus\offload` confirmed the new offload package owns the
  implementation files.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests still contain old production-path assumptions such as removed
  transfer request, manual reservation, worker shortcut, broad daemon client,
  manual daemon planning, adapter policy hints, old `runtime_engine` imports,
  public worker internals, package-level worker data-plane exports, and
  duck-typed offload clients, exported daemon intent protocol helpers, old
  `offload_store.py` imports, and compatibility entry points. Current-stage
  constraints defer test migration until the system implementation pass is
  complete.
- A final system-code closure audit still needs to continue looking for
  compatibility drift, application-side physical route controls, or public
  bypasses around daemon-issued execution tickets.

## Next Main Target

Continue the system-code closure audit for the daemon-first path while keeping
tests, benchmarks, paper validation, server validation, and experiments
deferred.
