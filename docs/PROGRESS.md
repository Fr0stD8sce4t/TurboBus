# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The system-level runtime path submits `TransferIntent`, uses daemon scheduling,
issues `ExecutionTicket` plans, and keeps the old `client_transfer.py` file
deleted. `TurboBusRuntimeSession.open()` is now the public system entry without
application-side relay selection: the target GPU is bound from registered CUDA
buffers and relay eligibility is discovered from the daemon before session
registration and profile bootstrap. Model loading, training offload, and
inference KV adapters now have runtime-session entry points. Worker service and
production process entry points route requests through the standard lifecycle.
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
The old `turbobus/worker/helper.py` and `turbobus/daemon/protocol.py` export
layers have also been removed. Server-only validation is deferred until after
the system implementation pass, so it no longer blocks code work in this stage.

## Completed This Round

- Changed public workload adapter constructors for model loading, training
  offload, inference KV, and vLLM KV to derive their transfer context from
  `TurboBusRuntimeSession`.
- Kept only an internal protected inference adapter path for already-derived
  group contexts, and removed offload context/receipt re-exports from the
  adapter package.
- Updated lower-level vLLM integration CPU backing allocation to create
  `SharedPinnedCpuBuffer` objects for the runtime-session path.

## Validation

- `python -m py_compile turbobus\adapters\model_loading.py
  turbobus\adapters\training_offload.py turbobus\adapters\inference.py
  turbobus\adapters\vllm.py turbobus\adapters\vllm_integration.py
  turbobus\adapters\vllm_kv_connector.py turbobus\adapters\__init__.py`
  passed.
- `git diff --check` passed with only CRLF conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM integration behavior, relay/pooled execution, and
  server-only behavior remain deferred until the full system implementation
  pass is complete.

## Next Main Target

Tighten cross-job isolation and daemon authority in the daemon/worker
production path by inspecting `OffloadStore` and the runtime-session transfer
lifecycle for any remaining bypass of session-owned buffer registration,
daemon scheduling, ticket execution, or receipt cleanup while keeping server
validation deferred.
