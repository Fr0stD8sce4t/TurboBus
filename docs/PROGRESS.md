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
Inference KV adapters can now derive their transfer context directly from a
`TurboBusRuntimeSession`, and the vLLM connector save/restore plus lower-level
vLLM integration paths construct their KV adapters from the runtime session
instead of manually assembling adapter contexts.
The old `turbobus/worker/helper.py` and `turbobus/daemon/protocol.py` export
layers have also been removed. Server-only validation is deferred until after
the system implementation pass, so it no longer blocks code work in this stage.

## Completed This Round

- Routed `VllmTurboBusIntegration` through `TurboBusRuntimeSession` instead of
  requiring callers to provide a daemon client and `AdapterTransferContext`.
- Updated lower-level vLLM adapter refresh to call
  `VllmKVSlotAdapter.from_runtime_session()`.

## Validation

- `python -m py_compile turbobus\adapters\vllm_integration.py
  turbobus\adapters\vllm.py` passed.
- `git diff --check` passed with only CRLF conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM integration behavior, relay/pooled execution, and
  server-only behavior remain deferred until the full system implementation
  pass is complete.

## Next Main Target

Tighten cross-job isolation and daemon authority in the daemon/worker
production path by inspecting remaining adapter and offload entry points that
still expose manual daemon-client or `AdapterTransferContext` assembly while
keeping server validation deferred.
