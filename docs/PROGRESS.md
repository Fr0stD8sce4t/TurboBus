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
The old `turbobus/worker/helper.py` and `turbobus/daemon/protocol.py` export
layers have also been removed. Server-only validation is deferred until after
the system implementation pass, so it no longer blocks code work in this stage.

## Completed This Round

- Tightened vLLM connector receipt tracing so save/restore transfers fail when
  handles are missing or do not expose a `TransferReceipt`.
- Kept existing receipt evidence validation before connector events or saved
  prefixes record receipt and ticket metadata.

## Validation

- `python -m py_compile turbobus\adapters\vllm_kv_connector.py
  turbobus\adapters\vllm.py turbobus\offload_store.py` passed.
- `python -m unittest test.python.unit.test_vllm_kv_connector_main_path`
  passed.
- `git diff --check` passed with only CRLF conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM integration behavior, and relay/pooled execution
  remain deferred until the full system implementation pass is complete.

## Next Main Target

Tighten cross-job isolation and daemon authority in the daemon/worker
production path while continuing code-first system implementation through
vLLM/offload adapter buffer lifecycle and keeping server validation deferred.
