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
The old `turbobus/worker/helper.py` and `turbobus/daemon/protocol.py` export
layers have also been removed. Server-only validation is deferred until after
the system implementation pass, so it no longer blocks code work in this stage.

## Completed This Round

- Updated `TurboBusRuntimeSession.close()` to clear all local session-owned
  state after successful daemon close.
- Closed sessions no longer retain registered buffers, target GPU, relay cache,
  profile bootstrap state, or cached intent client.
- Closing before daemon session open now clears pending local buffers.

## Validation

- `python -m py_compile turbobus\runtime_session.py` passed.
- `python -m unittest test.python.integration.test_client_worker_transfer`
  passed with one existing skip.
- `git diff --check` passed with only CRLF conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM integration behavior, and relay/pooled execution
  remain deferred until the full system implementation pass is complete.

## Next Main Target

Tighten cross-job isolation and daemon authority in the daemon/worker
production path while continuing code-first system implementation through
adapter-owned prefix backing and connector lifecycle cleanup while keeping
server validation deferred.
