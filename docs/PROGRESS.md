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
The old `turbobus/worker/helper.py` and `turbobus/daemon/protocol.py` export
layers have also been removed. Server-only validation is deferred until after
the system implementation pass, so it no longer blocks code work in this stage.

## Completed This Round

- Added runtime-session constructors to `ModelWeightLoader` and
  `TrainingOffloadManager`.
- These constructors register CPU/GPU buffers through `TurboBusRuntimeSession`
  and create `AdapterTransferContext` without physical route controls.
- Confirmed the existing inference KV adapter already has a matching
  runtime-session constructor.

## Validation

- `python -m py_compile turbobus\adapters\model_loading.py
  turbobus\adapters\training_offload.py turbobus\adapters\inference.py
  turbobus\offload_store.py` passed.
- `python -m unittest test.python.integration.test_client_worker_transfer`
  passed with one existing skip.
- `git diff --check` passed with only CRLF conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM integration behavior, and relay/pooled execution
  remain deferred until the full system implementation pass is complete.

## Next Main Target

Tighten cross-job isolation and daemon authority in the daemon/worker
production path while continuing code-first system implementation through
vLLM/offload adapter buffer lifecycle and keeping server validation deferred.
