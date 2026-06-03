# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The system-level runtime path submits `TransferIntent`, uses daemon scheduling,
issues `ExecutionTicket` plans, and keeps the old `client_transfer.py` file
deleted. `TurboBusRuntimeSession.open()` is now the public system entry without
application-side relay selection: the target GPU is bound from registered CUDA
buffers and relay eligibility is discovered from the daemon before session
registration and profile bootstrap. Worker service and production process entry
points route requests through the standard lifecycle. The old
`turbobus/worker/helper.py` and `turbobus/daemon/protocol.py` export layers have
also been removed. Server-only validation is deferred until after the system
implementation pass, so it no longer blocks code work in this stage.

## Completed This Round

- Removed `target_gpu` and `relay_gpus` from the public
  `TurboBusRuntimeSession.open()` entry.
- Made runtime session bind its target GPU only from registered CUDA buffers.
- Made runtime session cache relay eligibility only from daemon
  `discover_relays()` before daemon session registration and profile bootstrap.
- Left old compatibility export layers deleted and found no remaining pure
  export layer that needed removal in this pass.

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
offload/adapters and keeping server validation deferred.
