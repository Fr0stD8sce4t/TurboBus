# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The active route is no longer paper-validation tooling. The current code work
is the system-level Python runtime path: keep the old `client_transfer.py`
deleted, drive transfers through `TurboBusRuntimeSession`, bootstrap daemon
profile data, run daemon/worker as production socket services, and connect
upper adapters to the runtime session without application-side path selection.
The current pass is hardening shared CPU buffer and CUDA IPC buffer lifecycle.

## Completed This Round

- Runtime session now fingerprints each buffer registration, so a reused
  buffer id with a changed shared-memory or CUDA IPC handle is re-registered
  with the daemon.
- Worker data-plane resources now expose a closed state, CPU/device buffer
  roles, and reject property access after close.
- Worker resource binding now rejects double entry and clears partial binding
  state on open failure.
- Worker resource cleanup still closes CUDA IPC handles even when CPU resource
  close raises.

## Validation

- `python -m py_compile turbobus/runtime_session.py turbobus/worker/resources.py turbobus/worker/lifecycle.py turbobus/worker/cuda_executor.py`
  passed.
- `python -m unittest test.python.unit.test_worker_cuda_executor` passed.
- `python -m unittest test.python.integration.test_worker_helper` passed.
- `python -m unittest test.python.integration.test_client_worker_transfer` passed
  with one platform skip.
- `git diff --check` passed with Windows line-ending warnings only.

## Remaining Risk

- The runtime session profile bootstrap has not yet been validated on a CUDA
  multi-GPU server.
- vLLM runtime-session save/restore still needs real CUDA, native extension,
  daemon, and worker socket validation.
- Daemon admission, receipt completion, and cleanup state still need a focused
  control-plane pass.

## Next Main Target

Harden daemon control-plane state consistency for profile misses, delayed
admission, receipt completion, and cleanup.
