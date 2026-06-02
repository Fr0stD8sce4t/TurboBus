# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The active route is the system-level Python runtime path. The old
`client_transfer.py` file remains deleted, transfers run through
`TurboBusRuntimeSession`, profile bootstrap writes daemon profile data, daemon
and worker CLIs run socket services, and upper adapters use the runtime session
without application-side path selection.

## Completed This Round

- Split native extension loading, native plan conversion, tensor validation,
  and profile bootstrap into separate runtime/native boundary modules.
- Reduced `runtime_engine.py` to runtime options and transfer-handle ownership.
- Updated the CUDA backend to depend on the new native boundary modules instead
  of reaching into `runtime_engine` private state.
- Runtime session profile bootstrap now calls the profile module and still
  writes daemon profile data through `put_profile`.

## Validation

- `python -m py_compile turbobus/runtime_engine.py turbobus/native_runtime.py turbobus/native_plan.py turbobus/tensor_validation.py turbobus/profile.py turbobus/backends/cuda.py turbobus/runtime_session.py turbobus/worker/cuda_executor.py turbobus/direct_fallback.py`
  passed.
- `python -m unittest test.python.unit.test_runtime_engine` passed.
- `python -m unittest test.python.unit.test_backend_cuda` passed.
- `python -m unittest test.python.unit.test_worker_cuda_executor` passed.

## Remaining Risk

- Peer isolation has not yet been validated with separate OS users or
  containers on the real daemon socket.
- Direct fallback and worker CUDA execution still need a focused ticket-path
  pass before real CUDA multi-GPU validation.

## Next Main Target

Harden daemon-issued `ExecutionTicket` execution through direct fallback and
CUDA worker paths.
