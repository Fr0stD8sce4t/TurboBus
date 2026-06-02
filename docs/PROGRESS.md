# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The active route is the system-level Python runtime path. The old
`client_transfer.py` file remains deleted, transfers run through
`TurboBusRuntimeSession`, profile bootstrap writes daemon profile data, daemon
and worker CLIs run socket services, and upper adapters use the runtime session
without application-side path selection.

## Completed This Round

- `CudaWorkerExecutor` now revalidates the current worker request against its
  daemon-issued `ExecutionTicket` immediately before invoking the CUDA backend.
- Direct fallback now executes through an internal ticket-only plan path instead
  of exporting a bare direct-plan execution entry.
- The old `client_transfer.py` file remains deleted and no compatibility
  export layer was reintroduced.

## Validation

- `python -m py_compile turbobus/direct_fallback.py turbobus/worker/cuda_executor.py turbobus/runtime_session.py`
  passed.
- `python -m unittest test.python.unit.test_worker_cuda_executor` passed.
- `python -m unittest test.python.integration.test_client_worker_transfer`
  passed with one expected platform skip.
- `python -m unittest test.python.integration.test_worker_helper` passed.

## Remaining Risk

- Peer isolation has not yet been validated with separate OS users or
  containers on the real daemon socket.
- Runtime load feedback into scheduler decisions still needs a focused pass
  before real CUDA multi-GPU validation.

## Next Main Target

Wire runtime load and topology feedback into daemon-first scheduler decisions.
