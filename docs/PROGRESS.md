# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The active route is no longer paper-validation tooling. The current code work
is the system-level Python runtime path: keep the old `client_transfer.py`
deleted, drive transfers through `TurboBusRuntimeSession`, bootstrap daemon
profile data, keep daemon-first intent submission, and make worker/backend
completion remain the source of completed receipts.

## Completed This Round

- Split the old `client_transfer.py` responsibilities into focused runtime,
  intent execution, direct fallback, buffer registration, worker-managed, and
  shared execution modules.
- Removed the old `client_transfer.py` compatibility export layer and updated
  imports to the new owning modules.
- Added `TurboBusRuntimeSession` as the public session/job/buffer/intent entry
  point for H2D and D2H transfers.
- Exported `TurboBusRuntimeSession` from the package root.
- Added native CUDA profile bootstrap helpers in the backend/runtime layer.
- Wired `TurboBusRuntimeSession.bootstrap_profile()` and automatic
  profile-on-first-transfer into daemon `get_profile`/`put_profile`.
- Replaced old progress text that still pointed at paper-validation and
  correctness-gate work.

## Validation

- `python -m py_compile turbobus/backends/cuda.py turbobus/runtime_engine.py turbobus/runtime_session.py turbobus/__init__.py`
  passed.
- `python -c "from turbobus import TurboBusRuntimeSession; from turbobus import runtime_engine; from turbobus.backends.cuda import default_cuda_backend; print('imports ok')"`
  passed.
- `git diff --check` passed with Windows line-ending warnings only.

## Remaining Risk

- The runtime session profile bootstrap has not yet been validated on a CUDA
  multi-GPU server.
- Worker socket and daemon socket production startup still need a later pass.

## Next Main Target

After profile bootstrap is checked and committed, continue with daemon and
worker socket production startup cleanup.
