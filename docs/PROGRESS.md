# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The active route is no longer paper-validation tooling. The current code work
is the system-level Python runtime path: keep the old `client_transfer.py`
deleted, drive transfers through `TurboBusRuntimeSession`, bootstrap daemon
profile data, keep daemon-first intent submission, and make worker/backend
completion remain the source of completed receipts, and run daemon/worker as
production socket services.

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
- Changed daemon CLI from state preview to required production socket service.
- Changed worker CLI from helper/smoke-test process to production socket
  service entry point.
- Replaced old progress text that still pointed at paper-validation and
  correctness-gate work.

## Validation

- `python -m py_compile turbobus/daemon/__main__.py turbobus/daemon/startup.py turbobus/worker/__main__.py turbobus/worker/process.py turbobus/worker/transport.py turbobus/worker/__init__.py test/python/integration/test_worker_process.py`
  passed.
- `python -c "from turbobus.worker import build_worker_service_transport, run_worker_service_process; from turbobus.daemon.startup import DaemonStartupConfig; print('imports ok')"`
  passed.
- `python -m unittest test.python.integration.test_worker_process` passed
  with one Windows Unix-socket skip.
- `git diff --check` passed with Windows line-ending warnings only.

## Remaining Risk

- The runtime session profile bootstrap has not yet been validated on a CUDA
  multi-GPU server.
- Daemon and worker socket startup still need real server smoke runs.

## Next Main Target

After startup cleanup is checked and committed, connect upper layers such as
offload and vLLM adapters to `TurboBusRuntimeSession`.
