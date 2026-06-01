# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The active route is no longer paper-validation tooling. The current code work
is the system-level Python runtime path: split and remove the overloaded
`client_transfer.py`, add `TurboBusRuntimeSession`, keep daemon-first intent
submission, and make worker/backend completion remain the source of completed
receipts.

## Completed This Round

- Split the old `client_transfer.py` responsibilities into focused runtime,
  intent execution, direct fallback, buffer registration, worker-managed, and
  shared execution modules.
- Removed the old `client_transfer.py` compatibility export layer and updated
  imports to the new owning modules.
- Added `TurboBusRuntimeSession` as the public session/job/buffer/intent entry
  point for H2D and D2H transfers.
- Exported `TurboBusRuntimeSession` from the package root.
- Replaced old progress text that still pointed at paper-validation and
  correctness-gate work.

## Validation

- `python -m py_compile turbobus/buffer_registration.py turbobus/direct_fallback.py turbobus/intent_executor.py turbobus/runtime_session.py turbobus/transfer_execution.py turbobus/worker_managed.py turbobus/__init__.py test/python/integration/test_client_worker_transfer.py test/python/integration/test_paper_main_path.py`
  passed.
- `python -c "from turbobus import TurboBusRuntimeSession; from turbobus.intent_executor import WorkerIntentTransferExecutor; from turbobus.worker_managed import make_worker_managed_transfer_client; print('imports ok')"`
  passed.
- `git diff --check` passed with Windows line-ending warnings only.

## Remaining Risk

- The new runtime session has not yet been validated on a CUDA multi-GPU
  server.
- Profile bootstrap is still not wired into the runtime session.
- Worker socket and daemon socket production startup still need a later pass.

## Next Main Target

After this split is checked and committed, continue with profile bootstrap:
collect or install CUDA bandwidth profile data and submit it to daemon
`put_profile` before scheduling.
