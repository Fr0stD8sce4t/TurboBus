# TurboBus Progress

## Current State

Active plan: `docs/NEXT_STEPS.md`.

Current main target: real buffer correctness gate.

## Completed This Round

- Added native CUDA readback comparison entry point `verify_transfer`.
- Wired `CudaNativeBackend.verify_transfer` into public direct backend fallback
  and worker CUDA executor completion evidence.
- Kept daemon receipt completion gated on verified byte evidence.

## Validation

- `python -m unittest test.python.unit.test_backend_cuda test.python.unit.test_worker_cuda_executor`
  passed: 22 tests.
- `python -m unittest test.python.integration.test_paper_main_path test.python.integration.test_client_worker_transfer`
  passed: 50 tests, 1 skipped.
- `python -m unittest test.python.integration.test_daemon_state.DaemonStateTest.test_submit_transfer_intent_returns_ticketed_receipt test.python.integration.test_daemon_socket`
  passed: 24 tests, 17 skipped.
- `python -m unittest test.python.integration.test_worker_helper.WorkerHelperTest.test_status_reporter_maps_complete_to_daemon_complete_status test.python.integration.test_worker_helper.WorkerHelperTest.test_status_reporter_maps_failed_to_daemon_failed_status`
  passed: 2 tests.
- `python -m py_compile turbobus\backends\cuda.py turbobus\backends\base.py turbobus\client_transfer.py turbobus\worker\cuda_executor.py test\python\unit\test_backend_cuda.py test\python\unit\test_worker_cuda_executor.py test\python\integration\test_client_worker_transfer.py test\python\integration\test_paper_main_path.py`
  passed.
- `git diff --check`
  passed with local CRLF warnings only.

## Remaining Risk

- Native CUDA extension build was not run locally because `nvcc`, `cmake`, and
  `pybind11` are unavailable in this Windows environment.
- Real GPU H2D/D2H public intent correctness still needs server validation.
- Real vLLM workload execution has not been run.

## Next Main Target

Continue the real buffer correctness gate by running the native CUDA build and
real public intent H2D/D2H correctness checks on a CUDA server.
