# TurboBus Progress

## Current State

Active plan: `docs/NEXT_STEPS.md`.

Current main target: real buffer correctness gate.

## Completed This Round

- Added a daemon receipt/status gate for intent-backed completions: complete
  now requires worker/backend source plus verified byte evidence.
- Propagated verification evidence through public worker intent completion and
  direct backend fallback receipts.
- Added local coverage for missing verification evidence and content mismatch
  rejection.

## Validation

- `python -m unittest test.python.integration.test_paper_main_path`
  passed: 15 tests.
- `python -m unittest test.python.integration.test_client_worker_transfer`
  passed: 35 tests, 1 skipped.
- `python -m unittest test.python.integration.test_daemon_state.DaemonStateTest.test_submit_transfer_intent_returns_ticketed_receipt test.python.integration.test_daemon_socket`
  passed: 24 tests, 17 skipped.
- `python -m unittest test.python.integration.test_worker_helper.WorkerHelperTest.test_status_reporter_maps_complete_to_daemon_complete_status test.python.integration.test_worker_helper.WorkerHelperTest.test_status_reporter_maps_failed_to_daemon_failed_status test.python.unit.test_worker_cuda_executor`
  passed: 12 tests.
- `python -m py_compile turbobus\daemon\server.py turbobus\client_transfer.py turbobus\worker\lifecycle.py turbobus\worker\cuda_executor.py test\python\integration\test_paper_main_path.py`
  passed.
- `git diff --check`
  passed with local CRLF warnings only.

## Remaining Risk

- Native CUDA/readback verification still needs implementation and server
  validation.
- Real vLLM workload execution has not been run in this local Windows
  environment.

## Next Main Target

Continue the real buffer correctness gate by adding native CUDA/readback
evidence for public intent transfers.
