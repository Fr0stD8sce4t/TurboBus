# TurboBus Progress

## Current State

Active plan: `docs/NEXT_STEPS.md`.

Current main target: intent-to-worker execution loop.

## Completed This Round

- Added admission validation to public intent execution so delayed or expired
  plans are rejected before worker execution.
- Added public-intent tests for delayed admission, expired plans, worker
  failure cleanup, H2D execution, and D2H execution.
- Verified failure cleanup clears reservations, staging records, relay quota,
  and leaves a failed receipt.

## Validation

- `python -m unittest test.python.integration.test_paper_main_path`
- `python -m unittest test.python.integration.test_client_worker_transfer test.python.unit.test_public_client_api`
- `python -m unittest test.python.integration.test_worker_helper.WorkerHelperTest.test_status_reporter_maps_complete_to_daemon_complete_status test.python.integration.test_worker_helper.WorkerHelperTest.test_status_reporter_maps_failed_to_daemon_failed_status test.python.integration.test_daemon_socket`
- `python -m py_compile turbobus\client_transfer.py test\python\integration\test_paper_main_path.py`

All listed tests passed locally.

## Remaining Risk

- Timeout or stale-session cleanup still needs explicit public-intent coverage.
- CUDA, native extension, and real vLLM workload execution were not run in this
  local Windows environment.

## Next Main Target

Continue intent-to-worker execution loop. Do not move to the real buffer
correctness gate until timeout cleanup is covered for public-intent execution.
