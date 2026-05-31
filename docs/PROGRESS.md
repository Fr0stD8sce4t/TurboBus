# TurboBus Progress

## Current State

Active plan: `docs/NEXT_STEPS.md`.

Current main target: intent-to-worker execution loop.

## Completed This Round

- Added a daemon guard so intent-backed transfers cannot be marked complete
  without worker/backend execution evidence.
- Updated worker status reporting to identify worker completion.
- Updated direct daemon-ticketed backend fallback to identify backend
  completion.
- Added receipt metadata for `completion_source` and `executed`.
- Updated integration tests so intent-only completion is rejected and
  worker-backed completion produces a completed receipt.

## Validation

- `python -m unittest test.python.integration.test_paper_main_path`
- `python -m unittest test.python.integration.test_client_worker_transfer`
- `python -m unittest test.python.integration.test_worker_helper.WorkerHelperTest.test_status_reporter_maps_complete_to_daemon_complete_status test.python.integration.test_worker_helper.WorkerHelperTest.test_status_reporter_maps_failed_to_daemon_failed_status`
- `python -m unittest test.python.integration.test_daemon_socket`

All listed tests passed locally.

## Remaining Risk

- Public `TurboBusClient.submit_transfer_intent` still returns the initial
  daemon receipt; the next code step must connect public intent submission to
  worker/backend execution for executable H2D and D2H buffers.
- CUDA, native extension, and real vLLM workload execution were not run in this
  local Windows environment.

## Next Main Target

Continue intent-to-worker execution loop. Do not move to the real buffer
correctness gate until public intent execution is closed.
