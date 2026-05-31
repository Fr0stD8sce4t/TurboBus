# TurboBus Progress

## Current State

Active plan: `docs/NEXT_STEPS.md`.

Current main target: intent-to-worker execution loop.

## Completed This Round

- Added an optional public intent executor to `TurboBusClient`.
- Added `WorkerIntentTransferExecutor`, which executes daemon-submitted intent
  payloads using daemon-issued tickets and worker/backend completion.
- Added H2D and D2H public-intent integration coverage that proves final
  receipts carry worker execution evidence.

## Validation

- `python -m unittest test.python.integration.test_paper_main_path`
- `python -m unittest test.python.unit.test_public_client_api`
- `python -m unittest test.python.integration.test_client_worker_transfer`
- `python -m unittest test.python.integration.test_worker_helper.WorkerHelperTest.test_status_reporter_maps_complete_to_daemon_complete_status test.python.integration.test_worker_helper.WorkerHelperTest.test_status_reporter_maps_failed_to_daemon_failed_status test.python.integration.test_daemon_socket`

All listed tests passed locally.

## Remaining Risk

- Delayed and expired admission execution still need explicit public-intent
  tests before the intent-to-worker target is complete.
- CUDA, native extension, and real vLLM workload execution were not run in this
  local Windows environment.

## Next Main Target

Continue intent-to-worker execution loop. Do not move to the real buffer
correctness gate until delayed/expired admission and cleanup behavior are
closed for public-intent execution.
