# TurboBus Progress

## Current State

Active plan: `docs/NEXT_STEPS.md`.

Current main target: real buffer correctness gate.

## Completed This Round

- Closed the intent-to-worker execution loop target.
- Added public-intent timeout coverage where worker authorization registers
  daemon staging state, stale-session cleanup runs before completion, and the
  final receipt is canceled instead of completed.
- Confirmed timeout cleanup clears sessions, jobs, buffers, reservations,
  staging records, and relay quota state for the public intent path.

## Validation

- `python -m unittest test.python.integration.test_paper_main_path test.python.integration.test_client_worker_transfer`
  passed: 48 tests, 1 skipped.
- `python -m unittest test.python.integration.test_daemon_state.DaemonStateTest.test_stale_session_reap_releases_staging_records_and_owner_state test.python.integration.test_daemon_state.DaemonStateTest.test_stale_session_reap_releases_reservations_and_quota test.python.integration.test_daemon_state.DaemonStateTest.test_worker_failure_status_releases_reservation_and_staging_record`
  passed: 3 tests.
- `python -m py_compile turbobus\daemon\server.py test\python\integration\test_paper_main_path.py`
  passed.

## Remaining Risk

- CUDA, native extension, and real vLLM workload execution still need server
  validation.
- Completed receipts do not yet require verified destination bytes.

## Next Main Target

Add the real buffer correctness gate for public intent execution.
