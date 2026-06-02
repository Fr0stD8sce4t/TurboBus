# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The system-level runtime path submits `TransferIntent`, uses daemon scheduling,
issues `ExecutionTicket` plans, and keeps the old `client_transfer.py` file
deleted. Daemon planning now uses one daemon-owned topology snapshot for both
the scheduling snapshot id and relay eligibility, and scheduler decisions carry
topology metadata beside runtime load metadata.

## Completed This Round

- Reservation cleanup now validates authenticated peer ownership against stale
  staging records when the lease/reservation record is already gone.
- Orphan staging records are now actually removed by reservation cleanup rather
  than only being counted in the removed summary.
- Stale staging cleanup records audit and system cleanup events with the
  original transfer, session, job, buffer, and ticket context when available.

## Validation

- `python -m py_compile turbobus\daemon\server.py` passed.
- `python -m unittest test.python.integration.test_daemon_state` passed.
- `python -m unittest test.python.integration.test_worker_helper` passed.
- `python -m unittest test.python.integration.test_client_worker_transfer`
  passed with one expected platform skip.

## Remaining Risk

- Real CUDA/native multi-GPU execution has not been validated in this local
  environment.
- Worker cleanup coordination after authorization and status-report failures
  still needs a focused pass across daemon and worker socket execution.

## Next Main Target

Tighten cross-job isolation and daemon authority in the daemon/worker
production path.
