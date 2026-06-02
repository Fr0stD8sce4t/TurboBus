# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The system-level runtime path submits `TransferIntent`, uses daemon scheduling,
issues `ExecutionTicket` plans, and keeps the old `client_transfer.py` file
deleted. Daemon planning now uses one daemon-owned topology snapshot for both
the scheduling snapshot id and relay eligibility, and scheduler decisions carry
topology metadata beside runtime load metadata. Worker authorization-failure
cleanup now requires daemon-issued ticket context before it can touch daemon
reservation or session state. Worker socket/service envelopes can no longer
request session-wide cleanup.

## Completed This Round

- Worker service lifecycle now rejects any socket/service cleanup target other
  than `reservation`.
- Worker service request envelopes no longer allow `session` cleanup targets.
- Existing worker envelope checks now assert that socket requests cannot widen
  cleanup scope beyond a reservation.

## Validation

- `python -m py_compile turbobus\worker\models.py turbobus\worker\lifecycle.py turbobus\worker\codec.py turbobus\worker\endpoint.py turbobus\worker\process.py` passed.
- `python -m unittest test.python.integration.test_worker_helper` passed.
- `python -m unittest test.python.integration.test_client_worker_transfer`
  passed with one expected platform skip.
- `python -m unittest test.python.integration.test_daemon_socket` passed with
  expected platform skips.

## Remaining Risk

- Real CUDA/native multi-GPU execution has not been validated in this local
  environment.
- Daemon transfer status, cleanup, and release handling after worker socket
  completion still needs a focused pass for ticket-evidence binding.

## Next Main Target

Tighten cross-job isolation and daemon authority in the daemon/worker
production path.
