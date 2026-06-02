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
request session-wide cleanup. Reservation release for intent transfers now
rechecks stored completion evidence against the current daemon-issued ticket.

## Completed This Round

- `release_transfer()` now rejects reservation release for evidence-required
  intent transfers unless stored completion evidence is present and still
  matches the current daemon ticket.
- Idempotent complete status updates for evidence-required transfers also
  recheck the stored evidence before returning success.

## Validation

- `python -m py_compile turbobus\daemon\server.py turbobus\daemon\dispatch.py turbobus\daemon\client.py` passed.
- `python -m unittest test.python.integration.test_daemon_state` passed.
- `python -m unittest test.python.integration.test_client_worker_transfer`
  passed with one expected platform skip.
- `python -m unittest test.python.integration.test_daemon_socket` passed with
  expected platform skips.

## Remaining Risk

- Real CUDA/native multi-GPU execution has not been validated in this local
  environment.
- Daemon and worker production startup paths still need a focused pass for
  socket permissions and peer identity assumptions.

## Next Main Target

Tighten cross-job isolation and daemon authority in the daemon/worker
production path.
