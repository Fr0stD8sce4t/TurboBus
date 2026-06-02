# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The system-level runtime path submits `TransferIntent`, uses daemon scheduling,
issues `ExecutionTicket` plans, and keeps the old `client_transfer.py` file
deleted. Daemon planning now uses one daemon-owned topology snapshot for both
the scheduling snapshot id and relay eligibility, and scheduler decisions carry
topology metadata beside runtime load metadata. Worker authorization-failure
cleanup now requires daemon-issued ticket context before it can touch daemon
reservation or session state.

## Completed This Round

- Worker authorization errors now carry daemon authorization payload only when
  the daemon returned one.
- Worker cleanup after authorization failure now skips daemon cleanup when
  there is no valid daemon-issued `ExecutionTicket` payload.
- Authorization-failure cleanup uses ticket-bound lease/session ids rather than
  the raw worker authorization request when daemon cleanup is allowed.

## Validation

- `python -m py_compile turbobus\worker\lifecycle.py` passed.
- `python -m py_compile turbobus\worker\lifecycle.py turbobus\daemon\server.py turbobus\daemon\dispatch.py` passed.
- `python -m unittest test.python.integration.test_worker_helper` passed.
- `python -m unittest test.python.integration.test_client_worker_transfer`
  passed with one expected platform skip.
- `python -m unittest test.python.integration.test_daemon_socket` passed with
  expected platform skips.

## Remaining Risk

- Real CUDA/native multi-GPU execution has not been validated in this local
  environment.
- Worker socket/service boundary still needs a focused pass to ensure lifecycle
  envelopes cannot widen cleanup targets beyond daemon-issued ticket context.

## Next Main Target

Tighten cross-job isolation and daemon authority in the daemon/worker
production path.
