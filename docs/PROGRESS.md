# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The system-level runtime path submits `TransferIntent`, uses daemon scheduling,
issues `ExecutionTicket` plans, and keeps the old `client_transfer.py` file
deleted. Daemon planning now uses one daemon-owned topology snapshot for both
the scheduling snapshot id and relay eligibility, and scheduler decisions carry
topology metadata beside runtime load metadata.

## Completed This Round

- `RESERVE_TRANSFER` dispatch now passes the daemon-authenticated socket peer
  identity into relay reservation handling.
- Direct `reserve_transfer()` now enforces authenticated session ownership
  before consuming relay quota for a session.
- Existing direct daemon calls without an authenticated peer remain unchanged.

## Validation

- `python -m py_compile turbobus\daemon\server.py turbobus\daemon\dispatch.py`
  passed.
- `python -m unittest test.python.integration.test_daemon_state` passed.
- `python -m unittest test.python.integration.test_daemon_socket` passed with
  platform skips.
- `python -m unittest test.python.integration.test_worker_helper` passed.

## Remaining Risk

- Real CUDA/native multi-GPU execution has not been validated in this local
  environment.
- Peer stale worker cleanup ownership still needs a focused pass
  across daemon and worker socket execution.

## Next Main Target

Tighten cross-job isolation and daemon authority in the daemon/worker
production path.
