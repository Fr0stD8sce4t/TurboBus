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
Daemon and worker socket servers now create owner-only Unix socket files on
POSIX platforms and refuse to unlink non-socket paths during startup.

## Completed This Round

- Added shared socket security helpers for daemon and worker startup.
- Daemon `serve_forever()` and `reserve_socket()` now unlink only stale socket
  paths and set socket permissions to owner-only on POSIX platforms.
- Worker socket transport now uses the same stale-socket and permission
  handling.

## Validation

- `python -m py_compile turbobus\socket_security.py turbobus\daemon\server.py turbobus\worker\transport.py turbobus\worker\process.py turbobus\daemon\__main__.py` passed.
- `python -m unittest test.python.integration.test_daemon_socket` passed with
  expected platform skips.
- `python -m unittest test.python.integration.test_worker_transport` passed
  with one expected platform skip.
- `python -m unittest test.python.integration.test_worker_process` passed with
  one expected platform skip.

## Remaining Risk

- Real CUDA/native multi-GPU execution has not been validated in this local
  environment.
- Production daemon behavior on platforms without authenticated Unix peer
  credentials still needs a focused pass.

## Next Main Target

Tighten cross-job isolation and daemon authority in the daemon/worker
production path.
