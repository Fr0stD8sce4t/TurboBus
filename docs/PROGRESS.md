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
Production daemon instances now require authenticated socket peers and refuse
to serve on platforms where the current Unix credential mechanism is
unavailable. Intent transfer status updates from external request paths now
require worker/backend execution evidence bound to the current daemon-issued
`ExecutionTicket`, including failed and canceled reports. Failed and canceled
terminal transfers now drop their daemon execution ticket mappings during
cleanup or external status reporting.

## Completed This Round

- Centralized removal of daemon execution ticket mappings for non-complete
  terminal transfers.
- Session, job, buffer, reservation, and transfer cleanup paths now cancel
  transfers without leaving stale current-ticket mappings behind.
- Complete transfers still retain their ticket mapping for receipt and release
  evidence checks.

## Validation

- `python -m py_compile turbobus\daemon\server.py` passed.
- `python -m unittest test.python.integration.test_daemon_state` passed.
- `python -m unittest test.python.integration.test_client_worker_transfer`
  passed with one expected skip.
- `python -m unittest test.python.integration.test_daemon_socket` passed with
  expected platform skips.
- `git diff --check` passed.

## Remaining Risk

- Real CUDA/native multi-GPU execution has not been validated in this local
  environment.
- The current production peer credential implementation is Linux `SO_PEERCRED`
  only; unsupported platforms now fail closed instead of weakening isolation.
- Worker service and production process paths still need a focused pass for
  lifecycle bypasses.

## Next Main Target

Tighten cross-job isolation and daemon authority in the daemon/worker
production path.
