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
`ExecutionTicket`, including failed and canceled reports.

## Completed This Round

- Required current-ticket evidence for external intent transfer status updates
  beyond read-only queries.
- Kept complete reports on verified byte evidence and added ticket-bound
  evidence for failed/canceled worker or backend reports.
- Worker lifecycle failure results and direct fallback failure reports now carry
  daemon ticket binding when a ticket exists.

## Validation

- `python -m py_compile turbobus\daemon\server.py turbobus\worker\lifecycle.py turbobus\direct_fallback.py` passed.
- `python -m unittest test.python.integration.test_client_worker_transfer`
  passed with one expected skip.
- `python -m unittest test.python.integration.test_daemon_state` passed.
- `python -m unittest test.python.integration.test_daemon_socket` passed with
  expected platform skips.
- `git diff --check` passed.

## Remaining Risk

- Real CUDA/native multi-GPU execution has not been validated in this local
  environment.
- The current production peer credential implementation is Linux `SO_PEERCRED`
  only; unsupported platforms now fail closed instead of weakening isolation.
- Session/job/buffer cleanup still needs a focused pass to confirm ticket and
  staging records cannot survive into cross-job state.

## Next Main Target

Tighten cross-job isolation and daemon authority in the daemon/worker
production path.
