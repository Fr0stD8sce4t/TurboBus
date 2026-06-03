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
cleanup or external status reporting. Worker service and production process
entry points now route requests through the standard lifecycle. The old
`turbobus/worker/helper.py` export layer has been removed. Server-only
validation is now deferred until after the system implementation pass, so it no
longer blocks code work in this stage. The old `turbobus/daemon/protocol.py`
export layer has also been removed, and daemon imports now point at the real
owning modules.

## Completed This Round

- Removed the old `turbobus/daemon/protocol.py` pure export layer.
- Updated daemon modules to import protocol dataclasses directly from
  `turbobus.schema`.
- Updated integration imports that still referenced the old daemon protocol
  module.

## Validation

- `python -m py_compile turbobus\daemon\client.py
  turbobus\daemon\dispatch.py turbobus\daemon\leases.py
  turbobus\daemon\receipts.py turbobus\daemon\server.py
  turbobus\daemon\startup.py turbobus\daemon\__main__.py` passed.
- `python -m unittest test.python.integration.test_daemon_state` passed.
- `python -m unittest test.python.integration.test_daemon_socket` passed with
  expected platform skips.
- `git diff --check` passed with only existing CRLF conversion warnings.

## Remaining Risk

- Server-only daemon/worker socket behavior, CUDA/native execution, and
  relay/pooled execution remain deferred until the full system implementation
  pass is complete.

## Next Main Target

Tighten cross-job isolation and daemon authority in the daemon/worker
production path while continuing code-first system implementation and keeping
server validation deferred.
