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
`turbobus/worker/helper.py` export layer has been removed. Linux server
startup validation confirmed that production daemon and worker socket services
can start and listen with owner-only socket files while limited to the idle
GPU0/GPU1 startup-validation scope.

## Completed This Round

- Inspected the existing daemon and worker socket protocols for a minimal
  production request validation path.
- Selected `TurboBusDaemonClient.get_inventory()` as the daemon control-plane
  request because it uses the existing daemon socket client and does not submit
  transfers.
- Selected `WorkerServiceSocketClient.submit_envelope()` with an unauthorized
  worker envelope as the worker request because the existing lifecycle returns
  `authorization_failed` before staging allocation or CUDA execution.

## Validation

- Code inspection confirmed daemon `GET_INVENTORY` is routed through
  `TurboBusDaemonClient.send()` and the production daemon socket server.
- Code inspection confirmed worker authorization failure returns a completion
  envelope without a staging slot or worker result.
- The existing production services still need the selected requests run from
  the Linux CUDA server; no local substitute validation was added.

## Remaining Risk

- The selected daemon and worker socket requests have not yet been run against
  the live Linux server services.
- Relay and pooled execution still require the GPU5/GPU6 NVLink pair after it
  is idle.

## Next Main Target

Tighten cross-job isolation and daemon authority in the daemon/worker
production path by confirming an existing authenticated control-plane request
against the running production socket services.
