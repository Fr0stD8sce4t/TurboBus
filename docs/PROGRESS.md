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

- Confirmed on the Linux CUDA server that the production daemon can start with
  `CUDA_VISIBLE_DEVICES=0,1`, `--target-gpu 0`, `--min-relays 0`, and
  `--allow-missing-fabric`.
- Confirmed the production worker socket service can start against that daemon.
- Confirmed both services create owner-only Unix socket files and do not add
  TurboBus Python processes on the busy GPU5/GPU6 NVLink pair.

## Validation

- Server command passed by entering the daemon accept loop:
  `CUDA_VISIBLE_DEVICES=0,1 python -m turbobus.daemon --socket-path
  /tmp/turbobusd-$USER.sock --target-gpu 0 --min-relays 0
  --allow-missing-fabric`.
- Server command passed by entering the worker accept loop:
  `CUDA_VISIBLE_DEVICES=0,1 python -m turbobus.worker
  --daemon-socket-path /tmp/turbobusd-$USER.sock --socket-path
  /tmp/turbobus-worker-$USER.sock`.
- Server inspection showed `srw-------` socket files at
  `/tmp/turbobusd-sdu.sock` and `/tmp/turbobus-worker-sdu.sock`.
- Server `nvidia-smi` showed no new TurboBus Python process on GPU5/GPU6.

## Remaining Risk

- This startup pass did not send socket requests or execute transfers.
- Relay and pooled execution still require the GPU5/GPU6 NVLink pair after it
  is idle.

## Next Main Target

Tighten cross-job isolation and daemon authority in the daemon/worker
production path by confirming an existing authenticated control-plane request
against the running production socket services.
