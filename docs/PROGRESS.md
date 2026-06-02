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
entry points now route requests through the standard lifecycle, and the old
`turbobus/worker/helper.py` export layer has been removed.

## Completed This Round

- Repointed worker service, process, codec, socket client, CUDA executor, and
  package exports to real implementation modules.
- Deleted the old `turbobus/worker/helper.py` re-export layer.
- Confirmed worker socket service requests still enter through
  `submit_report_cleanup_lifecycle()` with reservation-scoped cleanup.

## Validation

- `python -m py_compile turbobus\worker\__init__.py turbobus\worker\codec.py turbobus\worker\endpoint.py turbobus\worker\process.py turbobus\worker\socket_client.py turbobus\worker\cuda_executor.py turbobus\worker\lifecycle.py turbobus\worker\models.py` passed.
- `python -m unittest test.python.integration.test_worker_transport` passed
  with one expected platform skip.
- `python -m unittest test.python.integration.test_worker_process` passed with
  one expected platform skip.
- `python -m unittest test.python.integration.test_client_worker_transfer`
  passed with one expected skip.
- `python -m unittest test.python.integration.test_worker_helper` passed.
- `python -m unittest test.python.unit.test_worker_cuda_executor` passed.
- `git diff --check` passed.

## Remaining Risk

- Real CUDA/native multi-GPU execution has not been validated in this local
  environment.
- Production daemon and worker socket startup still need Linux CUDA server
  validation with real Unix peer credentials and native CUDA resources.

## Next Main Target

Tighten cross-job isolation and daemon authority in the daemon/worker
production path.
