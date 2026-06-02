# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The system-level runtime path submits `TransferIntent`, uses daemon scheduling,
issues `ExecutionTicket` plans, and keeps the old `client_transfer.py` file
deleted. Daemon planning now uses one daemon-owned topology snapshot for both
the scheduling snapshot id and relay eligibility, and scheduler decisions carry
topology metadata beside runtime load metadata.

## Completed This Round

- Daemon transfer planning now captures a single topology inventory snapshot
  and reuses it for relay eligibility and the scheduling snapshot id.
- Relay eligibility now records the topology snapshot id and topology version
  that shaped planning.
- Scheduler decisions now include normalized topology metadata, including
  requested, eligible, and filtered relays, alongside runtime load policy
  metadata.

## Validation

- `python -m py_compile turbobus\scheduler\daemon.py turbobus\daemon\server.py`
  passed.
- `python -m unittest test.python.unit.test_daemon_scheduler` passed.
- `python -m unittest test.python.integration.test_daemon_state` passed.

## Remaining Risk

- Real CUDA/native multi-GPU execution has not been validated in this local
  environment.
- Peer isolation still needs a focused code pass across daemon and worker
  socket execution.

## Next Main Target

Tighten cross-job isolation and daemon authority in the daemon/worker
production path.
