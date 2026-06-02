# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The system-level runtime path submits `TransferIntent`, uses daemon scheduling,
issues `ExecutionTicket` plans, and keeps the old `client_transfer.py` file
deleted. Daemon planning now uses one daemon-owned topology snapshot for both
the scheduling snapshot id and relay eligibility, and scheduler decisions carry
topology metadata beside runtime load metadata.

## Completed This Round

- Intent transfer completion now requires verified byte evidence bound to the
  current daemon-issued `ExecutionTicket`.
- Worker completion reporting now carries ticket id, transfer id, and plan
  generation from the authorized worker request into completion evidence.
- Direct backend fallback completion now reports the same ticket binding before
  daemon receipt completion.

## Validation

- `python -m py_compile turbobus\daemon\server.py turbobus\worker\lifecycle.py turbobus\direct_fallback.py`
  passed.
- `python -m unittest test.python.unit.test_worker_authorization test.python.integration.test_worker_helper`
  passed.
- `python -m unittest test.python.integration.test_paper_main_path` passed.
- `python -m unittest test.python.integration.test_client_worker_transfer`
  passed with one expected platform skip.

## Remaining Risk

- Real CUDA/native multi-GPU execution has not been validated in this local
  environment.
- Peer cleanup and release ownership still need a focused pass across daemon
  and worker socket execution.

## Next Main Target

Tighten cross-job isolation and daemon authority in the daemon/worker
production path.
