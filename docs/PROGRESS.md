# TurboBus Progress

## Current State

Current main target: real buffer correctness gate.

The daemon, worker, backend, public client, adapter support, benchmarks, and
paper-validation paths reject completed intent transfers that lack execution
and verified-byte evidence. Complete `TransferReceipt` construction now also
requires worker/backend source, executed bytes, verified bytes, and
content-match evidence. Direct backend and worker CUDA execution require
backend `verify_transfer` instead of stats-only evidence. Native direct H2D and
D2H CUDA readback have been validated on the server. Public intent backend and
relay/pool CUDA validation still need server runs.

## Completed This Round

- Removed stats-only completion evidence fallback from direct backend execution.
- Removed stats-only completion evidence fallback from worker CUDA execution.
- Updated existing backend fixtures to model `verify_transfer` explicitly.

## Validation

- `python -m unittest test.python.integration.test_client_worker_transfer test.python.unit.test_worker_cuda_executor`
  passed.
- `python -m unittest test.python.unit.test_contract_schema test.python.unit.test_public_client_api test.python.integration.test_paper_main_path test.python.e2e.test_model_loading_benchmark test.python.e2e.test_training_offload_benchmark test.python.e2e.test_paper_validation`
  passed.
- `git diff --check` passed.

## Remaining Risk

- Public intent backend H2D/D2H correctness still needs real CUDA server
  validation.
- Worker relay and pooled correctness still need real CUDA validation on a P2P
  capable GPU pair.

## Next Main Target

Continue the real buffer correctness gate until public intent backend,
worker-relay, and pooled paths all prove executed and verified bytes on real
CUDA buffers.
