# TurboBus Progress

## Current State

Current main target: real buffer correctness gate.

The daemon, worker, backend, and public intent paths require completion
evidence for completed intent transfers. Native direct H2D and D2H CUDA
readback have been validated on the server. Public intent backend and
relay/pool CUDA validation still need server runs.

## Completed This Round

- Propagated receipt execution and verification evidence into model-loading and
  training-offload benchmark summaries.
- Made paper validation require `executed`, `verified`, matching
  `verified_bytes`, and `content_match` before reporting correctness as
  complete.
- Updated benchmark tests so fixture receipts represent executed and verified
  receipts instead of intent-only completion.

## Validation

- `python -m unittest test.python.e2e.test_model_loading_benchmark test.python.e2e.test_training_offload_benchmark test.python.e2e.test_benchmark_daemon_support test.python.e2e.test_paper_validation`
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
