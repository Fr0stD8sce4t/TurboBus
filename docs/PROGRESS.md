# TurboBus Progress

## Current State

Current main target: real buffer correctness gate.

The daemon, worker, backend, public client, adapter support, benchmarks, and
paper-validation paths reject completed intent transfers that lack execution
and verified-byte evidence. Native direct H2D and D2H CUDA readback have been
validated on the server. Public intent backend and relay/pool CUDA validation
still need server runs.

## Completed This Round

- Moved complete-receipt evidence validation into a shared API helper.
- Applied the same gate to `OffloadStore` submit/wait and vLLM receipt tracing
  so adapter boundaries cannot consume intent-only complete receipts.
- Updated adapter tests so normal fixtures carry verified evidence and negative
  cases assert rejection.

## Validation

- `python -m unittest test.python.unit.test_offload_store test.python.unit.test_public_client_api test.python.unit.test_vllm_kv_connector_main_path test.python.unit.test_package_boundaries test.python.integration.test_paper_main_path`
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
