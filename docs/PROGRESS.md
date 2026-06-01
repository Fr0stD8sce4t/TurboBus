# TurboBus Progress

## Current State

Current main target: real buffer correctness gate.

The daemon, worker, backend, public client, benchmarks, and paper-validation
paths reject completed intent transfers that lack execution and verified-byte
evidence. Native direct H2D and D2H CUDA readback have been validated on the
server. Public intent backend and relay/pool CUDA validation still need server
runs.

## Completed This Round

- Added a public `TurboBusClient` gate that rejects `complete` receipts without
  worker/backend source, `executed`, `verified`, matching `verified_bytes`, and
  `content_match`.
- Kept non-terminal and failed receipts pass-through so queued work and explicit
  failure still surface normally.
- Updated public client tests to cover rejection of intent-only complete
  receipts.

## Validation

- `python -m unittest test.python.unit.test_public_client_api test.python.integration.test_paper_main_path`
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
