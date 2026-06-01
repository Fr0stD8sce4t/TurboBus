# TurboBus Progress

## Current State

Current main target: real buffer correctness gate.

The daemon, worker, backend, public client, adapter support, benchmarks, and
paper-validation paths reject completed intent transfers that lack execution
and verified-byte evidence. Complete `TransferReceipt` construction now also
requires worker/backend source, executed bytes, verified bytes, and
content-match evidence. Direct backend and worker CUDA execution require
backend `verify_transfer` instead of stats-only evidence. The old manual
helper-socket verification CLI has been removed so active validation stays on
the public intent path. Native direct H2D and D2H CUDA readback have been
validated on the server. Public intent backend and relay/pool CUDA validation
still need server runs.

## Completed This Round

- Removed `turbobus.verification`, which exposed manual mode, target GPU, and
  relay GPU selection through the old worker-managed route.
- Confirmed the top-level package does not export the old worker-managed
  transfer client.

## Validation

- `python -m unittest test.python.unit.test_package_boundaries`
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
