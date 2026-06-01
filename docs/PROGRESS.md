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
the public intent path, and the old example-side physical GPU mapping helper
has also been removed. Native direct H2D and D2H CUDA readback have been
validated on the server. Public intent backend and relay/pool CUDA validation
still need server runs.

## Completed This Round

- Removed `turbobus.example_config`, which let examples construct physical
  target/relay GPU runtime mappings.
- Removed the obsolete unit test for that helper.
- Confirmed examples, benchmarks, and adapters do not expose physical path
  selection arguments.

## Validation

- `python -m unittest test.python.unit.test_package_boundaries test.python.e2e.test_model_loading_benchmark test.python.e2e.test_training_offload_benchmark`
  passed.
- `rg -n "example_config|configure_cuda_runtime_mapping|parse_gpu_list" turbobus examples benchmarks test\python -g "*.py"` found no active code references.
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
