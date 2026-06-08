# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G27 are complete.
- G27 model loading real integration closure is present: model-load lifecycle
  evidence now binds manifest tensors, bucket ranges, runtime-session CPU/GPU
  buffers, receipt ids, intent ids, ticket ids, completion contracts, byte
  counts, and path split evidence while keeping physical route policy owned by
  the daemon scheduler.
- Auto-advance continues with G28 as the only active target.

## Remaining Risk

- G28 training offload real integration closure is not complete: training-state
  prefetch/offload still needs a stable lifecycle that proves real
  runtime-session buffer registration, H2D/D2H TransferIntent submission,
  TransferReceipt consumption, receipt trace aggregation, direction, and cleanup
  ownership without exposing physical route policy to the adapter.
- End-to-end CUDA, vLLM, multi-GPU, server, benchmark, and paper-validation
  evidence remains deferred. Future validation work must use real executed
  daemon/worker/backend evidence, not fake receipts, synthetic evidence, JSON
  artifacts, or dry-run output.

## Next Main Target

G28 training offload real integration closure.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
