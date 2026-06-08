# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G28 are complete.
- G28 training offload real integration closure is present: training-state
  lifecycle evidence now binds prefetch/offload direction, bucket ranges,
  runtime-session CPU/GPU buffers, receipt ids, intent ids, ticket ids,
  completion contracts, byte counts, and path split evidence while keeping
  physical route policy owned by the daemon scheduler.
- Auto-advance continues with G29 as the only active target.

## Remaining Risk

- G29 unified reproduction evidence model is not complete: direct, relay, mixed
  pooled execution, failure cleanup, buffer lifetime, CUDA IPC lifecycle, and
  adapter lifecycle evidence still need one stable receipt-level view for later
  real-execution validation.
- End-to-end CUDA, vLLM, multi-GPU, server, benchmark, and paper-validation
  evidence remains deferred. Future validation work must use real executed
  daemon/worker/backend evidence, not fake receipts, synthetic evidence, JSON
  artifacts, or dry-run output.

## Next Main Target

G29 unified reproduction evidence model.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
