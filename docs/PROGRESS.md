# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G26 are complete.
- G26 vLLM real lifecycle closure is present: vLLM KV save/restore lifecycle
  evidence now binds request ids, block ids, range refs, runtime-session buffer
  bindings, receipt ids, intent ids, ticket ids, completion contracts, byte
  counts, and path split evidence while keeping physical route policy owned by
  the daemon scheduler.
- Auto-advance continues with G27 as the only active target.

## Remaining Risk

- G27 model loading real integration closure is not complete: model manifests
  and tensor buckets still need a stable lifecycle that proves real
  runtime-session buffer registration, TransferIntent submission,
  TransferReceipt consumption, receipt trace aggregation, and cleanup ownership
  without exposing physical route policy to the adapter.
- End-to-end CUDA, vLLM, multi-GPU, server, benchmark, and paper-validation
  evidence remains deferred. Future validation work must use real executed
  daemon/worker/backend evidence, not fake receipts, synthetic evidence, JSON
  artifacts, or dry-run output.

## Next Main Target

G27 model loading real integration closure.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
