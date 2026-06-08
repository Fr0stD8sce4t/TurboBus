# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G9 are complete.
- G9 CUDA IPC metadata and span validation is present: CUDA IPC buffer
  registrations carry exported allocation span metadata, schema normalization
  rejects invalid device views, workers validate opened IPC spans and request
  ranges before executor submit, and runtime receipt validation recognizes span
  validation evidence.
- Auto-advance continues with G10 as the only active target.

## Remaining Risk

- G10 worker asynchronous execution pool is not complete: worker execution still
  needs a production pool lifecycle that preserves daemon-issued ticket
  authority, cleanup, status reporting, and receipt evidence across queued,
  running, completed, and failed transfers.
- Scheduler cost model, admission priority queue, runtime feedback metrics,
  framework adapters, and final server validation remain later-stage work.
- Alternative verification paths, fake receipts, synthetic evidence, benchmark
  work, and dry-run deliverables remain out of scope for the current
  system-body pass.

## Next Main Target

G10 worker asynchronous execution pool.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
