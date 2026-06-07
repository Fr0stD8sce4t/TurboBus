# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G7 are complete.
- G8 buffer ownership lifecycle closure is present: daemon buffer registration
  returns ownership ledger evidence, daemon `describe()` exposes registered
  buffer ownership, transfer buffer snapshots freeze daemon ownership state,
  and buffer cleanup is blocked while active daemon-issued leases or execution
  tickets still protect the buffer.
- Auto-advance continues with G9 as the only active target.

## Remaining Risk

- G9 CUDA IPC metadata and span validation is not complete: worker-side GPU IPC
  opens still need production allocation-span metadata and pre-submit range
  rejection.
- Worker pool, scheduler cost model, admission priority queue, runtime feedback
  metrics, framework adapters, and final server validation remain later-stage
  work.
- Alternative verification paths, fake receipts, synthetic evidence, benchmark
  work, and dry-run deliverables remain out of scope for the current
  system-body pass.

## Next Main Target

G9 CUDA IPC metadata and span validation.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
