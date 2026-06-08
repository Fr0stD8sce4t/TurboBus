# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G12 are complete.
- G12 admission priority queue is present: delayed relay transfers are ordered
  by daemon-owned priority score using request priority, wait time, bytes,
  requested chunks, job pressure, and runtime readiness; promotion results and
  delayed queue state carry priority evidence into admission refresh responses
  and runtime queue records.
- Auto-advance continues with G13 as the only active target.

## Remaining Risk

- G13 runtime feedback metrics closure is not complete: worker/backend
  completion metrics, async worker pool evidence, cleanup evidence, CUDA IPC
  span validation, and terminal feedback still need a unified
  scheduler-consumable runtime view.
- Framework adapters and final auditable receipt closure remain later-stage
  work.
- Alternative verification paths, fake receipts, synthetic evidence, benchmark
  work, and dry-run deliverables remain out of scope for the current
  system-body pass.

## Next Main Target

G13 runtime feedback metrics closure.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
