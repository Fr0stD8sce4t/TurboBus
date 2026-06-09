# TurboBus Progress

## Current State

- The project is in code-function implementation. Functional validation,
  benchmark runs, examples, paper validation, server validation, new tests, mock
  gates, fake evidence, synthetic evidence, and dry-run deliverables remain
  deferred.
- G1 through G46 are complete.
- G46 buffer lifecycle pooling is present: `TurboBusRuntimeSession` records
  shared pinned CPU and CUDA IPC buffer registration, active intent use,
  terminal receipt use, cleanup, close, and runtime retention evidence.
- Auto-advance is active. The current main target is G47 multi-tenant fairness
  admission.

## Remaining Risk

- Functional validation, server validation, benchmark execution, paper
  validation, and multi-GPU execution remain deferred and were not run in this
  code-function queue.
- G47 still needs daemon admission to account for multi-tenant queued/running
  transfer load, active leases, buffer ownership, and fair accept/queue/reject
  evidence.

## Next Main Target

G47 multi-tenant fairness admission.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
