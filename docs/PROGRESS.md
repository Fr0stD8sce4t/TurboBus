# TurboBus Progress

## Current State

- The project is in code-function implementation. Functional validation,
  benchmark runs, examples, paper validation, server validation, new tests, mock
  gates, fake evidence, synthetic evidence, and dry-run deliverables remain
  deferred.
- G1 through G35 are complete.
- G35 runtime feedback strengthening is present: CUDA worker completion now
  carries executor runtime feedback into completion evidence, daemon runtime
  metrics aggregate worker executor cache/inflight/timing state, and scheduler
  load feedback folds those metrics into worker pressure for future planning.
- Auto-advance continues with G36 as the only active target.

## Remaining Risk

- G36 multi-tenant isolation strengthening is not complete: shared relay
  execution still needs tighter ownership binding across worker authorization,
  tickets, leases, cleanup, and peer identity.
- Functional validation, server validation, benchmark execution, paper
  validation, and multi-GPU execution remain deferred.

## Next Main Target

G36 multi-tenant isolation strengthening.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
