# TurboBus Progress

## Current State

- The project is in code-function implementation. Functional validation,
  benchmark runs, examples, paper validation, server validation, new tests, mock
  gates, fake evidence, synthetic evidence, and dry-run deliverables remain
  deferred.
- G1 through G31 are complete.
- G31 production execution entry convergence is present: production transfer
  submission, worker/backend execution, terminal receipt wait, active-intent
  cleanup, and real-execution validation now return through
  `TurboBusRuntimeSession` finalization, while standalone helper receipt waits
  and public worker executor factories no longer expose production-looking
  bypasses.
- Auto-advance continues with G32 as the only active target.

## Remaining Risk

- G32 CUDA mixed pooled strengthening is not complete: direct-only, relay-only,
  mixed direct+relay, H2D, D2H, multi-chunk, and multi-relay execution code
  still needs stronger exact-plan accounting and completion evidence cohesion.
- Functional validation, server validation, benchmark execution, paper
  validation, and multi-GPU execution remain deferred.

## Next Main Target

G32 CUDA mixed pooled strengthening.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
