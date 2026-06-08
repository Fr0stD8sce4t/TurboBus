# TurboBus Progress

## Current State

- The project is in code-function implementation. Functional validation,
  benchmark runs, examples, paper validation, server validation, new tests, mock
  gates, fake evidence, synthetic evidence, and dry-run deliverables remain
  deferred.
- G1 through G32 are complete.
- G31 production execution entry convergence is present: production transfer
  submission, worker/backend execution, terminal receipt wait, active-intent
  cleanup, and real-execution validation now return through
  `TurboBusRuntimeSession` finalization, while standalone helper receipt waits
  and public worker executor factories no longer expose production-looking
  bypasses.
- G32 CUDA mixed pooled strengthening is present: worker CUDA completion now
  canonicalizes success evidence around daemon exact-plan path split, verifies
  native direct/relay byte and chunk accounting against the daemon plan, and
  preserves per-relay device evidence even when only daemon-plan path stats are
  available.
- Auto-advance continues with G33 as the only active target.

## Remaining Risk

- G33 profile collection and daemon import closure is not complete: production
  profile measurement records still need a tighter code path into daemon profile
  import and scheduler consumption without treating synthetic profile data as
  production evidence.
- Functional validation, server validation, benchmark execution, paper
  validation, and multi-GPU execution remain deferred.

## Next Main Target

G33 profile collection and daemon import closure.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
