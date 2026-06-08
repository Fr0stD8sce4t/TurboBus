# TurboBus Progress

## Current State

- The project is in code-function implementation. Functional validation,
  benchmark runs, examples, paper validation, server validation, new tests, mock
  gates, fake evidence, synthetic evidence, and dry-run deliverables remain
  deferred.
- G1 through G33 are complete.
- G33 profile collection and daemon import closure is present: backend profile
  results now normalize into direct PCIe, relay PCIe, and GPU fabric measurement
  records, daemon import stores topology-bound production profile metadata, and
  scheduler cost-model metadata exposes imported measurement coverage.
- Auto-advance continues with G34 as the only active target.

## Remaining Risk

- G34 scheduler cost model strengthening is not complete: scheduler scoring
  still needs a tighter cost-model path that combines imported profile
  measurements, runtime pressure, relay admission state, and workload priority.
- Functional validation, server validation, benchmark execution, paper
  validation, and multi-GPU execution remain deferred.

## Next Main Target

G34 scheduler cost model strengthening.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
