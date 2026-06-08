# TurboBus Progress

## Current State

- The project is in code-function implementation. Functional validation,
  benchmark runs, examples, paper validation, server validation, new tests, mock
  gates, fake evidence, synthetic evidence, and dry-run deliverables remain
  deferred.
- G1 through G34 are complete.
- G34 scheduler cost model strengthening is present: scheduler planning now
  derives direct and relay path weights from one daemon cost model combining
  imported profile measurements, runtime pressure, relay admission state,
  workload kind, and priority, and planner output carries path-level cost
  metadata into daemon-issued plans.
- Auto-advance continues with G35 as the only active target.

## Remaining Risk

- G35 runtime feedback strengthening is not complete: daemon and worker/backend
  runtime state still need tighter feedback propagation into future scheduling
  inputs.
- Functional validation, server validation, benchmark execution, paper
  validation, and multi-GPU execution remain deferred.

## Next Main Target

G35 runtime feedback strengthening.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
