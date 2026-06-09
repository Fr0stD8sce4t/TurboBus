# TurboBus Progress

## Current State

- The project is in code-function implementation. Functional validation,
  benchmark runs, examples, paper validation, server validation, new tests, mock
  gates, fake evidence, synthetic evidence, and dry-run deliverables remain
  deferred.
- G1 through G44 are complete.
- G44 adaptive scheduling policy is present: daemon runtime telemetry now feeds
  direct, relay, and mixed pooled scheduler weights, and decisions carry
  adaptive policy metadata.
- Auto-advance is active. The current main target is G45 worker resident async
  execution.

## Remaining Risk

- Functional validation, server validation, benchmark execution, paper
  validation, and multi-GPU execution remain deferred and were not run in this
  code-function queue.
- G45 still needs worker-side resident async execution hardening for submit,
  wait, cancellation/failure cleanup, and evidence reporting.

## Next Main Target

G45 worker resident async execution.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
