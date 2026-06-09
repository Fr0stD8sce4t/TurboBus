# TurboBus Progress

## Current State

- The project is in code-function implementation. Functional validation,
  benchmark runs, examples, paper validation, server validation, new tests, mock
  gates, fake evidence, synthetic evidence, and dry-run deliverables remain
  deferred.
- G1 through G43 are complete.
- G43 runtime telemetry model is present: daemon, socket clients, runtime view,
  and `TurboBusRuntimeSession` expose a production telemetry snapshot for queue,
  active execution, relay load, recent terminal completion, job/session state,
  and worker runtime feedback.
- Auto-advance is active. The current main target is G44 adaptive scheduling
  policy.

## Remaining Risk

- Functional validation, server validation, benchmark execution, paper
  validation, and multi-GPU execution remain deferred and were not run in this
  code-function queue.
- G44 still needs scheduler policy code that consumes the telemetry model for
  adaptive direct/relay/mixed weighting.

## Next Main Target

G44 adaptive scheduling policy.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
