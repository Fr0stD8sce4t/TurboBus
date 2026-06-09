# TurboBus Progress

## Current State

- The project is in code-function implementation. Functional validation,
  benchmark runs, examples, paper validation, server validation, new tests, mock
  gates, fake evidence, synthetic evidence, and dry-run deliverables remain
  deferred.
- G1 through G52 are complete.
- G52 topology and fabric abstraction is present: daemon-discovered PCIe, NUMA,
  and scale-up fabric capability summaries flow into scheduler and profile
  metadata for direct, relay, and mixed pooled plans without synthetic
  production topology.
- Auto-advance is active. The current main target is G53 backend extensibility
  interface.

## Remaining Risk

- Functional validation, server validation, benchmark execution, paper
  validation, and multi-GPU execution remain deferred and were not run in this
  code-function queue.
- G53 still needs a backend extensibility interface for direct, relay, and mixed
  pooled exact daemon-issued plans while keeping CUDA as the current production
  backend and deferring ROCm/HIP.

## Next Main Target

G53 backend extensibility interface.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
