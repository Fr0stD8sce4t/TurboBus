# TurboBus Progress

## Current State

- The project is in code-function implementation. Functional validation,
  benchmark runs, examples, paper validation, server validation, new tests, mock
  gates, fake evidence, synthetic evidence, and dry-run deliverables remain
  deferred.
- G1 through G51 are complete.
- G51 training-state deep integration is present: training offload consumes
  TransferReceipt plus daemon recovery evidence for H2D/D2H movement through
  `TurboBusRuntimeSession` without exposing physical route choices.
- Auto-advance is active. The current main target is G52 topology and fabric
  abstraction hardening.

## Remaining Risk

- Functional validation, server validation, benchmark execution, paper
  validation, and multi-GPU execution remain deferred and were not run in this
  code-function queue.
- G52 still needs topology and fabric abstraction to carry daemon-discovered
  PCIe, NUMA, and scale-up fabric capabilities into scheduler/profile metadata
  without synthetic production topology.

## Next Main Target

G52 topology and fabric abstraction hardening.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
