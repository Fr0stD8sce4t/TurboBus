# TurboBus Progress

## Current State

- The project is in code-function implementation. Functional validation,
  benchmark runs, examples, paper validation, server validation, new tests, mock
  gates, fake evidence, synthetic evidence, and dry-run deliverables remain
  deferred.
- G1 through G50 are complete.
- G50 weight loading deep integration is present: model weight loading consumes
  TransferReceipt plus daemon recovery evidence for H2D weight movement through
  `TurboBusRuntimeSession` without exposing physical route choices.
- Auto-advance is active. The current main target is G51 training state deep
  integration.

## Remaining Risk

- Functional validation, server validation, benchmark execution, paper
  validation, and multi-GPU execution remain deferred and were not run in this
  code-function queue.
- G51 still needs training-state offload to use real `TurboBusRuntimeSession`
  buffers, submit H2D/D2H TransferIntent, and consume TransferReceipt plus
  daemon recovery evidence without adapter route selection.

## Next Main Target

G51 training state deep integration.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
