# TurboBus Progress

## Current State

- The project is in code-function implementation. Functional validation,
  benchmark runs, examples, paper validation, server validation, new tests, mock
  gates, fake evidence, synthetic evidence, and dry-run deliverables remain
  deferred.
- G1 through G49 are complete.
- G49 vLLM KV deep integration is present: KV save and restore consume
  TransferReceipt and daemon recovery state, preserving admission, queue,
  ticket, lease, buffer, cleanup, and completion evidence in lifecycle records
  without exposing physical route choices.
- Auto-advance is active. The current main target is G50 weight loading deep
  integration.

## Remaining Risk

- Functional validation, server validation, benchmark execution, paper
  validation, and multi-GPU execution remain deferred and were not run in this
  code-function queue.
- G50 still needs model weight loading to use real `TurboBusRuntimeSession`
  buffers, submit H2D TransferIntent, and consume TransferReceipt plus daemon
  recovery evidence without adapter route selection.

## Next Main Target

G50 weight loading deep integration.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
