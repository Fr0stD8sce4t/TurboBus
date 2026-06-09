# TurboBus Progress

## Current State

- The project is in code-function implementation. Functional validation,
  benchmark runs, examples, paper validation, server validation, new tests, mock
  gates, fake evidence, synthetic evidence, and dry-run deliverables remain
  deferred.
- G1 through G48 are complete.
- G48 daemon state recovery is present: daemon recovery exposes authoritative
  transfer state covering terminal receipt, admission, queue, ticket, lease,
  buffer, cleanup, completion, and archived evidence without synthetic
  receipts.
- Auto-advance is active. The current main target is G49 vLLM KV deep
  integration.

## Remaining Risk

- Functional validation, server validation, benchmark execution, paper
  validation, and multi-GPU execution remain deferred and were not run in this
  code-function queue.
- G49 still needs vLLM KV integration to use real `TurboBusRuntimeSession`
  buffers, submit TransferIntent, and consume TransferReceipt without adapter
  route selection.

## Next Main Target

G49 vLLM KV deep integration.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
