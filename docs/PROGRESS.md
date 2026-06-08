# TurboBus Progress

## Current State

- The project is in code-function implementation. Functional validation,
  benchmark runs, examples, paper validation, server validation, new tests, mock
  gates, fake evidence, synthetic evidence, and dry-run deliverables remain
  deferred.
- G1 through G40 are complete.
- G40 validation code entry recovery is present: runtime evidence validation
  now exposes code entries that consume only real `TransferReceipt` objects or
  adapter lifecycle receipt contracts, reject fake/synthetic/dry-run evidence,
  and return validation summaries without running functional validation.
- Auto-advance continues with G41 as the only active target.

## Remaining Risk

- G41 benchmark code recovery is not complete: benchmark-facing code entries
  still need to consume only production receipts and runtime evidence without
  creating synthetic evidence, fake receipts, dry-run deliverables, or running
  benchmark execution.
- Functional validation, server validation, benchmark execution, paper
  validation, and multi-GPU execution remain deferred.

## Next Main Target

G41 benchmark code recovery.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
