# TurboBus Progress

## Current State

- The project is in code-function implementation. Functional validation,
  benchmark runs, examples, paper validation, server validation, new tests, mock
  gates, fake evidence, synthetic evidence, and dry-run deliverables remain
  deferred.
- G1 through G41 are complete.
- G41 benchmark code recovery is present: benchmark-facing code now consumes
  production `TransferReceipt` objects through runtime evidence validation,
  formats receipt traces without reconstructing receipts from JSON-like traces,
  and avoids synthetic evidence, fake receipts, and dry-run deliverables.
- Auto-advance continues with G42 as the only active target.

## Remaining Risk

- G42 paper report code recovery is not complete: report-facing code entries
  still need to consume only production benchmark summaries and runtime
  evidence without creating synthetic evidence, fake receipts, dry-run
  deliverables, or running paper validation.
- Functional validation, server validation, benchmark execution, paper
  validation, and multi-GPU execution remain deferred.

## Next Main Target

G42 paper report code recovery.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
