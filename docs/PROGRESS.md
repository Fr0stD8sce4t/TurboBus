# TurboBus Progress

## Current State

- The project is in code-function implementation. Functional validation,
  benchmark runs, examples, paper validation, server validation, new tests, mock
  gates, fake evidence, synthetic evidence, and dry-run deliverables remain
  deferred.
- G1 through G39 are complete.
- G39 training offload code integration strengthening is present: training
  offload metadata now rejects physical route selection, and synchronous plus
  asynchronous H2D/D2H training-state paths record runtime-session submission,
  `ReceiptTransferHandle`, and `TransferReceipt` lifecycle evidence.
- Auto-advance continues with G40 as the only active target.

## Remaining Risk

- G40 validation code entry recovery is not complete: validation-facing code
  entries still need to consume only real production receipts and completion
  evidence without creating synthetic evidence, fake receipts, dry-run
  deliverables, or running functional validation.
- Functional validation, server validation, benchmark execution, paper
  validation, and multi-GPU execution remain deferred.

## Next Main Target

G40 validation code entry recovery.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
