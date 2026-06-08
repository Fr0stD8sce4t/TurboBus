# TurboBus Progress

## Current State

- The project is in code-function implementation. Functional validation,
  benchmark runs, examples, paper validation, server validation, new tests, mock
  gates, fake evidence, synthetic evidence, and dry-run deliverables remain
  deferred.
- G1 through G38 are complete.
- G38 model loading code integration strengthening is present: model-weight
  loader metadata and manifest metadata now reject physical route selection,
  and both synchronous and asynchronous manifest/tensor load paths record
  runtime-session submission, `ReceiptTransferHandle`, and `TransferReceipt`
  lifecycle evidence.
- Auto-advance continues with G39 as the only active target.

## Remaining Risk

- G39 training offload code integration strengthening is not complete:
  training offload adapter paths still need tighter runtime-session-only
  H2D/D2H submission and receipt consumption without physical route exposure.
- Functional validation, server validation, benchmark execution, paper
  validation, and multi-GPU execution remain deferred.

## Next Main Target

G39 training offload code integration strengthening.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
