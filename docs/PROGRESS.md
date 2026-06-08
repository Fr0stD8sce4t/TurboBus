# TurboBus Progress

## Current State

- The project is in code-function implementation. Functional validation,
  benchmark runs, examples, paper validation, server validation, new tests, mock
  gates, fake evidence, synthetic evidence, and dry-run deliverables remain
  deferred.
- G1 through G37 are complete.
- G37 vLLM KV code integration strengthening is present: vLLM KV request
  parameters and adapter metadata now reject physical route selection, and KV
  restore/save lifecycle evidence stays bound to runtime-session submission,
  `ReceiptTransferHandle`, and `TransferReceipt`.
- Auto-advance continues with G38 as the only active target.

## Remaining Risk

- G38 model loading code integration strengthening is not complete:
  model-loading adapter paths still need tighter runtime-session-only load
  submission and receipt consumption without physical route exposure.
- Functional validation, server validation, benchmark execution, paper
  validation, and multi-GPU execution remain deferred.

## Next Main Target

G38 model loading code integration strengthening.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
