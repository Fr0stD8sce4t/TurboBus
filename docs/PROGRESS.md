# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G16 are complete.
- G16 model loading takeover is present: model-weight load APIs now submit
  runtime-session-backed transfer intent, wait for daemon receipts, reject
  completed loads without `TransferReceipt` evidence, and expose receipt-derived
  lifecycle evidence for manifest and tensor loads without adapter-side route
  policy.
- Auto-advance continues with G17 as the only active target.

## Remaining Risk

- G17 training-state offload closure is not complete: training-state movement
  still needs a closed runtime-session path around buffer registration,
  offload/restore intent submission, receipt consumption, and lifecycle
  evidence.
- Final auditable receipt closure remains later-stage work.
- Alternative verification paths, fake receipts, synthetic evidence, benchmark
  work, and dry-run deliverables remain out of scope for the current
  system-body pass.

## Next Main Target

G17 training-state offload closure.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
