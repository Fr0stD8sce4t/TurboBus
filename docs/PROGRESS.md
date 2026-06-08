# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G17 are complete.
- G17 training-state offload closure is present: training-state prefetch and
  offload APIs now submit runtime-session-backed transfer intent, wait for
  daemon receipts, reject completed transfers without `TransferReceipt`
  evidence, and expose receipt-derived lifecycle evidence for bucket movement
  without adapter-side route policy.
- Auto-advance continues with G18 as the only active target.

## Remaining Risk

- G18 unified auditable receipt closure is not complete: framework-facing
  receipt lifecycle evidence still needs one shared structure across offload,
  model loading, training state, and vLLM KV paths.
- Alternative verification paths, fake receipts, synthetic evidence, benchmark
  work, and dry-run deliverables remain out of scope for the current
  system-body pass.

## Next Main Target

G18 unified auditable receipt closure.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
