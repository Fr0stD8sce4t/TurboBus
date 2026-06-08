# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G15 are complete.
- G15 prefix store productionization is present: prefix storage now returns
  lifecycle mutation evidence for save, replacement, capacity eviction, drain,
  cleanup, and backing release/close handling while remaining bound to
  `TurboBusRuntimeSession` receipt-derived lifecycle state.
- Auto-advance continues with G16 as the only active target.

## Remaining Risk

- G16 model loading takeover is not complete: model-weight movement still needs
  a closed runtime-session path around buffer registration, `TransferIntent`
  submission, daemon receipt consumption, and application-policy isolation.
- Training-state offload and final auditable receipt closure remain later-stage
  work.
- Alternative verification paths, fake receipts, synthetic evidence, benchmark
  work, and dry-run deliverables remain out of scope for the current
  system-body pass.

## Next Main Target

G16 model loading takeover.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
