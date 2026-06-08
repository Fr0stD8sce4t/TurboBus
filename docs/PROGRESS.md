# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G13 are complete.
- G13 runtime feedback metrics closure is present: daemon runtime state now
  aggregates worker/backend completion counts, execution-path evidence, cleanup
  outcomes, async worker pool state, CUDA IPC span validation, and recent
  terminal feedback into scheduler-consumable metrics; scheduler load feedback
  consumes those metrics for worker/backend pressure, and runtime receipt
  validation requires worker async pool evidence for worker completions.
- Auto-advance continues with G14 as the only active target.

## Remaining Risk

- G14 vLLM KV lifecycle closure is not complete: vLLM KV save/restore still
  needs a fully closed runtime-session lifecycle around real buffer
  registration, TransferIntent submission, and TransferReceipt consumption.
- Prefix store productionization, model loading, training-state offload, and
  final auditable receipt closure remain later-stage work.
- Alternative verification paths, fake receipts, synthetic evidence, benchmark
  work, and dry-run deliverables remain out of scope for the current
  system-body pass.

## Next Main Target

G14 vLLM KV lifecycle closure.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
