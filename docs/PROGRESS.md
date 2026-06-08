# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G30 are complete.
- G30 real-execution validation and evaluation entry recovery is present:
  validation-facing code now accepts only terminal `TransferReceipt` objects,
  validates daemon/worker/backend `reproduction_evidence`, rejects fake,
  synthetic, JSON-only, and dry-run proof, and exposes a normalized
  `turbobus.real_execution_validation.v1` view for later evaluation.
- The G19-G30 auto-advance queue has no remaining active target.

## Remaining Risk

- End-to-end CUDA, vLLM, multi-GPU, server, benchmark, and paper-validation
  evidence remains deferred to a future user-provided target queue. Future
  validation and evaluation must consume real executed daemon/worker/backend
  receipt evidence through the recovered validation entry.

## Next Main Target

None for the current G19-G30 queue.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
