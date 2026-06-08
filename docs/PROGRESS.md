# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G24 are complete.
- G24 failure recovery and cleanup closure is present: failed and canceled
  worker/backend paths now merge daemon cleanup results into a
  `failure_cleanup_contract`, archive it into completion evidence, refresh
  terminal runtime feedback, and expose the contract through `TransferReceipt`.
- Auto-advance continues with G25 as the only active target.

## Remaining Risk

- G25 CUDA IPC lifecycle hardening is not complete: CUDA IPC GPU buffer
  registration, worker open/close evidence, active ticket/lease protection, and
  ownership-bound cleanup retention still need to be tightened into one
  lifecycle contract.
- End-to-end CUDA, vLLM, multi-GPU, server, benchmark, and paper-validation
  evidence remains deferred. Future validation work must use real executed
  daemon/worker/backend evidence, not fake receipts, synthetic evidence, JSON
  artifacts, or dry-run output.

## Next Main Target

G25 CUDA IPC lifecycle hardening.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
