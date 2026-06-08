# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G18 are complete.
- G18 unified auditable receipt closure is present: model loading,
  training-state offload, and vLLM KV paths now share receipt-derived lifecycle
  evidence through `turbobus/offload/lifecycle.py`, while retaining
  daemon-issued `TransferIntent` and `TransferReceipt` authority.
- Auto-advance queue is complete.

## Remaining Risk

- End-to-end CUDA, vLLM, multi-GPU, server, benchmark, and paper-validation
  evidence remains deferred and has not been added in this system-body queue.
- Future validation work must use real executed daemon/worker/backend evidence,
  not fake receipts, synthetic evidence, JSON artifacts, or dry-run output.

## Next Main Target

No active main target remains in the current auto-advance queue.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
