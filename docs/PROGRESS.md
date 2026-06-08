# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G25 are complete.
- G25 CUDA IPC lifecycle hardening is present: worker resource binding now
  produces a `cuda_ipc_lifecycle` contract for successful execution and binding
  failure cleanup, daemon completion evidence preserves that contract, and
  `TransferReceipt` exposes it through metadata, completion contract, and buffer
  lifetime evidence.
- Auto-advance continues with G26 as the only active target.

## Remaining Risk

- G26 vLLM real lifecycle closure is not complete: vLLM KV save/restore still
  needs a stable workload lifecycle that proves real runtime-session buffer
  registration, TransferIntent submission, TransferReceipt consumption, receipt
  trace aggregation, and cleanup ownership without exposing physical route
  policy to the adapter.
- End-to-end CUDA, vLLM, multi-GPU, server, benchmark, and paper-validation
  evidence remains deferred. Future validation work must use real executed
  daemon/worker/backend evidence, not fake receipts, synthetic evidence, JSON
  artifacts, or dry-run output.

## Next Main Target

G26 vLLM real lifecycle closure.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
