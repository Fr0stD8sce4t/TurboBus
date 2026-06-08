# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G29 are complete.
- G29 unified reproduction evidence model is present: every production
  `TransferReceipt` now exposes a stable `reproduction_evidence` view covering
  daemon-owned scheduling, direct/relay/mixed execution mode, completion
  contract, cleanup, buffer lifetime, CUDA IPC lifecycle, and fake/synthetic/
  dry-run rejection markers.
- Auto-advance continues with G30 as the only active target.

## Remaining Risk

- G30 real-execution validation and evaluation entry recovery is not complete:
  validation-facing code still needs a production entry that consumes only real
  daemon/worker/backend receipt evidence and rejects fake, synthetic, JSON-only,
  or dry-run proof.
- End-to-end CUDA, vLLM, multi-GPU, server, benchmark, and paper-validation
  evidence remains deferred until validation is bound to real executed
  daemon/worker/backend evidence.

## Next Main Target

G30 real-execution validation and evaluation entry recovery.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
