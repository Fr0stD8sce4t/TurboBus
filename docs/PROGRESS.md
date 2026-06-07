# TurboBus Progress

## Current State

- The project is still in system-body implementation, not adapters,
  benchmarks, or paper work.
- `TurboBusRuntimeSession` remains the intended single production entry.
- Managed daemon/worker startup and buffer lifetime closure are in place, and
  the runtime production socket path now owns a persistent control connection
  plus connection-scoped daemon session cleanup.
- Scheduler/load-accounting now uses live queued, running, active, and recent
  terminal runtime feedback to influence relay admission and delayed-promotion
  behavior, not just bandwidth estimation.
- A second production-facing workload family now uses explicit
  runtime-session-owned submit, wait, and receipt consumption on the vLLM KV
  path instead of hiding terminal behavior behind synchronous adapter calls.
- Shared relay ownership is now bound end to end across daemon-issued
  `owner_binding`, worker request construction, cleanup authorization, cleanup
  response validation, and receipt-facing completion evidence.

## Remaining Risk

- Daemon-issued mixed direct + relay plans still need one full execution
  closure that runs all direct and relay chunks and returns one valid receipt.
- Buffer lifetime, server, CUDA, benchmark, and adapter validation remain
  later-stage risks and do not block current implementation rounds.

## Next Main Target

Finish one full daemon-issued mixed direct + relay execution closure into one
valid `TransferReceipt`. After that, choose exactly one of these per round:

- one complete buffer registration to execution to cleanup to receipt closure
  only if mixed execution no longer blocks the main system path.
- one complete runtime-session-facing adapter expansion closure for another
  workload family only if mixed execution no longer blocks the main system
  path.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
