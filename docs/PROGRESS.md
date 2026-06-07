# TurboBus Progress

## Current State

- The project is still in system-body implementation, not adapters,
  benchmarks, or paper work.
- `TurboBusRuntimeSession` remains the intended single production entry.
- Managed daemon/worker startup is in place, and the runtime production socket
  path owns a persistent control connection plus connection-scoped daemon
  session cleanup.
- Scheduler/load-accounting now uses live queued, running, active, and recent
  terminal runtime feedback to influence relay admission and delayed-promotion
  behavior, not just bandwidth estimation.
- A second production-facing workload family now uses explicit
  runtime-session-owned submit, wait, and receipt consumption on the vLLM KV
  path instead of hiding terminal behavior behind synchronous adapter calls.
- Shared relay ownership is now bound end to end across daemon-issued
  `owner_binding`, worker request construction, cleanup authorization, cleanup
  response validation, and receipt-facing completion evidence.
- Daemon-issued mixed direct + relay execution now closes through one
  production path: direct chunks run on the backend, relay chunks run on the
  worker, and daemon completion evidence keeps unified path split plus cleanup
  evidence for one valid receipt.
- Registered buffer lifetime now closes through one production receipt
  contract: runtime-owned registration snapshots are paired with worker resource
  open/close evidence, and binding failures keep explicit cleanup evidence
  instead of dropping out before receipt formation.

## Remaining Risk

- Daemon/worker production startup still needs one full closure from runtime
  bootstrap through authenticated execution, failure handling, and cleanup
  evidence.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish one full daemon/worker production startup closure from runtime-session
bootstrap through authenticated execution, failure handling, and cleanup.
After that, choose exactly one of these per round:

- one complete runtime-session-facing adapter expansion closure for another
  workload family.
- one complete scheduler/topology feedback closure only if startup no longer
  blocks the main system path.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
