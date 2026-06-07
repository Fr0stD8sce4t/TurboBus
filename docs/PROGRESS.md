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
- Runtime-session-owned managed daemon/worker startup now closes through one
  production path: startup failures return explicit daemon/worker evidence plus
  shutdown evidence, and worker startup/authentication evidence survives into
  worker-backed receipts instead of disappearing during daemon normalization.

## Remaining Risk

- Scheduler/runtime feedback still needs one full closure from real
  queued/running/active execution state through relay admission and path
  selection.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish one full scheduler/runtime feedback closure from real
queued/running/active execution state through relay admission and path
selection. After that, choose exactly one of these per round:

- one complete cross-job isolation and ownership hardening closure.
- one complete runtime-session-facing adapter expansion closure only if
  scheduler feedback no longer blocks the main system path.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
