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

## Remaining Risk

- Cross-job relay sharing still needs one full ownership and isolation closure
  across daemon, worker, lease, and cleanup boundaries.
- Shared relay delayed admission and terminal cleanup still depend on later
  ownership hardening to rule out cross-job leakage under failure ordering.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish one full cross-job isolation and ownership hardening closure on shared
relay use. After that, choose exactly one of these per round:

- one complete adapter expansion closure for another workload family only if
- ownership no longer blocks the main system path.
- one complete native direct/relay/mixed data-path hardening closure only if
  ownership no longer blocks the main system path.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
