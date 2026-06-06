# TurboBus Progress

## Current State

- The project is still in system-body implementation, not adapters,
  benchmarks, or paper work.
- `TurboBusRuntimeSession` remains the intended single production entry.
- Managed daemon/worker startup and buffer lifetime closure are in place, and
  the runtime production socket path now owns a persistent control connection
  plus connection-scoped daemon session cleanup.
- A second production-facing workload family now uses explicit
  runtime-session-owned submit, wait, and receipt consumption on the vLLM KV
  path instead of hiding terminal behavior behind synchronous adapter calls.

## Remaining Risk

- Scheduler/load-accounting still needs one full closure driven by real queued,
  running, active, and recent terminal transfer state.
- Cross-job relay sharing still depends on later ownership and isolation
  hardening after scheduler/runtime feedback is fully closed.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish one full scheduler/load-accounting closure driven by live transfer
state. After that, choose exactly one of these per round:

- one complete cross-job isolation and ownership hardening closure on shared
  relay use.
- one complete adapter expansion closure for another workload family only if
  scheduler/runtime load feedback no longer blocks the main system path.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
