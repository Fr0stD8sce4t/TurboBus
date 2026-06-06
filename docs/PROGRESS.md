# TurboBus Progress

## Current State

- The project is still in system-body implementation, not adapters,
  benchmarks, or paper work.
- `TurboBusRuntimeSession` remains the intended single production entry.
- Managed daemon/worker startup and buffer lifetime closure are in place
  enough to support the remaining execution-path closures.
- The next required system closure is a relay-only path that ends in one
  daemon-owned receipt contract.

## Remaining Risk

- Relay-only execution still relies on a less explicit terminal path than mixed
  execution.
- Daemon execution ownership still spans several modules and needs one cleaner
  mode-owned closure at a time.
- Scheduler/runtime load feedback still remains a later core-system closure.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish relay-only execution as one full daemon-owned closure. After that,
choose exactly one of these per round:

- one complete runtime-session-owned startup/execution/cleanup closure;
- one complete scheduler/runtime load-feedback closure.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
