# TurboBus Progress

## Current State

- The project is still in system-body implementation, not adapters,
  benchmarks, or paper work.
- `TurboBusRuntimeSession` remains the intended single production entry.
- Managed daemon/worker startup and buffer lifetime closure are in place
  enough to support the remaining production-path closures.
- Direct-only, relay-only, and mixed execution now share a daemon-owned
  terminal receipt contract closely enough to move the next round back to
  runtime-session ownership.

## Remaining Risk

- Runtime-session-owned execution and cleanup still spans several modules and
  needs one clearer single-entry closure.
- Daemon execution ownership still needs more of the production path to be
  pulled behind `TurboBusRuntimeSession`.
- Scheduler/runtime load feedback still remains a later core-system closure.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish one full runtime-session-owned execution and cleanup closure. After
that, choose exactly one of these per round:

- one complete scheduler/runtime load-feedback closure.
- one complete cross-job isolation and ownership closure.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
