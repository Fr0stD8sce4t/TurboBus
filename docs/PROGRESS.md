# TurboBus Progress

## Current State

- The project is still in system-body implementation, not adapters,
  benchmarks, or paper work.
- `TurboBusRuntimeSession` remains the intended single production entry.
- Managed daemon/worker startup and buffer lifetime closure are in place
  enough to support the remaining production-path closures.
- Runtime session now owns more of the production execution path, including
  submitted intent ownership and terminal receipt consumption.

## Remaining Risk

- Scheduler/runtime load feedback still needs one clearer owned path that uses
  real queued/running/active transfer state.
- Daemon scheduling still needs tighter visibility into active execution load
  and post-completion ownership updates.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish one full scheduler/runtime load-feedback closure. After that, choose
exactly one of these per round:

- one complete cross-job isolation and ownership closure.
- one complete framework adapter closure after the core system path is stable.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
