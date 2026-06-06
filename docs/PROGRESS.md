# TurboBus Progress

## Current State

- The project is still in system-body implementation, not adapters,
  benchmarks, or paper work.
- `TurboBusRuntimeSession` remains the intended single production entry.
- Managed daemon/worker startup and buffer lifetime closure are in place
  enough to support the remaining production-path closures.
- Scheduler/runtime load feedback now stays on the daemon-owned production path
  longer, including recent terminal completion ownership after transfers retire.

## Remaining Risk

- Cross-job isolation and ownership still need one tighter production closure
  across shared relay execution, cleanup, and receipt retirement.
- Worker and daemon ownership checks still need to prove that one job cannot
  consume or retire another job's transfer state through shared resources.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish one full cross-job isolation and ownership closure. After that, choose
exactly one of these per round:

- one complete framework adapter closure after the core system path is stable.
- one complete server-backed validation closure after the system body is
  complete.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
