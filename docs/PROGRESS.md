# TurboBus Progress

## Current State

- The project is still in system-body implementation, not adapters,
  benchmarks, or paper work.
- `TurboBusRuntimeSession` remains the intended single production entry.
- Managed daemon/worker startup and buffer lifetime closure are in place
  enough to support the remaining production-path closures.
- Shared relay execution now keeps daemon-issued ownership scope through worker
  authorization, cleanup, and terminal receipt evidence instead of letting the
  worker infer cleanup scope on its own.

## Remaining Risk

- Production adapters still need one full path that uses
  `TurboBusRuntimeSession` for real buffer registration, intent submission, and
  receipt consumption.
- Some framework-facing entry points may still carry older execution ownership
  assumptions until they are migrated onto the runtime-session production path.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish one full framework adapter closure through `TurboBusRuntimeSession`.
After that, choose
exactly one of these per round:

- one complete server-backed validation closure after the system body is
  complete.
- one complete adapter expansion closure for the next workload family on the
  same runtime-session production path.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
