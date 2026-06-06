# TurboBus Progress

## Current State

- The project is still in system-body implementation, not adapters,
  benchmarks, or paper work.
- `TurboBusRuntimeSession` remains the intended single production entry.
- Managed daemon/worker startup and buffer lifetime closure are in place
  enough to support the remaining production-path closures.
- One production-facing adapter family on top of `OffloadStore` now uses
  `TurboBusRuntimeSession` as a real submit-then-wait owner instead of hiding a
  synchronous fetch/evict shortcut behind adapter submit APIs.

## Remaining Risk

- The next adapter workload family still needs to close on the same
  runtime-session-owned submit/receipt path, especially the more vLLM-shaped
  integration surface.
- Some framework-facing entry points may still carry older assumptions about
  when submit APIs are terminal versus when receipt consumption is deferred.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish one full adapter expansion closure for the next workload family on the
same `TurboBusRuntimeSession` production path. After that, choose
exactly one of these per round:

- one complete server-backed validation closure after the system body is
  complete.
- one complete server/runtime production-startup hardening closure if adapters
  no longer block the main system path.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
