# TurboBus Progress

## Current State

- The project is still in system-body implementation, not adapters,
  benchmarks, or paper work.
- `TurboBusRuntimeSession` remains the intended single production entry.
- Managed daemon/worker startup and buffer lifetime closure are in place
  enough to support the remaining production-path closures.
- A second production-facing workload family now uses explicit
  runtime-session-owned submit, wait, and receipt consumption on the vLLM KV
  path instead of hiding terminal behavior behind synchronous adapter calls.

## Remaining Risk

- Production startup is still split across runtime, daemon, worker, and native
  bootstrap boundaries that need one hardened owning path.
- Some production-looking startup surfaces may still expose duplicate or
  weaker entry behavior outside `TurboBusRuntimeSession`.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish one full server/runtime production-startup hardening closure on the
single `TurboBusRuntimeSession` production entry. After that, choose exactly
one of these per round:

- one complete scheduler/load-accounting closure driven by live transfer
  state.
- one complete adapter expansion closure for another workload family only if
  startup no longer blocks the main system path.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
