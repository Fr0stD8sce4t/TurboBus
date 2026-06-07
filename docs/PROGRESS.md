# TurboBus Progress

## Current State

- The project is still in system-body and production adapter implementation;
  benchmarks, paper validation, and server validation remain deferred.
- `TurboBusRuntimeSession` remains the intended single production entry, and
  daemon-issued execution, receipt formation, cleanup, and runtime feedback
  stay on the real production path.
- Runtime-session-facing model-weight, training-state, inference KV, and vLLM
  KV adapter layers now bind registered CPU/GPU buffers instead of recreating
  ad hoc transfer backings per operation.
- `VllmTurboBusIntegration` now owns a request-scoped KV lifecycle: it records
  grouped vLLM allocations, registers request slots on demand, runs grouped
  restore/save through `VllmKVSlotAdapter`, reports request transfer stats, and
  forgets request-scoped adapter state when cleanup is needed.

## Remaining Risk

- The production vLLM connector path still builds one-off adapter flows around
  prefix save/restore instead of consuming the request-scoped integration
  lifecycle directly.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish one full runtime-session-facing production vLLM connector lifecycle on
top of `VllmTurboBusIntegration`. After that, choose exactly one of these per
round:

- one complete runtime-session-facing closure for the next remaining production
  workload entry.
- one complete validation/evaluation preparation closure only if system-body
  implementation no longer blocks it.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
