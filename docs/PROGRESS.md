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
- `VllmTurboBusIntegration` now owns a request-scoped KV lifecycle with
  production vLLM range-ref layout, and the production vLLM connector now uses
  that lifecycle for restore/save instead of rebuilding one-off adapter flows
  around each request or layer operation.

## Remaining Risk

- The managed production socket path still needs one runtime-owned startup and
  shutdown closure so daemon/worker process ownership, readiness, and cleanup
  evidence stay centered on `TurboBusRuntimeSession`.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish one full `TurboBusRuntimeSession`-managed production startup and
shutdown lifecycle through `open_managed_production_socket()`. After that,
choose exactly one of these per round:

- one complete production system-body closure for the next remaining runtime,
  scheduler, worker, or execution path gap.
- one complete validation/evaluation preparation closure only if system-body
  implementation no longer blocks it.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
