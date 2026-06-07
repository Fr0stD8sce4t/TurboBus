# TurboBus Progress

## Current State

- The project is still in system-body and production adapter implementation;
  benchmarks, paper validation, and server validation remain deferred.
- `TurboBusRuntimeSession` remains the intended single production entry, and
  daemon-issued execution, receipt formation, cleanup, and runtime feedback
  stay on the real production path.
- The managed production socket path is now runtime-owned end to end:
  `open_managed_production_socket()` injects the effective socket paths into
  runtime options, keeps managed daemon/worker startup records on the session,
  checks owned service liveness before runtime use, and returns shutdown
  payloads with pre/post managed runtime snapshots plus shutdown evidence.
- Runtime-session-facing model-weight, training-state, inference KV, and vLLM
  KV adapter layers now bind registered CPU/GPU buffers instead of recreating
  ad hoc transfer backings per operation.
- `VllmTurboBusIntegration` now owns a request-scoped KV lifecycle with
  production vLLM range-ref layout, and the production vLLM connector now uses
  that lifecycle for restore/save instead of rebuilding one-off adapter flows
  around each request or layer operation.

## Remaining Risk

- Scheduler and relay admission still need one live runtime-load-feedback
  closure so later scheduling decisions consume real queued/running/active
  transfer state and completion evidence.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish one full live scheduler runtime-load-feedback lifecycle in the daemon
control path. After that, choose exactly one of these per round:

- one complete production system-body closure for the next remaining runtime,
  worker, execution, or ownership-hardening gap.
- one complete validation/evaluation preparation closure only if system-body
  implementation no longer blocks it.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
