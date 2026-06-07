# TurboBus Progress

## Current State

- The project is still in system-body implementation, not adapters,
  benchmarks, or paper work.
- `TurboBusRuntimeSession` remains the intended single production entry, and
  daemon-issued execution, receipt formation, and runtime feedback stay on the
  real production path.
- Daemon-side production registration now keeps `job_id` and `buffer_id`
  ownership claims bound to the owning peer instead of allowing later peers or
  conflicting registrations to overwrite them.
- Runtime-session and connection-scoped session teardown now retire
  session-owned jobs and buffers as daemon cleanup targets, preserving owner
  evidence and visible retired target ids after session close or disconnect.

## Remaining Risk

- Cross-job isolation still needs the remaining relay-lease ownership closure
  across worker cleanup scope and retired reservation cleanup.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish the remaining relay-lease ownership closure across daemon lease
validation, worker cleanup scope, and retired reservation cleanup. After that,
choose exactly one of these per round:

- one complete runtime-session-facing adapter expansion closure for another
  workload family.
- one complete validation/evaluation preparation closure only if system-body
  implementation no longer blocks it.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
