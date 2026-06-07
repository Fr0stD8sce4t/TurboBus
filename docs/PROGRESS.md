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

## Remaining Risk

- Cross-job isolation still needs the remaining relay-lease, retired-cleanup,
  and runtime-teardown ownership closure.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish the remaining cross-job isolation and ownership closure across relay
leases, retired cleanup targets, and runtime-session-driven teardown. After
that, choose exactly one of these per round:

- one complete runtime-session-facing adapter expansion closure for another
  workload family.
- one complete validation/evaluation preparation closure only if system-body
  implementation no longer blocks it.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
