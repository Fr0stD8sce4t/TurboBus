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
- Worker authorization failure now carries a daemon-issued cleanup contract, so
  relay lease cleanup stays inside the authorized reservation scope instead of
  falling back to timeout-only retirement when execution never starts.
- `ModelWeightLoader` now binds one runtime-session-owned CPU/GPU buffer pair
  and registers model-weight buckets by offset/bytes against that bound
  transfer context, instead of acting like a generic store that resupplies
  backings per bucket.
- `TrainingOffloadManager` now binds one runtime-session-owned CPU/GPU buffer
  pair and registers training-state buckets by offset/bytes against that bound
  transfer context, instead of acting like a generic store that resupplies
  backings per bucket.
- `InferenceKVSlotAdapter` now manages a full runtime-session-bound slot
  lifecycle, including slot registry, contiguous slot registration, restore/save
  all helpers, and explicit KV slot state marking on top of the closed core
  transfer path.

## Remaining Risk

- Another real workload family still needs one full runtime-session-facing
  adapter closure on top of the closed core transfer path.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish one full runtime-session-facing adapter expansion for the vLLM KV
workload family through `VllmKVSlotAdapter`. After that, choose exactly one of
these per round:

- one complete runtime-session-facing adapter expansion closure for another
  workload family.
- one complete validation/evaluation preparation closure only if system-body
  implementation no longer blocks it.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
