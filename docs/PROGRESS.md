# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, paper
  validation, and server validation remain deferred.
- `TurboBusRuntimeSession` stays the intended production entry, and
  daemon-issued execution, receipt formation, cleanup, and runtime feedback
  remain on the real production path.
- Live scheduler feedback now retires terminal no-reservation transfers out of
  the live runtime queue, moves them into terminal feedback state, and lets
  delayed-transfer promotion see the current daemon load instead of stale
  completed direct transfers.

## Remaining Risk

- Cross-job cleanup ownership still needs one full closure across live,
  archived, and missing targets so shared relay cleanup cannot escape the
  owning peer scope.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish one full cross-job ownership and cleanup-isolation lifecycle in the
daemon and worker control path.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
