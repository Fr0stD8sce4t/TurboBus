# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, paper
  validation, and server validation remain deferred.
- `TurboBusRuntimeSession` stays the intended production entry, and
  daemon-issued execution, receipt formation, cleanup, and runtime feedback
  remain on the real production path.
- Scheduler-facing runtime feedback now carries execution-path-aware active and
  terminal direct/relay evidence from real daemon-issued transfer records, so
  scheduler pressure uses more than shallow queued/running counters.

## Remaining Risk

- Cross-job ownership and retained-state isolation still need one full closure
  so archived receipts, cleanup, and terminal retention stay bound to the same
  authenticated owner contract during shared relay use.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish one full cross-job ownership and isolation closure across daemon cleanup,
archived receipt access, terminal retention, and worker-issued relay cleanup.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
