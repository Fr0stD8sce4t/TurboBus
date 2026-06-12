# TurboBus Progress

Keep this file short and forward-looking. Replace current state after each
completed system capability loop. Do not accumulate implementation history.

## Current State

- The project is in system-code reproduction. Benchmark runs, examples, paper
  validation, server validation, new tests, mock gates, fake evidence,
  synthetic evidence, and dry-run deliverables remain deferred.
- The PCIe shared-fabric bandwidth-pool foundation is present in the production
  scheduling path. Daemon runtime state now builds a PCIe fabric snapshot,
  derives edge load from active daemon paths, publishes a bandwidth-pool view,
  exposes it through runtime telemetry, and feeds it into scheduler load view.
- Scheduler cost weighting now consumes daemon-owned PCIe bandwidth-pool facts.
  Direct paths are capped by target PCIe availability, relay paths are capped by
  relay/target PCIe availability plus existing runtime and fabric constraints,
  and cost metadata records the bandwidth-pool adjustment source.
- Scheduler decisions now carry a daemon-owned block plan. Large transfers are
  split into block records with path ids, allowed path ids, attempt counters,
  and runtime block metadata for direct-only, relay-only, and mixed pooled
  transfers.
- The current code still does not issue block runtime tickets, leases, cleanup,
  progress, or receipts. The next round closes that daemon-owned block runtime
  loop.

## Remaining Risk

- PCIe load is currently derived from daemon active path state. Hardware counter
  sampling can replace or enrich it later, but must report explicit unknown
  state when unavailable.
- Functional validation, server validation, benchmark execution, paper
  validation, and multi-GPU execution remain deferred to the later validation
  and evaluation stage.
- Existing unrelated dirty files remain outside this round's intended change
  boundary.
- Goal-mode rounds must close complete system capability loops. Small fixes,
  field moves, helper relocation, and documentation updates do not count unless
  they finish the same active loop.

## Next Main Target

Build daemon-issued block runtime, tickets, leases, progress, cleanup, and
receipts on top of block-level scheduling.

This target remains open until the daemon can describe block progress and
terminal receipts for direct-only, relay-only, and mixed pooled block plans.
