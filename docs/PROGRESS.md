# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G21 are complete.
- G21 scheduler cost model closure is present: planner chunk allocation now uses
  scheduler net bandwidth weights derived from measured daemon profiles, trusted
  topology binding, runtime pressure, and job policy, while scheduling metadata
  exposes path weights, allocation ratios, and topology-bound cost context.
- Auto-advance continues with G22 as the only active target.

## Remaining Risk

- G22 mixed pooled execution hardening is not complete: mixed direct plus relay
  daemon-issued plans still need stronger worker/backend completion aggregation,
  path-level execution evidence, and cleanup semantics across direct and relay
  chunks.
- End-to-end CUDA, vLLM, multi-GPU, server, benchmark, and paper-validation
  evidence remains deferred. Future validation work must use real executed
  daemon/worker/backend evidence, not fake receipts, synthetic evidence, JSON
  artifacts, or dry-run output.

## Next Main Target

G22 mixed pooled execution hardening.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
