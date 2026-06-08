# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G20 are complete.
- G20 profile measurement closure is present: daemon profile writes now require
  measured direct and relay bandwidth, bind the profile cache to the current
  trusted topology snapshot, reject topology-ineligible relay profiles, and
  purge stale or topology-mismatched cache entries before scheduler use.
- Auto-advance continues with G21 as the only active target.

## Remaining Risk

- G21 paper-grade scheduler cost model is not complete: scheduler planning still
  needs a stronger measured-cost allocation model that uses topology-bound
  profiles, live queue/running/admitted state, relay pressure, and job priority
  to produce explainable direct, relay, or mixed pooled splits.
- End-to-end CUDA, vLLM, multi-GPU, server, benchmark, and paper-validation
  evidence remains deferred. Future validation work must use real executed
  daemon/worker/backend evidence, not fake receipts, synthetic evidence, JSON
  artifacts, or dry-run output.

## Next Main Target

G21 paper-grade scheduler cost model.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
