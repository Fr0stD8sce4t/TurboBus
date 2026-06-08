# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G11 are complete.
- G11 scheduler cost-model upgrade is present: daemon scheduling now folds live
  queued, admitted, running, active direction, relay, job fairness, and
  worker/backend evidence into runtime cost pressure, adjusts direct and relay
  bandwidth through that model, and records per-candidate and selected path cost
  metadata in daemon-issued scheduling decisions.
- Auto-advance continues with G12 as the only active target.

## Remaining Risk

- G12 admission priority queue is not complete: delayed relay admission still
  needs daemon-owned priority ordering and promotion evidence across competing
  jobs and runtime resource availability.
- Runtime feedback metrics, framework adapters, and final auditable receipt
  closure remain later-stage work.
- Alternative verification paths, fake receipts, synthetic evidence, benchmark
  work, and dry-run deliverables remain out of scope for the current
  system-body pass.

## Next Main Target

G12 admission priority queue.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
