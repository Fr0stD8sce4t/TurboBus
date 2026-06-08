# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G23 are complete.
- G23 cross-job admission and fairness closure is present: daemon relay
  admission now evaluates live weighted job pressure, delays new relay plans
  when a job exceeds the fairness threshold, records fairness evidence in queue
  and receipt metadata, and lets delayed transfers re-enter through the daemon
  priority queue once resource checks pass.
- Auto-advance continues with G24 as the only active target.

## Remaining Risk

- G24 failure recovery and cleanup closure is not complete: worker/backend
  failure paths still need a single cleanup contract that releases relay
  reservations, staging records, execution tickets, and runtime feedback while
  preserving partial completion evidence in receipts.
- End-to-end CUDA, vLLM, multi-GPU, server, benchmark, and paper-validation
  evidence remains deferred. Future validation work must use real executed
  daemon/worker/backend evidence, not fake receipts, synthetic evidence, JSON
  artifacts, or dry-run output.

## Next Main Target

G24 failure recovery and cleanup closure.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
