# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G10 are complete.
- G10 worker asynchronous execution pool is present: worker transfers enter a
  long-lived pool after daemon ticket validation, keep queued/running/terminal
  records, execute exact daemon-issued requests through the worker resource
  binder and backend executor, preserve cleanup on failure, and attach async
  pool evidence to worker results and daemon completion evidence.
- Auto-advance continues with G11 as the only active target.

## Remaining Risk

- G11 scheduler cost-model upgrade is not complete: scheduling still needs a
  stronger runtime-load cost model for queued, admitted, running, relay, direct,
  worker/backend evidence, and job pressure.
- Admission priority queue, runtime feedback metrics, framework adapters, and
  final auditable receipt closure remain later-stage work.
- Alternative verification paths, fake receipts, synthetic evidence, benchmark
  work, and dry-run deliverables remain out of scope for the current
  system-body pass.

## Next Main Target

G11 scheduler cost-model upgrade.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
