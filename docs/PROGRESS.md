# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G6 are complete.
- G7 production profile closure is present: `TurboBusRuntimeSession` uses the
  production profile client to bootstrap daemon profiles from CUDA profiling or
  a fresh daemon cache entry, verifies daemon cache visibility after install,
  and exposes profile-bootstrap evidence through the runtime session.
- Auto-advance continues with G8 as the only active target.

## Remaining Risk

- G8 buffer ownership lifecycle is not complete: daemon buffer metadata exists,
  but the next round must close active-ticket ownership/refcount protection and
  terminal cleanup evidence.
- G9 CUDA IPC metadata and span validation is still pending.
- Worker pool, scheduler cost model, admission priority queue, runtime feedback
  metrics, framework adapters, and final server validation remain later-stage
  work.
- Alternative verification paths, fake receipts, synthetic evidence, benchmark
  work, and dry-run deliverables remain out of scope for the current
  system-body pass.

## Next Main Target

G8 buffer ownership lifecycle closure.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
