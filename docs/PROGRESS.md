# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, paper
  validation, and server validation remain deferred.
- G1 long-lived asynchronous data-plane closure is present in worker production
  code.
- G2 mixed pooled worker/backend execution is present in production code.
- G3 unified scheduling-model closure is present across daemon-issued plans,
  direct-only fallback, worker/backend completion, and receipt evidence.
- G4 dynamic feedback-loop closure is present: daemon runtime snapshots now
  expose admitted, delayed, queued, running, active, terminal, worker/backend,
  direct, relay, mixed, lease, staging, and completion evidence signals that
  scheduler load feedback consumes before planning.
- Auto-advance remains active for this goal run, with exactly one active target
  at a time.
- The active target is G5 daemon admission loop.
- Current rounds must still deliver complete production system capabilities,
  not benchmark/example/test scaffolding or narrow bug-style edits.

## Remaining Risk

- G5 still needs one full closure so daemon admission state updates consistently
  across planning, delayed work, worker authorization, terminal cleanup, lease
  expiry, and promoted transfers.
- Direct-only worker authorization remains a later daemon admission concern:
  current production direct-only execution stays a daemon-issued direct
  fallback outcome rather than a fake relay-worker route.
- Existing worker CUDA unit fixtures still encode the retired relay-scoped
  worker-plan expectation for mixed plans; they should be repaired only when the
  active plan moves to validation/test update work.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.
- Alternative verification paths, fake receipts, synthetic evidence, and dry-run
  deliverables remain out of scope for the current system-body pass.
- Auto-advance must stop rather than skip ahead if the next queued target needs
  benchmark, example, paper-validation, server-validation, new test, fake
  receipt, synthetic evidence, dry-run, or replacement verification work.

## Next Main Target

G5: finish one daemon admission-loop closure across production admission state,
delayed transfer promotion, worker authorization, terminal cleanup, lease
expiry, and scheduler/runtime feedback.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
