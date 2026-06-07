# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, paper
  validation, and server validation remain deferred.
- G1 long-lived asynchronous data-plane closure is present in worker production
  code.
- G2 mixed pooled worker/backend execution is present in production code.
- G3 unified scheduling-model closure is present across daemon-issued plans,
  direct-only fallback, worker/backend completion, and receipt evidence.
- G4 dynamic feedback-loop closure is present across daemon runtime snapshots
  and scheduler load feedback.
- G5 daemon admission-loop closure is present across production admission state,
  delayed transfer promotion, worker authorization, terminal cleanup, lease
  expiry, and scheduler/runtime feedback.
- G6 multi-tenant isolation closure is present: daemon-owned job, session,
  buffer, lease, staging, ticket, cleanup, and archived receipt ownership now
  preserve authenticated peer binding across shared relay use and terminal
  cleanup.
- Auto-advance queue G1 through G6 is complete.
- Current rounds must still avoid benchmark/example/test scaffolding,
  paper-validation, server-validation, fake evidence, synthetic evidence,
  dry-run deliverables, and replacement verification paths unless a future
  active plan explicitly moves into validation work.

## Remaining Risk

- Direct-only worker authorization remains a later daemon admission concern:
  current production direct-only execution stays a daemon-issued direct
  fallback outcome rather than a fake relay-worker route.
- Existing worker CUDA unit fixtures still encode the retired relay-scoped
  worker-plan expectation for mixed plans; they should be repaired only when the
  active plan moves to validation/test update work.
- Server, CUDA, benchmark, adapter, and paper validation remain later-stage
  risks and do not block the completed G1-G6 system-body queue.
- Alternative verification paths, fake receipts, synthetic evidence, and dry-run
  deliverables remain out of scope for the current system-body pass.

## Next Main Target

No active G1-G6 target remains. A new user-approved system-body target is needed
before further auto-advance work.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
