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
- G5 daemon admission-loop closure is present: resource changes, lease expiry,
  cleanup, terminal status updates, delayed promotion, queue records, and
  admission summaries now flow through one daemon-owned admission refresh path.
- Auto-advance remains active for this goal run, with exactly one active target
  at a time.
- The active target is G6 multi-tenant isolation hardening.
- Current rounds must still deliver complete production system capabilities,
  not benchmark/example/test scaffolding or narrow bug-style edits.

## Remaining Risk

- G6 still needs one full closure so daemon-owned job, session, buffer, lease,
  staging, ticket, cleanup, and receipt ownership remain bound to authenticated
  peers across shared relay use and archived terminal state.
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
- Auto-advance must stop rather than skip ahead if G6 requires benchmark,
  example, paper-validation, server-validation, new test, fake receipt,
  synthetic evidence, dry-run, or replacement verification work.

## Next Main Target

G6: finish one multi-tenant isolation closure across daemon peer ownership,
worker authorization, transfer status updates, cleanup retention, staging
records, lease ownership, and archived receipt access.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
