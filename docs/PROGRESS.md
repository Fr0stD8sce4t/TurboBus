# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, paper
  validation, and server validation remain deferred.
- G1 long-lived asynchronous data-plane closure is present in worker production
  code.
- G2 mixed pooled worker/backend execution is present in production code.
- G3 unified scheduling-model closure is present: scheduler output, daemon
  tickets, direct-only fallback, worker/backend completion, and receipts now
  preserve the daemon-issued plan contract and unified worker completion
  evidence for direct, relay, and mixed pooled transfers.
- Auto-advance remains active for this goal run, with exactly one active target
  at a time.
- The active target is G4 dynamic feedback loop.
- Current rounds must still deliver complete production system capabilities,
  not benchmark/example/test scaffolding or narrow bug-style edits.

## Remaining Risk

- G4 still needs one full closure so daemon scheduler input reflects live
  queued/running/active transfer state, relay leases, staging records,
  worker/backend completion, and failure evidence from production runtime state.
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

G4: finish one dynamic feedback-loop closure across daemon runtime state,
scheduler input, worker/backend status updates, completion evidence, and failure
signals.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
