# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, paper
  validation, and server validation remain deferred.
- G1 long-lived asynchronous data-plane closure is present in worker production
  code: `CudaWorkerExecutor` separates submit and wait, retains reusable native
  runtimes, tracks in-flight and terminal transfer handles, and reports async
  execution evidence through worker completion metadata.
- G2 mixed pooled execution is now present in production code: daemon worker
  authorization returns exact daemon-issued plan ranges, worker request
  validation uses the ticket full-plan ranges, CUDA worker execution keeps
  direct and relay assignments in one native plan, and runtime intent execution
  no longer pre-executes mixed direct chunks outside the worker/backend path.
- Auto-advance remains active for this goal run, with exactly one active target
  at a time.
- The active target is G3 unified scheduling model.
- Current rounds must still deliver complete production system capabilities,
  not benchmark/example/test scaffolding or narrow bug-style edits.

## Remaining Risk

- G3 still needs one full closure so scheduler output, daemon admission,
  worker authorization, execution tickets, runtime execution, and receipt
  evidence all consume one canonical direct/relay/mixed plan contract.
- Direct-only worker authorization remains a later daemon admission concern:
  current production direct-only execution stays a daemon-issued direct
  fallback outcome rather than a fake relay-worker route.
- Existing worker CUDA unit fixtures still encode the retired relay-scoped
  worker-plan expectation for mixed plans; they fail against the new G2
  production contract and should be repaired only when the active plan moves to
  validation/test update work.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.
- Alternative verification paths, fake receipts, synthetic evidence, and dry-run
  deliverables remain out of scope for the current system-body pass.
- Auto-advance must stop rather than skip ahead if the next queued target needs
  benchmark, example, paper-validation, server-validation, new test, fake
  receipt, synthetic evidence, dry-run, or replacement verification work.

## Next Main Target

G3: finish one unified scheduling-model closure across scheduler plan output,
daemon admission, ticket construction, runtime execution, and receipt evidence.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
