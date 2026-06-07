# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, paper
  validation, and server validation remain deferred.
- G1 long-lived asynchronous data-plane closure is now present in worker
  production code: `CudaWorkerExecutor` separates submit and wait, retains
  reusable native runtimes, tracks in-flight and terminal transfer handles, and
  reports async execution evidence through worker completion metadata.
- `WorkerTransferClient` now prefers the submit/wait worker path while keeping
  the existing synchronous execution helper as a compatibility wrapper.
- Auto-advance remains active for this goal run, with exactly one active target
  at a time.
- The active target is G2 mixed pool unified execution.
- Current rounds must still deliver complete production system capabilities,
  not benchmark/example/test scaffolding or narrow bug-style edits.

## Remaining Risk

- G2 still needs one full closure so daemon-issued plans containing both direct
  and relay assignments execute through one worker/backend path instead of
  relay-only worker narrowing.
- Cross-job ownership and retained-state isolation remain queued for G6 so
  archived receipts, cleanup, and terminal retention stay bound to the same
  authenticated owner contract during shared relay use.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.
- Alternative verification paths, fake receipts, synthetic evidence, and dry-run
  deliverables remain out of scope for the current system-body pass.
- Auto-advance must stop rather than skip ahead if the next queued target needs
  benchmark, example, paper-validation, server-validation, new test, fake
  receipt, synthetic evidence, dry-run, or replacement verification work.

## Next Main Target

G2: finish one mixed direct+relay pooled execution closure across daemon-issued
worker plans, native plan conversion, backend execution, and unified completion
evidence.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
