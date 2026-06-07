# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G2: close mixed pooled execution so daemon-issued plans with both direct and
relay assignments execute through one worker/backend path and produce unified
completion evidence.

## Exit Criteria

- Worker-side plan conversion keeps direct and relay assignments from the same
  daemon-issued `ExecutionTicket` instead of narrowing worker execution to
  relay-only chunks.
- Direct-only, relay-only, and mixed direct+relay plans use the same
  submit/wait/result path in worker CUDA execution.
- Completion evidence reports direct bytes/chunks, relay bytes/chunks, and
  aggregate bytes from the exact daemon-issued plan.
- Failure paths still produce worker/backend failure receipts and cleanup
  evidence without fake completion.
- The closure stays in worker/runtime/backend production code and does not add
  benchmark-owned, example-owned, test-owned, dry-run, or synthetic evidence
  paths.

## Current Code Work

- `turbobus/worker/cuda_executor.py`
- `turbobus/worker/lifecycle.py`
- `turbobus/worker/models.py`
- `turbobus/worker/validation.py`
- `turbobus/native_plan.py`
- `turbobus/backends/cuda.py`
- `cpp/src/executor_cuda.cu`

Round rules:

- Start each round with `git status`, then read `AGENTS.md`,
  `docs/TURBOBUS_ROADMAP.md`, `docs/NEXT_STEPS.md`, and `docs/PROGRESS.md`.
- Choose the single round target from this file first and `docs/PROGRESS.md`
  second.
- Finish one full system closure per round, not local bug-style fixes.
- Treat a round as complete only when the system gains one independently
  describable production capability on the current target path.
- Do not advance benchmark, example, paper-validation, server-validation, new
  test, dry-run, fake receipt, synthetic evidence, or replacement verification
  entry work during the current system-body pass.
- State assumptions when they matter, prefer the simplest correct change, and
  keep edits surgical to the active target.
- Update this file and `docs/PROGRESS.md` after each completed closure.
- Keep only active and next work here. Do not append completed history.

## Next Entry

Start at `worker/cuda_executor.py` around daemon plan assignment filtering.
Remove relay-only narrowing only if the worker ticket and authorization already
prove the plan is daemon-issued and scoped to the worker. Then follow the exact
plan into `native_plan.py`, `backends/cuda.py`, and `cpp/src/executor_cuda.cu`
only as needed to close one mixed direct+relay production execution path.

After the current target closes in auto-advance mode, the next queued target is:

- G3 unified scheduling model.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.

## Auto-Advance Policy

Auto-advance is enabled for the current goal run because the user explicitly
started TurboBus Auto-Advance Mode.

Remaining auto-advance target queue:

1. G2 mixed pool unified execution.
2. G3 unified scheduling model.
3. G4 dynamic feedback loop.
4. G5 daemon admission loop.
5. G6 multi-tenant isolation hardening.

In auto-advance mode:

- keep exactly one current active target at a time;
- after each completed target, rewrite this file and `docs/PROGRESS.md` so the
  next queued target becomes the only current active target;
- for each queued target, carry forward the same system contracts from
  `AGENTS.md` and the same no-benchmark/no-test/no-fake-evidence constraints
  from this file;
- continue only while the next queued target is still system-body work;
- stop when the queue is complete, external environment blocks the target, a
  real architecture choice needs user review, or continuing would require
  benchmark, example, paper-validation, server-validation, new test, fake
  receipt, synthetic evidence, dry-run, or replacement verification work.
