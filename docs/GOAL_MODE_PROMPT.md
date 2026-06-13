# TurboBus Goal Mode Prompt

Use this prompt for every goal-mode continuation.

Continue the TurboBus main line. The current stage only advances system-code
reproduction and necessary refactoring. Do not advance benchmarks, examples,
paper validation, server validation, new tests, alternative validation CLIs,
fake receipts, synthetic evidence, or dry-run deliverables.

When goal mode can continue across turns, do not treat one finished round as
the whole goal being complete. Finish the current main target, update the plan
files to the next target, and let the next continuation reread the plan files
before advancing again. Stop the overall goal only when the roadmap is complete
or when the same external blocker prevents meaningful system-code progress.

## Required Reading

At the start of every round, read and obey:

1. `AGENTS.md`
2. `docs/TURBOBUS_ROADMAP.md`
3. `docs/NEXT_STEPS.md`
4. `docs/PROGRESS.md`

## Target Selection

- Do not hard-code the current main target in this prompt.
- Each round must derive the single current main target from
  `docs/NEXT_STEPS.md` sections `Current Main Target`, `Current Code Work`,
  and `Next Entry`, plus `docs/PROGRESS.md` sections `Current State`,
  `Remaining Risk`, and `Next Main Target`.
- Conflict priority is:
  1. `docs/NEXT_STEPS.md`
  2. `docs/PROGRESS.md`
  3. `AGENTS.md`
  4. `docs/TURBOBUS_ROADMAP.md`
- Stop after the current main target is complete. Do not automatically enter the
  next target.
- Do not do the next target early unless it is the smallest necessary blocker
  for the current target.
- In continuous goal mode, this means the current round stops at the target
  boundary, then the next automatic continuation starts a new round from the
  updated plan files. Do not mark the whole goal complete just because one
  target closed.

## Current Stage Rules

- Only advance the system body.
- Do not do benchmark, example, paper validation, server validation, new tests,
  alternative validation entrypoints, fake receipt paths, synthetic evidence, or
  dry-run artifacts.
- Adapter migration is allowed only when it directly blocks the active system
  path.
- Missing local CUDA, vLLM, multi-GPU, or server hardware is an external
  validation blocker, not a reason to add mock gates, fake execution paths, or
  local substitute frameworks.
- If external validation is blocked, continue implementing the current
  non-environment-dependent system-code capability until the code boundary is
  closed.
- If push, CUDA, vLLM, multi-GPU, or server validation is blocked by external
  state, record the blocker and keep advancing code that does not depend on
  that external state.

## Architecture Constraints

- Daemon and scheduler are the only production source of transfer plans.
- Applications, benchmarks, examples, and adapters may only submit
  `TransferIntent` and consume `TransferReceipt`.
- Worker, data plane, and CUDA executor may only execute daemon-issued
  `ExecutionTicket` objects or exact daemon-issued plans.
- Applications, benchmarks, examples, adapters, workers, and CUDA executor must
  not choose direct, relay, pool, target GPU, or relay GPU.
- Do not restore old `Runtime` or planner compatibility APIs.
- Do not restore single-process, single-job, or manual-relay production routes.
- Do not treat synthetic topology, fake receipts, JSON artifacts, or dry-run
  output as reproduction evidence.
- Do not let benchmark or example code define core architecture.

## Round Requirement

- Each round must complete one full system capability subtarget.
- A completed round must add a real code capability that can be described
  independently.
- Do not count a local bug fix, wait semantic tweak, field rename, helper move,
  import cleanup, documentation sync, or boundary tightening as a completed
  round unless it is the last step needed to close the same system capability.
- If the round requires refactoring or deleting old entrypoints, continue to the
  real capability loop under that same system boundary. Do not stop halfway.
- Do not split one system subtarget into many tiny rounds. Prefer one round that
  closes a whole main-path segment.
- Prefer converging on one production entrypoint. If `TurboBusRuntimeSession`
  overlaps with other production-looking paths, tighten or remove duplicated
  responsibility.
- This stage prioritizes large missing system capabilities over isolated bug
  repair.

## Preferred Capability Loops

- `TurboBusRuntimeSession` as the single production entrypoint.
- `TransferIntent -> SchedulingDecision -> ExecutionTicket -> worker/backend
  execution -> TransferReceipt`.
- Daemon-issued direct-only, relay-only, and mixed pooled execution.
- Unified completion evidence for direct, relay, and mixed pooled paths.
- Worker failure -> cleanup -> receipt.
- Session/job/buffer registration -> real execution.
- Shared pinned CPU buffer and CUDA IPC GPU buffer lifecycle.
- Worker/backend state feeding daemon runtime feedback.
- Scheduler load accounting from real queued/running/active transfer state.
- Runtime session -> daemon/worker/socket production startup and execution.

## Not A Complete Round By Itself

- Changing only one wait branch.
- Fixing only receipt parsing.
- Tightening only one compatibility API.
- Moving helpers or renaming fields.
- Updating only documents or plans.
- Adding tests, benchmarks, examples, or validation wrappers before the active
  system path needs them.

## Work Method

- Start every round with `git status`.
- Identify existing dirty files. Do not overwrite, revert, or mix unrelated
  dirty work.
- Current code entrypoints come from `docs/NEXT_STEPS.md` `Current Code Work`
  and `Next Entry`.
- Modify production main paths first. Do not drive system design from
  benchmark, example, or test code.
- You may use sub-agents for parallel work only when scopes are disjoint.
- Sub-agents cannot choose the current main target, expand the stage, or let
  benchmarks/examples/tests define architecture.
- The lead agent owns integration, review, checks, staging, commit, push, and
  final completion judgement.
- If a blocker repeats across continuations, first look for another necessary
  code path inside the same main target. Report the goal blocked only when no
  meaningful system-code progress remains possible without external change.

## Documentation Rules

- When a real system capability subtarget is complete, update
  `docs/NEXT_STEPS.md` and `docs/PROGRESS.md`.
- Keep both files short and forward-looking.
- They should contain current state, one current main target, current code
  entrypoints, next entry, remaining risk, and the rule that goal-mode progress
  is by whole system capability loops, not small bug-sized changes.
- Do not append long history.
- `docs/NEXT_STEPS.md` must contain exactly one current main target.
- Update `AGENTS.md` or `docs/TURBOBUS_ROADMAP.md` only when long-term order or
  global rules truly change.

## Checks

- Do not add or run vLLM tests, benchmark runs, paper validation, server
  validation, or new alternative validation entrypoints in this stage.
- Run only minimal existing checks directly related to the current code change.
- For documentation-only changes, run `git diff --check`.
- For Python changes, prefer `python -m py_compile` on the changed production
  files.
- If CUDA, vLLM, multi-GPU, or server validation is unavailable, state it as a
  deferred external validation risk. Do not compensate by adding fake execution
  paths.

## Completion Judgement

The round is complete only when:

- `docs/NEXT_STEPS.md` and `docs/PROGRESS.md` agree on current state, current
  target, and next entry;
- the diff adds one independently describable system capability loop;
- daemon/scheduler remain the only production plan source;
- applications, benchmarks, adapters, workers, and CUDA executor still cannot
  choose route, relay, pool, target GPU, or relay GPU;
- no benchmark, example, paper-validation, server-validation, fake receipt,
  synthetic evidence, or dry-run deliverable was added;
- staged diff contains only files for this round;
- checks cover the direct risk of this round or state the external blocker.

## Commit And Push

- Before committing, inspect `git diff --cached`.
- Do not stage unrelated dirty files.
- After completing the current system capability subtarget, commit and push the
  current branch.
- If push fails for an external reason, report the reason and keep the local
  commit.

## Final Response

The final response must include:

1. Current main target selected at round start.
2. Completed full system subtarget.
3. Key files changed and responsibility changes.
4. Checks run and results.
5. Deferred validation risks.
6. Commit id and push result.
