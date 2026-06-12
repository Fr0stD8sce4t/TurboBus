# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead
of appending history.

## Goal Mode Rule

Each round must derive one current main target from this file first, then
`docs/PROGRESS.md`, then `AGENTS.md`, then `docs/TURBOBUS_ROADMAP.md`.

If the files conflict, this file wins. Do not hard-code the target in prompts
or agent instructions. Stop after the current main target is closed.

Each round must deliver one complete system capability loop. A small bug fix,
field rename, helper move, import cleanup, boundary tightening, or
documentation update does not count as a completed round unless it is necessary
to finish the same capability loop.

Use `docs/GOAL_MODE_PROMPT.md` as the repeatable prompt body for the next
round. Keep it target-agnostic and let the four plan files choose the target.

## Current Main Target

Converge the production boundary around `TurboBusRuntimeSession` and adapter
lifecycle evidence.

Keep the single production entrypoint, remove duplicated runtime-looking paths,
and keep adapter validation bound to `TurboBusRuntimeSession` adapter evidence
records, snapshots, and real receipts. This is a system-code refactor target,
not a validation target.

## Current Code Work

- `turbobus/runtime_session.py`: keep the production entrypoint record as the
  source of session-level startup, registration, execution, receipt, adapter
  construction, adapter evidence, recovery, and close evidence.
- `turbobus/runtime/session_records.py`: keep session-entry snapshots and
  production boundary records aligned with the single entrypoint, including
  adapter context, adapter intent/receipt evidence records, recovery records,
  and close cleanup records.
- `turbobus/offload/lifecycle.py`: keep adapter lifecycle evidence bound to
  `TurboBusRuntimeSession` snapshots, adapter construction records, adapter
  evidence records, and real receipts.
- `turbobus/offload/handles.py`: keep low-level `ReceiptTransferHandle`
  submit/wait receipt consumption bound to RuntimeSession entrypoint records.
- `turbobus/runtime/evidence.py`: keep adapter lifecycle validation strict
  about `TurboBusRuntimeSession`, `TransferIntent`, `TransferReceipt`, and
  daemon scheduler policy source, and reject lifecycle evidence that was not
  recorded by the RuntimeSession entrypoint.
- `turbobus/adapters/`: remain consumers of `TurboBusRuntimeSession` evidence
  and must not create route, plan, relay, pool, or target-GPU policy.

Adapter migration is allowed only if it directly fixes a system-code boundary
bug found before validation starts.

## Next Entry

Continue the same production-boundary refactor in the current code path.
Do not start benchmark, example, paper validation, server validation, vLLM
validation, new tests, substitute validation entrypoints, fake receipt paths,
synthetic evidence, or dry-run deliverables.

Next inspect the remaining adapter lifecycle evidence contract assembly and
runtime evidence validation paths. Tighten any adapter evidence, receipt
contract, or route-policy path that can still be produced without being
present in the RuntimeSession entrypoint record before moving to the next
boundary.

## Round Rules

- Start with `git status`, then read `AGENTS.md`,
  `docs/TURBOBUS_ROADMAP.md`, `docs/NEXT_STEPS.md`, and
  `docs/PROGRESS.md`.
- Choose exactly one main target for the round from `Current Main Target`,
  `Current Code Work`, and `Next Entry`.
- Do not advance benchmark, example, paper validation, server validation, new
  tests, mock gates, fake receipts, synthetic evidence, or dry-run deliverables
  in the current stage.
- Keep daemon and scheduler as the only production source of transfer plans.
  Applications, benchmarks, adapters, workers, and CUDA executors must not
  choose direct, relay, pool, target GPU, or relay GPU.
- If a refactor or deletion is needed, continue to the same system capability
  loop instead of stopping at cleanup.
- After closing a real system capability loop, update this file and
  `docs/PROGRESS.md`.
- Keep this file short. It must contain one current main target only.
- If sub-agents are used, keep their scopes disjoint and let the lead agent
  integrate, verify, and decide whether the capability loop is closed.
- Before commit, confirm `git diff --cached` contains only the current round's
  files. Commit and push after the current target is closed. If push fails for
  an external reason, report it without changing the target definition.
- Final replies must include the chosen main target, completed system
  capability, key files, checks, deferred validation risk, commit id, and push
  result.

## Auto-Advance Policy

Auto-advance is allowed only within the current main target. After that target
is closed, stop the current round and update this file instead of starting the
next target in the same round.

In continuous goal mode, the next continuation must start a new round by
reading `docs/GOAL_MODE_PROMPT.md`, `AGENTS.md`, `docs/TURBOBUS_ROADMAP.md`,
`docs/NEXT_STEPS.md`, and `docs/PROGRESS.md`, then advance the newly selected
current target. Do not mark the whole goal complete because one round closed.

## Round Completion Standard

The current round is complete only when the plan files agree on a single
production-boundary refactor target and no benchmark, example, server-
validation, fake-evidence, synthetic-evidence, or dry-run path has been added.
If sub-agents are used, their work must stay in disjoint scopes and the lead
agent must make the final completion decision.
