# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Goal Mode Rule

Each round must derive one current main target from this file first, then
`docs/PROGRESS.md`, then `AGENTS.md`, then `docs/TURBOBUS_ROADMAP.md`.

If the files conflict, this file wins. Do not hard-code the target in prompts or
agent instructions. Stop after the current main target is closed.

Each round must deliver one complete system capability loop. A small bug fix,
field rename, helper move, import cleanup, boundary tightening, or documentation
update does not count as a completed round unless it is necessary to finish the
same capability loop.

Use `docs/GOAL_MODE_PROMPT.md` as the repeatable prompt body for the next
round. Keep it target-agnostic and let the four plan files choose the target.

## Current Main Target

Build daemon-issued block runtime, tickets, leases, progress, cleanup, and
receipts on top of block-level scheduling.

The target is complete only when the daemon turns a block plan into per-block
tickets or leases, tracks queued/running/completed/failed block state, records
progress and cleanup ownership, and emits receipts from real block completion
or explicit failure.

## Current Code Work

- `turbobus/daemon/block_runtime.py`: define daemon-owned block runtime state
  and per-block lifecycle transitions.
- `turbobus/daemon/cleanup_helpers.py`: extend cleanup ownership handling for
  block-scoped records.
- `turbobus/daemon/server.py`: issue block runtime records, progress, and
  receipts from block plans.
- `turbobus/daemon/receipts.py`: convert block-level completion evidence into
  receipts.
- `turbobus/worker/lifecycle.py` and `turbobus/worker/cuda_executor.py`: carry
  daemon-issued block runtime identifiers through execution and completion.
- `turbobus/runtime_session.py`: keep block runtime hidden behind the
  production session API.

Adapter migration is allowed only if one of these paths directly blocks the
current target.

## Next Entry

Start by defining daemon block runtime records and the ticket/lease lifecycle,
then connect block completion and cleanup into receipt emission. Stop after the
daemon can describe block progress and terminal receipts for direct-only,
relay-only, and mixed pooled block plans.

The round should stay inside this loop until the daemon can show one coherent
block runtime story from scheduling decision to terminal receipt.

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
  choose direct, relay, pool, target GPU, or relay GPU routes.
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
is closed, stop and update this file instead of starting the next target.

## Round Completion Standard

The current round is complete only when the daemon-owned block runtime can be
described as one path from scheduled block plan to terminal receipt, and the
final diff is limited to files needed for that path. If sub-agents are used,
their work must stay in disjoint scopes and the lead agent must make the final
completion decision.
