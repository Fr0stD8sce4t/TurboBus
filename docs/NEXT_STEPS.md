# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead
of appending history.

## Goal Mode Rule

Each round must derive one current main target from this file first, then
`docs/PROGRESS.md`, then `AGENTS.md`, then `docs/TURBOBUS_ROADMAP.md`.

Each round must close one complete system capability loop. Small fixes, helper
moves, field renames, import cleanup, and documentation updates do not count
unless they finish the same capability loop.

Do not start benchmark, example, paper validation, server validation, vLLM
validation, new tests, substitute validation entrypoints, fake receipt paths,
synthetic evidence, or dry-run deliverables in the current stage.

## Current Main Target

Converge the production boundary around `TurboBusRuntimeSession` and adapter
lifecycle evidence.

Keep the single production entrypoint, remove duplicated runtime-looking paths,
and keep adapter validation bound to `TurboBusRuntimeSession` adapter evidence
records, snapshots, and real receipts.

## Current Code Work

- `turbobus/runtime_session.py`: remain the production entrypoint for startup,
  registration, transfer submission, receipt consumption, adapter construction,
  recovery, close, and session-level evidence.
- `turbobus/runtime/session_records.py`: keep entrypoint snapshots aligned with
  adapter context, intent, receipt, adapter evidence, recovery, and cleanup
  records.
- `turbobus/offload/lifecycle.py`: expose only RuntimeSession-bound adapter
  lifecycle and receipt trace helpers. Raw receipt trace extraction and handle
  receipt collection stay private.
- `turbobus/offload/handles.py`: keep low-level handle submit/wait bindings
  tied to RuntimeSession entrypoint records through offload-internal helpers.
- `turbobus/runtime/evidence.py`: continue rejecting missing RuntimeSession,
  missing adapter evidence records, fake evidence, exposed route policy, and
  lifecycle identity drift.
- `turbobus/adapters/`: consume RuntimeSession-bound evidence only. Adapters
  must not create route, plan, relay, pool, or target-GPU policy.

## Next Entry

Continue the same production-boundary refactor in the current code path. Next
inspect remaining adapter-facing lifecycle, cleanup, recovery, and close
summaries in `turbobus/adapters/`, `turbobus/offload/`, and `turbobus/runtime/`.
Tighten any path that can emit runtime-looking evidence without a
`TurboBusRuntimeSession` entrypoint adapter evidence record.

## Remaining Risk

- Hardware-backed CUDA, vLLM, multi-GPU, server, benchmark, and paper
  validation remain deferred.
- PCIe load is still derived from daemon active path state until hardware
  counter sampling is added later.
- Goal-mode rounds must still inspect `git status`, avoid unrelated dirty
  files, and stage only the active target.

## Completion Standard

The current round is complete only when the code closes one production-boundary
capability loop, `docs/NEXT_STEPS.md` and `docs/PROGRESS.md` agree, no
benchmark or validation substitute was added, minimal relevant checks passed,
and the staged diff contains only this round's files.
