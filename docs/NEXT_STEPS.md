# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G36 multi-tenant isolation strengthening.

Multi-tenant isolation must keep job, session, buffer, lease, relay staging,
execution ticket, cleanup, and worker authorization ownership bound to daemon
state while shared relay use remains cross-job safe.

## Current Code Work

- `turbobus/daemon/server.py`: peer ownership, transfer ownership, cleanup
  ownership, lease ownership, and cross-job relay authorization.
- `turbobus/daemon/peer_auth.py`: authenticated peer identity matching.
- `turbobus/worker/validation.py`: daemon-issued ticket and lease validation.
- `turbobus/worker/lifecycle.py`: worker cleanup and status reporting
  ownership evidence.

Round rules:

- Start each round with `git status`, then read `AGENTS.md`,
  `docs/TURBOBUS_ROADMAP.md`, `docs/NEXT_STEPS.md`, and `docs/PROGRESS.md`.
- Choose the single round target from this file first and `docs/PROGRESS.md`
  second.
- Finish one full system closure per round, not local bug-style fixes.
- Treat a round as complete only when the system gains one independently
  describable production capability on the current target path.
- Current stage only advances code functionality. Do not run functional
  validation, benchmark, example, paper validation, server validation, multi-GPU
  execution, new tests, mock gates, fake receipts, synthetic evidence, or
  dry-run deliverables.
- Update this file and `docs/PROGRESS.md` after each completed closure.
- Keep only active and next work here. Do not append completed history.

## Next Entry

After G36 is complete, continue automatically to G37 as the only current
target.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction code-function queue.

Remaining auto-advance target queue:

- G36 multi-tenant isolation strengthening.
- G37 vLLM KV code integration strengthening.
- G38 model loading code integration strengthening.
- G39 training offload code integration strengthening.
- G40 validation code entry recovery.
- G41 benchmark code recovery.
- G42 paper report code recovery.
