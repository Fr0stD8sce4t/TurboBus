# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G47 multi-tenant fairness admission.

G46 is complete: runtime sessions now maintain a pooled buffer lifecycle record
for shared pinned CPU and CUDA IPC buffers across daemon registration, intent
use, receipt finalization, cleanup, close, and receipt-facing retention
evidence. The current target is to close daemon-side fairness admission.

## Current Code Work

- `turbobus/daemon/server.py`: transfer admission, job/session ownership,
  queued/running state, buffer protection, and rejection evidence.
- `turbobus/scheduler/load.py`: runtime load view used by admission and
  scheduling policy.
- `turbobus/scheduler/daemon.py`: scheduler-facing admission inputs and
  metadata.
- `docs/PROGRESS.md`: current completed state and deferred validation risk.

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

Implement G47 as one complete production capability: daemon admission should use
multi-tenant job/session, queued/running transfer, active lease, and buffer
ownership state to accept, queue, or reject TransferIntent requests with
receipt-facing admission evidence.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction code-function queue.

Remaining auto-advance target queue: G47, G48, G49, G50, G51, G52, G53, G54.
