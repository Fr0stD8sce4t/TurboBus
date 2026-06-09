# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G46 buffer lifecycle pooling.

G45 is complete: worker execution now exposes resident async pool state,
queue/running/terminal snapshots, cancellation, drain, close, and failure
evidence for daemon-issued ticket work. The current target is to strengthen
buffer lifecycle pooling.

## Current Code Work

- `turbobus/runtime_session.py`: runtime-owned buffer registration, cleanup,
  and close behavior.
- `turbobus/runtime/buffers.py`: runtime buffer backing validation and
  registration metadata.
- `turbobus/buffer_registration.py`: executable buffer registration helpers.
- `turbobus/worker/resources.py`: worker-side shared pinned CPU and CUDA IPC
  resource open/close lifecycle.
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

Implement G46 as one complete production capability: shared pinned CPU buffers
and CUDA IPC buffers should have a clear pooled lifecycle with registration,
reference/lease ownership, execution use, cleanup, and receipt-facing evidence.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction code-function queue.

Remaining auto-advance target queue: G46, G47, G48, G49, G50, G51, G52, G53,
G54.
