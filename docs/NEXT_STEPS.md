# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G50 weight loading deep integration.

G49 is complete: vLLM KV save and restore now consume TransferReceipt and
daemon recovery state, preserving admission, queue, ticket, lease, buffer,
cleanup, and completion evidence in KV lifecycle records without exposing
physical route choices. The current target is weight loading deep integration.

## Current Code Work

- `turbobus/adapters/model_loading.py`: model weight loading adapter path.
- `turbobus/adapters/`: shared adapter contracts that submit TransferIntent
  and consume TransferReceipt.
- `turbobus/runtime_session.py`: production API for real buffer registration
  and receipt consumption.
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

Implement G50 as one complete production capability: model weight loading
should register real runtime buffers through `TurboBusRuntimeSession`, submit
H2D TransferIntent for weight movement, and consume TransferReceipt plus daemon
recovery evidence without exposing direct, relay, pool, target GPU, or relay GPU
choice to adapter callers.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction code-function queue.

Remaining auto-advance target queue: G50, G51, G52, G53, G54.
