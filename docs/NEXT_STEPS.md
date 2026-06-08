# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G38 model loading code integration strengthening.

Model-loading integration code must submit model-weight H2D work through
`TurboBusRuntimeSession` using registered buffers, `TransferIntent`, and
`TransferReceipt` consumption without exposing direct, relay, target-GPU, or
relay-GPU route selection to adapter callers.

## Current Code Work

- `turbobus/adapters/model_loading.py`: model-weight loader entry points,
  manifest binding, and receipt-backed load lifecycle.
- `turbobus/offload/context.py` and `turbobus/offload/store.py`: shared adapter
  context, registered buffer use, physical-policy rejection, and receipt
  consumption.
- `turbobus/runtime_session.py`: production runtime-session factory for
  model-loading adapters.

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

After G38 is complete, continue automatically to G39 as the only current
target.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction code-function queue.

Remaining auto-advance target queue:

- G38 model loading code integration strengthening.
- G39 training offload code integration strengthening.
- G40 validation code entry recovery.
- G41 benchmark code recovery.
- G42 paper report code recovery.
