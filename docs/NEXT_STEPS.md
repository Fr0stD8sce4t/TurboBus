# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G37 vLLM KV code integration strengthening.

vLLM KV integration code must submit KV-cache H2D/D2H work through
`TurboBusRuntimeSession` using registered buffers, `TransferIntent`, and
`TransferReceipt` consumption without exposing direct, relay, target-GPU, or
relay-GPU route selection to adapter callers.

## Current Code Work

- `turbobus/adapters/vllm.py`: vLLM-facing KV adapter entry points.
- `turbobus/adapters/vllm_kv_connector.py`: KV connector transfer submission
  through runtime session.
- `turbobus/adapters/vllm_integration.py`: vLLM integration helpers and
  runtime-session binding.
- `turbobus/offload/context.py` and `turbobus/offload/store.py`: shared adapter
  context, registered buffer use, and receipt consumption.

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

After G37 is complete, continue automatically to G38 as the only current
target.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction code-function queue.

Remaining auto-advance target queue:

- G37 vLLM KV code integration strengthening.
- G38 model loading code integration strengthening.
- G39 training offload code integration strengthening.
- G40 validation code entry recovery.
- G41 benchmark code recovery.
- G42 paper report code recovery.
