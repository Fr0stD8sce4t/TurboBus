# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G53 backend extensibility interface.

G52 is complete: topology and fabric abstraction now carries daemon-discovered
PCIe, NUMA, and scale-up fabric capability summaries into scheduler and profile
metadata for direct, relay, and mixed pooled plans without synthetic production
topology. The current target is backend extensibility interface.

## Current Code Work

- `turbobus/backends/cuda.py`: CUDA backend contract and runtime operations.
- `turbobus/native_runtime.py`: native runtime plan execution boundary.
- `turbobus/worker/cuda_executor.py`: worker-side backend execution wrapper.
- `turbobus/intent_executor.py`: exact daemon-issued plan execution bridge.
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

Implement G53 as one complete production capability: backend execution should
expose a clear extensibility interface for direct, relay, and mixed pooled exact
daemon-issued plans while keeping CUDA as the current production backend and
without adding ROCm/HIP work or alternate validation entrypoints.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction code-function queue.

Remaining auto-advance target queue: G53, G54.
