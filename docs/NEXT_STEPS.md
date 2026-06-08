# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G32 CUDA mixed pooled strengthening.

CUDA worker/backend code must strengthen daemon-issued direct-only, relay-only,
and mixed direct+relay execution paths across H2D, D2H, multi-chunk, and
multi-relay plans. Completion evidence must preserve exact path split, native
path stats, relay device stats, cleanup, and byte accounting.

## Current Code Work

- `turbobus/worker/cuda_executor.py`: strengthen daemon exact-plan conversion,
  mixed pooled path accounting, native stats propagation, and cleanup evidence.
- `turbobus/native_plan.py`: keep native path conversion aligned with
  daemon-issued path kind, direction, relay device, and chunk ranges.
- `cpp/src/executor_cuda.cu`: strengthen direct, relay, and mixed pooled CUDA
  execution code without adding validation-only shortcuts.

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

After G32 is complete, continue automatically to G33 as the only current
target.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction code-function queue.

Remaining auto-advance target queue:

- G32 CUDA mixed pooled strengthening.
- G33 profile collection and daemon import closure.
- G34 scheduler cost model strengthening.
- G35 runtime feedback strengthening.
- G36 multi-tenant isolation strengthening.
- G37 vLLM KV code integration strengthening.
- G38 model loading code integration strengthening.
- G39 training offload code integration strengthening.
- G40 validation code entry recovery.
- G41 benchmark code recovery.
- G42 paper report code recovery.
