# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G22 mixed pooled execution hardening.

Mixed direct plus relay plans must execute as exact daemon-issued plans through
worker/backend code, preserve path-level timing and bytes, and expose unified
completion evidence for direct and relay chunks without application-side route
selection.

## Current Code Work

- `turbobus/worker/cuda_executor.py`: mixed worker/backend execution evidence.
- `turbobus/worker/lifecycle.py`: worker request, async execution, status
  report, cleanup, and completion aggregation.
- `turbobus/direct_fallback.py`: direct-only backend execution evidence.
- `turbobus/intent_executor.py`: daemon-issued mixed plan execution bridge.
- `cpp/src/executor_cuda.cu`: native mixed direct and relay path execution.

Round rules:

- Start each round with `git status`, then read `AGENTS.md`,
  `docs/TURBOBUS_ROADMAP.md`, `docs/NEXT_STEPS.md`, and `docs/PROGRESS.md`.
- Choose the single round target from this file first and `docs/PROGRESS.md`
  second.
- Finish one full system closure per round, not local bug-style fixes.
- Treat a round as complete only when the system gains one independently
  describable production capability on the current target path.
- Do not advance benchmark, example, paper-validation, server-validation, new
  test, dry-run, fake receipt, synthetic evidence, or replacement verification
  entry work during the current system-body pass.
- Update this file and `docs/PROGRESS.md` after each completed closure.
- Keep only active and next work here. Do not append completed history.

## Next Entry

After G22 is complete, continue automatically to G23 as the only current target.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction system queue.

Remaining auto-advance target queue:

- G22 mixed pooled execution hardening.
- G23 cross-job admission and fairness closure.
- G24 failure recovery and cleanup closure.
- G25 CUDA IPC lifecycle hardening.
- G26 vLLM real lifecycle closure.
- G27 model loading real integration closure.
- G28 training offload real integration closure.
- G29 unified reproduction evidence model.
- G30 real-execution validation and evaluation entry recovery.
