# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G33 profile collection and daemon import closure.

Profile collection and import code must produce production measurement records
for direct PCIe, relay PCIe, and GPU-GPU fabric paths, normalize them into the
daemon profile format, and make daemon scheduler consumption explicit without
using synthetic profile evidence as production proof.

## Current Code Work

- `turbobus/profiling/bootstrap.py`: profile bootstrap from runtime/backend
  measurement into daemon profile import.
- `turbobus/profiling/daemon_format.py`: production profile schema validation
  for direct, relay, and fabric measurements.
- `turbobus/daemon/profiles.py`: daemon profile cache/import/invalidation
  behavior consumed by scheduler planning.
- `turbobus/scheduler/daemon.py`: scheduler profile consumption path.

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

After G33 is complete, continue automatically to G34 as the only current
target.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction code-function queue.

Remaining auto-advance target queue:

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
