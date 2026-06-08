# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G20 profile measurement closure.

Direct H2D/D2H, relay H2D/D2H, and GPU-GPU fabric profile measurement must
produce daemon-ingested profile records that are bound to the trusted topology
snapshot from G19. The profile path must remain production code and must not add
benchmark, example, paper-validation, dry-run, fake receipt, or synthetic
evidence entry points.

## Current Code Work

- `cpp/src/profiler_cuda.cu`: native CUDA direct, relay, and fabric timing.
- `turbobus/profiling/bootstrap.py`: runtime-session profile collection and
  daemon profile install path.
- `turbobus/profiling/daemon_format.py`: daemon profile schema validation.
- `turbobus/backends/cuda.py`: native profile bridge.
- `turbobus/daemon/server.py`: profile cache, topology binding, and scheduler
  profile lookup.

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

After G20 is complete, continue automatically to G21 as the only current target.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction system queue.

Remaining auto-advance target queue:

- G20 profile measurement closure.
- G21 paper-grade scheduler cost model.
- G22 mixed pooled execution hardening.
- G23 cross-job admission and fairness closure.
- G24 failure recovery and cleanup closure.
- G25 CUDA IPC lifecycle hardening.
- G26 vLLM real lifecycle closure.
- G27 model loading real integration closure.
- G28 training offload real integration closure.
- G29 unified reproduction evidence model.
- G30 real-execution validation and evaluation entry recovery.
