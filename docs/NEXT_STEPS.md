# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G21 paper-grade scheduler cost model.

Scheduler decisions must use measured daemon profiles, trusted topology
bindings, live queue/running/admitted state, relay pressure, and job priority to
produce explainable direct, relay, or mixed pooled path splits. The scheduler
must remain the only production source of transfer plans.

## Current Code Work

- `turbobus/scheduler/daemon.py`: scheduler cost model, profile consumption,
  path cost metadata, relay policy, and fallback behavior.
- `turbobus/scheduler/load_feedback.py`: runtime pressure, fairness, queue,
  worker/backend feedback, and relay-load accounting.
- `turbobus/planner_engine.py`: chunk-to-path allocation using measured path
  costs.
- `turbobus/daemon/server.py`: runtime state snapshots and scheduler inputs.

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

After G21 is complete, continue automatically to G22 as the only current target.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction system queue.

Remaining auto-advance target queue:

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
