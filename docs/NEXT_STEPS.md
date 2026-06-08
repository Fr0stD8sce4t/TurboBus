# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G23 cross-job admission and fairness closure.

Daemon admission and scheduling must account for live cross-job queued,
running, active, lease, and relay pressure so pooled PCIe sharing remains
daemon-owned, fair across jobs, and isolated by job/session ownership.

## Current Code Work

- `turbobus/daemon/server.py`: transfer admission, relay reservation, live
  transfer accounting, and runtime feedback.
- `turbobus/scheduler/daemon.py`: measured cost, runtime pressure, job policy,
  and relay fairness weighting.
- `turbobus/scheduler/load_feedback.py`: queued/running/active load model.
- `turbobus/daemon/leases.py`: relay quota, reservation, lease ownership, and
  cleanup records.
- `turbobus/worker/validation.py`: ticket, lease, and owner binding checks.

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

After G23 is complete, continue automatically to G24 as the only current target.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction system queue.

Remaining auto-advance target queue:

- G23 cross-job admission and fairness closure.
- G24 failure recovery and cleanup closure.
- G25 CUDA IPC lifecycle hardening.
- G26 vLLM real lifecycle closure.
- G27 model loading real integration closure.
- G28 training offload real integration closure.
- G29 unified reproduction evidence model.
- G30 real-execution validation and evaluation entry recovery.
