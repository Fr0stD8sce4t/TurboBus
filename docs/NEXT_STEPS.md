# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G13 runtime feedback metrics closure.

Daemon runtime feedback must preserve worker/backend completion metrics,
execution-path evidence, cleanup evidence, async worker pool state, CUDA IPC
span validation, and recent terminal feedback in one scheduler-consumable view.
The feedback path must remain derived from real worker/backend status updates
and daemon receipts, not synthetic evidence or dry-run artifacts.

## Current Code Work

- `turbobus/daemon/server.py`: runtime resource state, terminal feedback
  records, completion evidence aggregation, and scheduler feedback summaries.
- `turbobus/runtime/validation.py`: receipt validation expectations for
  runtime feedback evidence.
- `turbobus/scheduler/load_feedback.py`: scheduler-consumable feedback fields
  and runtime pressure inputs.

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

After G13 is complete, advance to G14 vLLM KV lifecycle closure.

## Auto-Advance Policy

Auto-advance is active for the system-body queue.

Remaining auto-advance target queue:

- G13 runtime feedback metrics closure.
- G14 vLLM KV lifecycle closure.
- G15 prefix store productionization.
- G16 model loading takeover.
- G17 training-state offload closure.
- G18 unified auditable receipt closure.

In auto-advance mode:

- keep exactly one current active target at a time;
- after each completed target, rewrite this file and `docs/PROGRESS.md` so the
  next queued target becomes the only current active target;
- carry forward the no-benchmark, no-example, no-test, no-fake-evidence, and
  daemon-issued-plan constraints;
- stop when the queue is complete, external environment blocks the target, a
  real architecture choice needs user review, or continuing would leave the
  system-body scope.
