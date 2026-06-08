# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G25 CUDA IPC lifecycle hardening.

CUDA IPC GPU buffers must be registered, authorized, opened, used, and closed
only inside daemon-issued execution paths, with lifecycle evidence bound to
job/session/ticket ownership.

## Current Code Work

- `turbobus/buffer_registration.py`: CUDA IPC handle conversion and ownership
  evidence for runtime buffers.
- `turbobus/worker/resources.py`: worker data-plane resource binding, CUDA IPC
  open/close evidence, and span validation.
- `turbobus/runtime_session.py`: runtime-owned CUDA buffer registration and
  release.
- `turbobus/daemon/server.py`: buffer ownership, active lease/ticket
  protection, and cleanup retention.
- `turbobus/worker/validation.py`: ticket and buffer owner checks.

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

After G25 is complete, continue automatically to G26 as the only current target.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction system queue.

Remaining auto-advance target queue:

- G25 CUDA IPC lifecycle hardening.
- G26 vLLM real lifecycle closure.
- G27 model loading real integration closure.
- G28 training offload real integration closure.
- G29 unified reproduction evidence model.
- G30 real-execution validation and evaluation entry recovery.
