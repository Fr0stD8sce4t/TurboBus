# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full framework adapter path through `TurboBusRuntimeSession` on top of
the now tighter daemon-owned execution and ownership contracts.

## Exit Criteria

- At least one production-facing adapter path registers real buffers through
  `TurboBusRuntimeSession`, submits `TransferIntent`, and consumes
  `TransferReceipt`.
- Adapter code stops bypassing runtime-session-owned submit/receipt flow or
  reintroducing route choice outside the daemon.
- Adapter closure lives on the production path and does not depend on
  benchmark-only or synthetic control paths.

## Current Code Work

- `turbobus/runtime_session.py`
- `turbobus/adapters/vllm.py`
- `turbobus/adapters/vllm_integration.py`
- `turbobus/adapters/model_loading.py`
- `turbobus/offload/context.py`

Round rules:

- Start each round with `git status`, then read `AGENTS.md`,
  `docs/TURBOBUS_ROADMAP.md`, `docs/NEXT_STEPS.md`, and `docs/PROGRESS.md`.
- Choose the single round target from this file first and `docs/PROGRESS.md`
  second.
- Finish one full system closure per round, not local bug-style fixes.
- State assumptions when they matter, prefer the simplest correct change, and
  keep edits surgical to the active target.
- Update this file and `docs/PROGRESS.md` after each completed closure.
- Keep only active and next work here. Do not append completed history.

## Next Entry

Start at `runtime_session.py` and the production-facing adapter entry points in
`adapters/vllm.py`, `adapters/vllm_integration.py`,
`adapters/model_loading.py`, and `offload/context.py`.

After the current target closes, the next round should finish exactly one of
these:

- one full server-backed validation closure after the system body is complete.
- one full adapter expansion closure for the next workload family on the same
  runtime-session production path.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
