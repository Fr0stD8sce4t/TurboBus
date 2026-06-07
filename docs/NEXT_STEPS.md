# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full runtime-session-facing adapter expansion for the inference KV
workload family through `InferenceKVSlotAdapter`.

## Exit Criteria

- Inference KV adapter entry stays on `TurboBusRuntimeSession` and does not
  bypass daemon-issued transfer intent, receipt consumption, or runtime-owned
  buffer registration.
- `InferenceKVSlotAdapter` and its store/context path use the same production
  runtime-session submit, wait, cleanup, and state model as the closed core
  transfer path.
- The closure stays in production adapter/runtime code and does not add
  benchmark-owned or example-owned compatibility paths.

## Current Code Work

- `turbobus/runtime_session.py`
- `turbobus/adapters/inference.py`
- `turbobus/offload/store.py`
- `turbobus/offload/context.py`
- `turbobus/offload/blocks.py`

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

Start at `runtime_session.py`, then follow
`make_inference_kv_slot_adapter()` through `adapters/inference.py`,
`offload/store.py`, `offload/context.py`, and `offload/blocks.py` to close one
real runtime-session-facing inference KV path.

After the current target closes, the next round should finish exactly one of
these:

- one full runtime-session-facing adapter expansion closure for another real
  workload family.
- one full validation/evaluation preparation closure only if system-body
  implementation no longer blocks it.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
