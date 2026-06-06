# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full adapter expansion path for the next workload family on the same
`TurboBusRuntimeSession` production path.

## Exit Criteria

- Another production-facing workload family closes on top of the same
  runtime-session-owned submit/receipt path, not just the offload-style path
  already in place.
- Adapter expansion continues to use `TurboBusRuntimeSession` for real buffer
  registration, `TransferIntent` submission, and `TransferReceipt`
  consumption without route choice leakage.
- Expansion stays on the production path and does not depend on benchmark-only
  or synthetic control paths.

## Current Code Work

- `turbobus/runtime_session.py`
- `turbobus/adapters/vllm_integration.py`
- `turbobus/adapters/vllm.py`
- `turbobus/adapters/vllm_kv_connector.py`
- `turbobus/adapters/training_offload.py`
- `turbobus/adapters/model_loading.py`

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

Start at `runtime_session.py` and the next adapter workload family in
`adapters/vllm_integration.py`, `adapters/vllm.py`,
`adapters/vllm_kv_connector.py`, `adapters/training_offload.py`, and
`adapters/model_loading.py`.

After the current target closes, the next round should finish exactly one of
these:

- one full server-backed validation closure after the system body is complete.
- one full server/runtime production-startup hardening closure if adapters no
  longer block the main system path.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
