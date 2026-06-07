# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full runtime-session-facing production vLLM connector lifecycle on
top of `VllmTurboBusIntegration`.

## Exit Criteria

- The production vLLM connector save/restore path uses one
  `VllmTurboBusIntegration` request lifecycle instead of rebuilding one-off
  `VllmKVSlotAdapter` flows around each operation.
- Request allocation recording, request registration, request restore/save,
  request stats, and request forget/cleanup stay on the
  `TurboBusRuntimeSession` path.
- The closure stays in production connector/adapter/runtime code and does not
  add benchmark-owned, example-owned, or dry-run compatibility paths.

## Current Code Work

- `turbobus/adapters/vllm_integration.py`
- `turbobus/adapters/vllm_kv_connector.py`
- `turbobus/adapters/vllm_backing_pool.py`
- `turbobus/adapters/vllm_prefix_store.py`
- `turbobus/runtime_session.py`

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

Start at `vllm_kv_connector.py` where prefix save/restore still creates
one-off adapter flows, then move through `vllm_integration.py`,
`vllm_backing_pool.py`, `vllm_prefix_store.py`, and `runtime_session.py` to
close one real connector-facing request/session lifecycle.

After the current target closes, the next round should finish exactly one of
these:

- one full runtime-session-facing closure for the next remaining production
  workload entry.
- one full validation/evaluation preparation closure only if system-body
  implementation no longer blocks it.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
