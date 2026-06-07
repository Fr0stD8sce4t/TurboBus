# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Close one full daemon-issued mixed direct + relay execution path into one valid
`TransferReceipt`.

## Exit Criteria

- One scheduler-issued mixed plan executes all direct chunks and relay chunks
  instead of collapsing to one path family.
- Worker and backend terminal completion merges into one receipt path with
  direct/relay split evidence or explicit failure evidence.
- The closure stays on daemon/runtime/worker/backend production boundaries and
  does not move route choice into adapters, benchmarks, or examples.

## Current Code Work

- `turbobus/daemon/server.py`
- `turbobus/worker/cuda_executor.py`
- `turbobus/worker/lifecycle.py`
- `turbobus/backends/cuda.py`
- `turbobus/native_runtime.py`
- `cpp/src/executor_cuda.cu`

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

Start at `daemon/server.py`, then follow mixed-plan execution through
`worker/cuda_executor.py`, `worker/lifecycle.py`, `backends/cuda.py`,
`native_runtime.py`, and `cpp/src/executor_cuda.cu`.

After the current target closes, the next round should finish exactly one of
these:

- one full buffer registration to execution to cleanup to receipt lifecycle
  closure only if mixed execution no longer blocks the main system body.
- one full runtime-session-facing adapter expansion closure for another real
  workload family only if mixed execution no longer blocks the main system
  body.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
