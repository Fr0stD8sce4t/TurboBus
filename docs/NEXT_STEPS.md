# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Finish the core system body before adapter migration, benchmarks, or paper
evaluation.

- `TurboBusRuntimeSession` is the only production entry.
- One `TransferIntent` maps to one daemon-owned lifecycle.
- Direct, relay, and mixed execution stay daemon-issued outcomes.
- Execution, receipt, and cleanup stay one contract.

## Exit Criteria

- Runtime session owns startup, registration, intent submission, and receipt use.
- Daemon lifecycle stays explicit from admission to cleanup.
- Direct, relay, and mixed plans share one completion contract.
- Shared pinned CPU and CUDA IPC GPU buffers stay inside daemon-issued lifetime.

## Current Code Work

- `turbobus/runtime_session.py`
- `turbobus/daemon/server.py`
- `turbobus/intent_executor.py`
- `turbobus/worker/lifecycle.py`
- `turbobus/daemon/receipts.py`

Current gap:

- land one full system capability per round, not local bug-style fixes;
- keep daemon scheduling as the only plan authority;
- finish the remaining execution closures on top of the managed runtime-session
  daemon/worker socket path;
- keep direct, relay, and mixed execution bound to one receipt/cleanup path
  after the buffer lifetime closure now reaches final receipts.

## Next Entry

Start at `TurboBusRuntimeSession`, `daemon/server.py`, `intent_executor.py`,
and `worker/lifecycle.py`.

Next round should finish exactly one of these:

- one full relay-only execution closure;
- one full runtime-session-owned execution and cleanup closure.
- one full daemon-owned direct / relay / mixed completion contract closure.

Plan-file rule:

- after each real system sub-goal, rewrite this file to the new current target;
- keep only the active target, active code entry, and next closure candidates;
- do not append completed work history here.
