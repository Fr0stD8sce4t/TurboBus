# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

Finish the core system body before adapter migration, benchmarks, or paper
evaluation.

- `TurboBusRuntimeSession` is the only production system entry;
- one `TransferIntent` maps to one daemon-owned scheduling lifecycle;
- direct, relay, and mixed pooled execution stay daemon-issued outcomes;
- execution, terminal receipt, and cleanup stay one contract.

## Exit Criteria

- Runtime session owns startup, registration, intent submission, execution, and
  receipt consumption.
- Daemon transfer lifecycle stays explicit from admission through ticket,
  execution status, terminal receipt, and cleanup.
- Direct-only, relay-only, and mixed plans share one completion-evidence
  contract.
- Shared pinned CPU and CUDA IPC GPU buffer lifetime stays inside the
  daemon-issued session/job lifecycle.

## Current Code Work

- `turbobus/runtime_session.py`
- `turbobus/api.py`
- `turbobus/daemon/server.py`
- `turbobus/intent_executor.py`

Current gap:

- remove remaining duplicate production-looking entry paths;
- keep daemon scheduling as the only plan authority;
- keep direct, relay, and mixed execution bound to one receipt/cleanup path.

## Next Entry

Start at `TurboBusRuntimeSession`, `api.py`, `daemon/server.py`, and
`intent_executor.py`. Prefer changes that further collapse production entry
ownership and execution lifecycle ownership. Do not spend this pass on
benchmarks, examples, adapters, or validation tooling.
