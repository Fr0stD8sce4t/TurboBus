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
- `turbobus/daemon/server.py`
- `turbobus/intent_executor.py`
- `turbobus/worker/lifecycle.py`

Current gap:

- system work must land as full capability closure, not isolated bug-style
  fixes;
- keep daemon scheduling as the only plan authority;
- build the remaining execution closures on top of the runtime-session-owned
  managed daemon/worker socket lifecycle;
- keep direct, relay, and mixed execution bound to one terminal
  receipt/cleanup path all the way through buffer lifetime closure.

## Next Entry

Start at `TurboBusRuntimeSession`, `daemon/server.py`, `intent_executor.py`,
and `worker/lifecycle.py`. The runtime-session-to-daemon/worker startup path is
now a managed production lifecycle; mixed pooled receipt evidence now carries
worker cleanup/staging lifecycle evidence. Push the next round into one full
relay-only or buffer-lifecycle closure rather than another local hardening step.
