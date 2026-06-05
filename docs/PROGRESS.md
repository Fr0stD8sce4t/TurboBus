# TurboBus Progress

## Current State

Current main target: real H2D / D2H execution path closure.

The codebase has a daemon-first control-plane shape: `TransferIntent`
submission, daemon scheduling, daemon-issued `ExecutionTicket`, worker/backend
completion reporting, and `TransferReceipt` consumption through
`TurboBusRuntimeSession`.

The in-process runtime path now has mixed pooled execution: one daemon plan is
split by assignment type, direct chunks execute through backend exact-plan code,
relay chunks execute through worker authorization and cleanup, and
`WorkerIntentTransferExecutor` reports one merged daemon completion.

The production worker socket path now carries the same deferred-terminal mode:
socket request envelopes preserve whether the worker should report terminal
daemon status, and `WorkerTransferService` passes that choice into the worker
lifecycle so mixed pooled relay completion can be returned for executor-side
merge.

Daemon terminal completion now keeps executed direct plus relay evidence in
receipt metadata and runtime feedback summaries. Runtime feedback records
terminal executed direct/relay bytes from completion evidence rather than
static plan output.

Shared pinned CPU and CUDA IPC GPU buffer lifecycle evidence now reaches the
same completion path. Worker resource close state is merged into worker
completion evidence, direct backend completion records CUDA host unregister
state, and `TurboBusRuntimeSession` keeps session-owned CPU buffer cleanup and
release evidence on explicit cleanup and session close.

Production worker socket startup now performs a daemon handshake before
serving requests: it fetches daemon-owned topology inventory and daemon
identity state, rejects synthetic production topology, and attaches worker
startup evidence to worker completion or explicit failure metadata.

Server validation, benchmark work, paper validation, experiments, and new test
code remain deferred until the full system implementation pass is complete.

## Completed Recently

- Worker and direct-backend completion evidence now carries executed path split
  metadata (`direct_bytes`, `relay_bytes`, direct/relay chunk counts, executor,
  path, target, relay, and buffer identity) into daemon-normalized completion
  evidence and `TransferReceipt.metadata`.
- `WorkerIntentTransferExecutor` now executes daemon-issued mixed pooled plans
  in the in-process worker-client path by combining direct backend completion
  evidence with deferred-terminal relay worker completion evidence before the
  daemon receives one terminal complete update.
- Worker socket request envelopes now preserve `report_terminal_status`, and
  socket worker clients can participate in mixed pooled direct-plus-relay
  execution without independently completing the whole transfer.
- Daemon completion evidence now preserves mixed direct and relay child
  completion records, exposes direct/relay evidence on receipts, and summarizes
  terminal executed direct/relay bytes in runtime feedback.
- Shared pinned CPU and CUDA IPC GPU buffer lifecycle evidence now records
  worker close state, direct backend CUDA host unregister state, and
  runtime-owned CPU buffer release results during cleanup and session close.
- Worker socket process startup now binds to daemon-owned topology inventory,
  rejects synthetic topology sources, records daemon-observed peer identity
  where available, and carries that startup evidence into worker result and
  daemon completion evidence.
- Worker CUDA execution scopes relay work to authorized relay assignments, while
  direct backend execution scopes native plans to direct assignments from the
  same daemon-issued ticket.
- `TurboBusRuntimeSession` owns session/job/buffer registration, profile
  bootstrap, worker intent executor construction, H2D/D2H submission helpers,
  adapter factory construction, and owned CPU buffer cleanup.
- Daemon runtime feedback records terminal worker/backend evidence and exposes
  live queued/running/active transfer state to scheduler metadata.
- CUDA backend and worker executor execute exact daemon plans rather than
  choosing direct, relay, or pool routes locally.
- Offload, inference, training, model-loading, and vLLM adapter construction
  has moved toward `TurboBusRuntimeSession` factories and receipt consumption.

## Validation

- `python -m py_compile turbobus/intent_execution_support.py
  turbobus/worker/models.py turbobus/worker/codec.py
  turbobus/worker/lifecycle.py turbobus/worker/socket_client.py
  turbobus/worker/endpoint.py` passed.
- `python -m py_compile turbobus/daemon/server.py
  turbobus/daemon/receipts.py` passed.
- `python -m py_compile turbobus/worker/resources.py
  turbobus/worker/lifecycle.py turbobus/direct_fallback.py
  turbobus/runtime_session.py turbobus/daemon/server.py
  turbobus/daemon/receipts.py` passed.
- `python -m py_compile turbobus/daemon/dispatch.py
  turbobus/daemon/server.py turbobus/daemon/client.py
  turbobus/worker/__init__.py turbobus/worker/process.py
  turbobus/worker/lifecycle.py turbobus/worker/endpoint.py
  turbobus/worker/socket_client.py turbobus/intent_executor.py` passed.
- `git diff --check` passed for the current code and documentation update,
  with CRLF normalization warnings on edited files.
- Existing CUDA/native execution, vLLM runtime behavior, relay/pooled
  execution, and server-only behavior remain unverified in this session.

## Remaining Risk

- The production worker socket startup and deferred-terminal paths now carry
  daemon topology and completion evidence through code paths, but still need
  full end-to-end CUDA/server confirmation after the system path is complete.
- Mixed pooled completion still needs full end-to-end CUDA/server confirmation
  after the system path is complete.
- Runtime feedback now includes terminal executed path evidence, but its
  server-side observation path has not been server-verified in this session.
- Profile bootstrap still depends on CUDA/backend behavior and daemon profile
  RPCs that have not been server-verified in this session.
- Shared pinned CPU and CUDA IPC GPU buffer lifecycle evidence is now wired
  through code paths, but real CUDA IPC/shared-memory behavior still needs
  end-to-end confirmation after the system path is complete.
- The worker intent executor remains dependent on the worker client and runtime
  buffer map being live inside the session.
- Worker execution still depends on CUDA backend/device handle support in the
  active runtime environment.
- Adapter context creation still depends on callers providing valid CPU and GPU
  buffers that can be registered against the active daemon session.
- vLLM adapter setup still depends on real vLLM tensors and buffer backings in
  the active runtime environment.
- Older benchmark and example surfaces still use `TurboBusClient` and have not
  been migrated to the runtime-session-first API.
- Existing tests, examples, and benchmarks still contain old production-path
  assumptions; migration is deferred until system implementation is complete.

## Next Main Target

Close scheduler load feedback and relay isolation so daemon planning consumes
real queued/running/active transfer state, relay leases, staging usage,
completion source history, and job weights.
