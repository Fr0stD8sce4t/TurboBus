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

- `python -m py_compile turbobus/intent_executor.py
  turbobus/intent_execution_support.py turbobus/direct_fallback.py
  turbobus/worker/lifecycle.py turbobus/worker/cuda_executor.py
  turbobus/daemon/server.py turbobus/daemon/receipts.py` passed.
- `git diff --check` passed for the current code and documentation update,
  with CRLF normalization warnings on edited files.
- Existing CUDA/native execution, vLLM runtime behavior, relay/pooled
  execution, and server-only behavior remain unverified in this session.

## Remaining Risk

- The production worker socket request/envelope path still needs the
  deferred-terminal mixed pooled mode that the in-process worker client now
  supports.
- Mixed pooled completion still needs full end-to-end CUDA/server confirmation
  after the system path is complete.
- Runtime-state feedback still depends on server-side observation paths that
  have not been server-verified in this session.
- Profile bootstrap still depends on CUDA/backend behavior and daemon profile
  RPCs that have not been server-verified in this session.
- Shared pinned CPU and CUDA IPC GPU buffer lifecycle behavior still needs
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

Carry deferred-terminal mixed pooled execution through the production worker
socket path without adding application-side route controls.
