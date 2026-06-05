# TurboBus Progress

## Current State

Current main target: real H2D / D2H execution path closure.

The codebase has a daemon-first control-plane shape: `TransferIntent`
submission, daemon scheduling, daemon-issued `ExecutionTicket`, worker/backend
completion reporting, and `TransferReceipt` consumption through
`TurboBusRuntimeSession`.

The largest remaining implementation gap is mixed pooled execution. Direct-only
backend execution and relay worker execution exist as separate paths, but a
single pooled plan must still execute both direct chunks and relay chunks,
merge their completion evidence, clean up state, and produce one receipt.

Server validation, benchmark work, paper validation, experiments, and new test
code remain deferred until the full system implementation pass is complete.

## Completed Recently

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

- `git diff --check` passed for the current documentation update, with CRLF
  normalization warnings on edited files.
- Existing CUDA/native execution, vLLM runtime behavior, relay/pooled
  execution, and server-only behavior remain unverified in this session.

## Remaining Risk

- Mixed pooled direct-plus-relay execution is not yet a complete single
  transfer lifecycle.
- Direct fallback and relay worker execution may still report completion on
  separate control paths instead of one daemon transfer receipt.
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

Complete daemon-issued mixed pooled H2D / D2H execution through
`WorkerIntentTransferExecutor`, worker/backend completion, cleanup, and
receipt generation.
