# TurboBus Progress

## Current State

Current main target: real H2D / D2H execution path closure.

The daemon-first path remains the production route:
`TransferIntent` submission, daemon scheduling, daemon-issued
`ExecutionTicket`, worker/backend completion, and `TransferReceipt`
consumption through `TurboBusRuntimeSession`.

Server validation, benchmark work, paper validation, experiments, and new test
code remain deferred until the full system implementation pass is complete.

## Completed This Round

- `TurboBusRuntimeSession` now owns the sole adapter construction path for
  the offload, inference, and vLLM adapters that feed `fetch_h2d()` /
  `offload_d2h()`, the last `AdapterTransferContext.from_runtime_session`
  wrapper was removed, and the vLLM adapter no longer constructs transfer
  contexts itself.
- The vLLM KV connector now calls `runtime_session.make_vllm_kv_slot_adapter()`
  directly, and the old `VllmKVSlotAdapter.from_runtime_session()` wrapper was
  removed.
- The remaining offload, inference, training, and vLLM integration
  `from_runtime_session()` wrappers were removed so the session-owned factories
  are the only construction path.
- `TurboBusRuntimeSession.open_session()` now bootstraps the daemon profile and
  installs it with `put_profile` before the first transfer path is used.
- `bootstrap_profile()` now returns a no-op success response when the session
  profile is already installed and `force=False`.
- `TurboBusRuntimeSession.close()` now cleans daemon-registered buffers before
  closing the session, instead of only dropping local references.
- `TurboBusRuntimeSession` now exposes a session-owned worker intent executor
  factory and uses it for the daemon-issued transfer path.
- Kept the round free of new test, experiment, benchmark, paper-validation,
  server-validation, or compatibility export-layer code.

## Validation

- Not run yet for this turn.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Profile bootstrap still depends on CUDA/backend behavior and daemon profile
  RPCs that have not been server-verified in this session.
- Buffer cleanup on close still depends on daemon cleanup RPC success for the
  registered buffers in the active session.
- The worker intent executor remains dependent on the worker client and
  runtime buffer map being live inside the session.
- Older benchmark and example surfaces still use `TurboBusClient` and have not
  been migrated to the runtime-session-first API yet.
- Existing tests, examples, and benchmarks still contain old production-path
  assumptions; current-stage constraints defer migration until system
  implementation is complete.

## Next Main Target

Continue with one concrete implementation boundary: real H2D / D2H execution
path closure.
