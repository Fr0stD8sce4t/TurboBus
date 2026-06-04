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

- `daemon/server.py` now advances `_runtime_state_version` when a session is
  actually closed, so session/job/buffer retirement shows up in the runtime
  state that scheduler feedback reads.
- Kept the round free of new test, experiment, benchmark, paper-validation,
  server-validation, or compatibility export-layer code.

## Validation

- `git diff --check` passed with CRLF normalization warnings on the edited
  files.
- `python -m py_compile turbobus/daemon/server.py` passed.

## Remaining Risk

- Runtime-state feedback still depends on server-side observation paths that
  have not been server-verified in this session.
- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Profile bootstrap still depends on CUDA/backend behavior and daemon profile
  RPCs that have not been server-verified in this session.
- Buffer cleanup on close still depends on daemon cleanup RPC success for the
  registered buffers in the active session.
- The worker intent executor remains dependent on the worker client and
  runtime buffer map being live inside the session.
- Worker execution still depends on CUDA backend/device handle support in the
  active runtime environment.
- Adapter context creation still depends on the caller providing valid CPU and
  GPU buffers that can be registered against the active daemon session.
- Older benchmark and example surfaces still use `TurboBusClient` and have not
  been migrated to the runtime-session-first API yet.
- Existing tests, examples, and benchmarks still contain old production-path
  assumptions; current-stage constraints defer migration until system
  implementation is complete.

## Next Main Target

Continue with one concrete implementation boundary: real H2D / D2H execution
path closure.
