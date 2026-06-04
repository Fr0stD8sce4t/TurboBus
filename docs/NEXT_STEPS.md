# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: real H2D / D2H execution path closure. Offload and vLLM
adapter submission should go through `TurboBusRuntimeSession.fetch_h2d()`,
`offload_d2h()`, and the session-owned vLLM factory so the public runtime
session API owns the transfer path.

## Exit Criteria

- Ownership and cleanup paths retire runtime state only for the owning job,
  session, or buffer.
- Scheduler feedback continues to consume live runtime state rather than
  static plan output.
- Offload adapter submissions use the public runtime session H2D / D2H methods
  with preserved intent identity and adapter metadata.
- Adapter transfer context and offload / inference / vLLM adapter creation are
  owned solely by `TurboBusRuntimeSession`.
- The offload, inference, training, and vLLM adapter wrappers no longer expose
  their own `from_runtime_session()` constructors; session-owned factories are
  the only construction path.
- Session open now triggers daemon profile bootstrap and `put_profile` before
  the first transfer path is used.
- Session close now cleans daemon-registered buffers before closing the
  session, so buffer ownership retires through the runtime session API.
- The runtime session now exposes a worker intent executor factory for the
  daemon-issued transfer path.
- The CUDA worker executor now binds its own data-plane resources in the
  default execution path instead of failing immediately.
- Adapter transfer context creation now rolls back newly registered buffers if
  session bootstrap or context construction fails.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

`turbobus/runtime_session.py` should own the sole adapter construction path
that feeds `fetch_h2d()` / `offload_d2h()`, the worker intent executor, and
the offload / inference / vLLM adapters, including the vLLM KV connector path.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: real H2D / D2H execution path closure.
