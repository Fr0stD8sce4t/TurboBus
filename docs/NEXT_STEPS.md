# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

The next task is to tighten profile bootstrap and runtime/native profile
ownership so `TurboBusRuntimeSession` can populate daemon profile state without
applications selecting physical routes or assembling profile control-plane
calls manually.

## Exit Criteria

- Runtime profile bootstrap remains owned by `TurboBusRuntimeSession` and the
  daemon profile API.
- Profile conversion keeps target/relay data tied to daemon-discovered session
  relays, not application-provided route choices.
- Native profile helpers expose only the capabilities needed by the unified
  runtime session path.
- No benchmark, paper-validation, experiment, server-validation, compatibility
  shim, or export layer code is added during this pass.

## Current Code Work

- Inspect `turbobus/runtime_session.py`, `turbobus/profile.py`,
  `turbobus/runtime_engine.py`, and `turbobus/backends/cuda.py` for profile
  bootstrap ownership and native profile conversion.
- Keep `TurboBusRuntimeSession.open_socket()` as the public socket entry; the
  old worker-managed manual target/relay client path has been removed.
- Keep the old `client_transfer.py`, `turbobus/worker/helper.py`,
  `turbobus/daemon/protocol.py`, and `turbobus/worker_managed.py` files
  deleted. Do not recreate compatibility export layers.
- Continue code implementation and refactoring without adding server test
  commands or using server validation as the current entry point.

## Next Entry

Continue the code implementation pass by inspecting profile bootstrap and
runtime/native profile ownership. Keep the work focused on system code; defer
tests, benchmarks, paper-validation, experiments, and server validation until
the full system implementation pass is complete.
