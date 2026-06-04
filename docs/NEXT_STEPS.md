# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

The next task is to tighten daemon/worker production startup and socket
ownership so the runtime session, daemon socket client, worker socket service,
and worker lifecycle keep using the same daemon-issued `ExecutionTicket`
contract without application-side physical route control.

## Exit Criteria

- Daemon and worker socket entry points expose the production runtime path
  without restoring old single-process or manual-relay APIs.
- Worker socket requests continue through the standard worker lifecycle and
  cannot execute stale, application-selected, or non-daemon-issued plans.
- Runtime-session `open_socket()` remains the public socket entry for
  applications and adapters.
- No benchmark, paper-validation, experiment, server-validation, compatibility
  shim, or export layer code is added during this pass.

## Current Code Work

- Inspect `turbobus/daemon/__main__.py`, `turbobus/daemon/startup.py`,
  `turbobus/worker/__main__.py`, `turbobus/worker/process.py`, and
  `turbobus/worker/transport.py` for production startup consistency.
- Inspect `TurboBusRuntimeSession.open_socket()` and worker socket clients for
  route-selection or stale-ticket bypasses.
- Keep daemon cleanup and release paths retired from active scheduling state
  after terminal or cleaned transfers.
- Keep the old `client_transfer.py`, `turbobus/worker/helper.py`, and
  `turbobus/daemon/protocol.py` files deleted. Do not recreate compatibility
  export layers.
- Continue code implementation and refactoring without adding server test
  commands or using server validation as the current entry point.

## Next Entry

Continue the code implementation pass by inspecting daemon and worker
production startup/socket paths. Keep the work focused on system code; defer
tests, benchmarks, paper-validation, experiments, and server validation until
the full system implementation pass is complete.
