# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

The next task is to tighten runtime load feedback into daemon scheduling so
the scheduler can reason about active transfer state without applications or
adapters choosing physical routes.

## Exit Criteria

- Daemon scheduling consumes runtime resource state for active transfers and
  relay load.
- Runtime state is derived from daemon-owned transfer, lease, ticket, and
  cleanup records rather than adapter or benchmark hints.
- Applications and adapters still submit only `TransferIntent` and consume
  `TransferReceipt`.
- No benchmark, paper-validation, experiment, server-validation, compatibility
  shim, or export layer code is added during this pass.

## Current Code Work

- Inspect `turbobus/daemon/server.py`, `turbobus/scheduler/daemon.py`, and
  `turbobus/scheduler/load_feedback.py` for runtime-state scheduling inputs.
- Keep offload and vLLM adapter handoff owned by `TurboBusRuntimeSession`; vLLM
  connector prefix state uses the daemon runtime session id while preserving
  the connector engine id only as metadata.
- Keep the old `client_transfer.py`, `turbobus/worker/helper.py`,
  `turbobus/daemon/protocol.py`, and `turbobus/worker_managed.py` files
  deleted. Do not recreate compatibility export layers.
- Continue code implementation and refactoring without adding server test
  commands or using server validation as the current entry point.

## Next Entry

Continue the code implementation pass by inspecting runtime load feedback into
daemon scheduling. Keep the work focused on system code; defer tests,
benchmarks, paper-validation, experiments, and server validation until the full
system implementation pass is complete.
