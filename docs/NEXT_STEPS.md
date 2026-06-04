# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

The next task is to tighten native/data-plane resource ownership across direct
fallback and worker CUDA execution so shared pinned CPU buffers and CUDA IPC
GPU buffers remain bound to daemon-issued `ExecutionTicket` data and cleaned up
through the unified runtime session path.

## Exit Criteria

- Direct fallback and worker CUDA execution continue to execute only
  daemon-issued plans and tickets.
- Shared pinned CPU buffer registration and CUDA IPC device handle ownership
  remain tied to runtime-session registered buffers.
- Resource evidence recorded in completion metadata reflects the buffers and
  ticket used by the daemon-issued execution.
- No benchmark, paper-validation, experiment, server-validation, compatibility
  shim, or export layer code is added during this pass.

## Current Code Work

- Inspect `turbobus/direct_fallback.py`, `turbobus/worker/resources.py`,
  `turbobus/worker/cuda_executor.py`, and `turbobus/buffer_registration.py`
  for resource ownership and cleanup consistency.
- Keep profile bootstrap owned by `TurboBusRuntimeSession` and daemon profile
  APIs; profile target/relay data must match daemon-discovered session relays.
- Keep the old `client_transfer.py`, `turbobus/worker/helper.py`,
  `turbobus/daemon/protocol.py`, and `turbobus/worker_managed.py` files
  deleted. Do not recreate compatibility export layers.
- Continue code implementation and refactoring without adding server test
  commands or using server validation as the current entry point.

## Next Entry

Continue the code implementation pass by inspecting direct fallback and worker
CUDA resource ownership. Keep the work focused on system code; defer tests,
benchmarks, paper-validation, experiments, and server validation until the full
system implementation pass is complete.
