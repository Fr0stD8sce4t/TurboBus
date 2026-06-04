# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: real buffer correctness gate. Runtime session buffer
registration should only accept live shared pinned CPU buffers and live CUDA
IPC GPU buffers, not synthetic backing objects.

## Exit Criteria

- Ownership and cleanup paths retire runtime state only for the owning job,
  session, or buffer.
- Scheduler feedback continues to consume live runtime state rather than
  static plan output.
- Runtime session buffer registration rejects stale or synthetic buffer
  backing before it can be used in an intent.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

`turbobus/runtime_session.py` should validate real shared-memory and CUDA IPC
buffer backing before buffers enter the runtime session registry.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: benchmark data-plane repair.
