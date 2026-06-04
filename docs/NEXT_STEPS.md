# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: benchmark data-plane repair. Offload block registration
should require real CPU and GPU backing instead of fabricating placeholder
GPU tensors.

## Exit Criteria

- Ownership and cleanup paths retire runtime state only for the owning job,
  session, or buffer.
- Scheduler feedback continues to consume live runtime state rather than
  static plan output.
- Offload block registration rejects placeholder GPU backing before it can be
  used in a transfer intent.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

`turbobus/offload/store.py` should reject missing GPU backing and force the
benchmark-facing offload adapters to use real paired buffers.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: runtime load feedback.
