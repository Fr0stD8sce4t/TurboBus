# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: shared buffer lifecycle cleanup. Runtime-owned CPU and GPU
buffer registration must carry enough daemon/worker lifecycle state for
daemon-issued H2D, D2H, direct, relay, and pooled execution without
application route selection.

## Exit Criteria

- Runtime session buffer registration records daemon-visible ownership and
  worker-usable buffer state before transfer intent submission.
- Shared pinned CPU buffers and CUDA IPC GPU buffers have one clear cleanup path
  after worker/backend completion or failure.
- Public client and runtime-session consumers still submit `TransferIntent`
  and consume `TransferReceipt` without route selection.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

Close buffer lifecycle gaps in the runtime/daemon/worker production path
without restoring old runtime or planner entry points.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: daemon/worker production startup.
