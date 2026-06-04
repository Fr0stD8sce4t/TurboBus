# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: session/job/buffer registration into the TransferIntent
execution path. Adapter submissions now need to re-confirm the runtime
session and pending buffer registrations at submit time, not only when the
context is created.

## Exit Criteria

- Ownership and cleanup paths retire runtime state only for the owning job,
  session, or buffer.
- Scheduler feedback continues to consume live runtime state rather than
  static plan output.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

`turbobus/offload/store.py` should refresh the runtime session and pending
buffer registrations before it submits a transfer intent.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: `WorkerIntentTransferExecutor` and the daemon-issued worker path.
