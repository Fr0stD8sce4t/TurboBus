# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: session/job/buffer registration into the TransferIntent
execution path. Runtime-session buffer registration now needs to roll back
its local state if daemon registration fails after a buffer has already been
added.

## Exit Criteria

- Ownership and cleanup paths retire runtime state only for the owning job,
  session, or buffer.
- Scheduler feedback continues to consume live runtime state rather than
  static plan output.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

`turbobus/runtime_session.py` should remove the newly added buffer from its
local registry when submit-time daemon registration fails.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: `WorkerIntentTransferExecutor` and the daemon-issued worker path.
