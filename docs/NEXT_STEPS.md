# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: complete worker/backend completion evidence cleanup.
Production transfer completion must keep using daemon-issued plans and
receipt-oriented public APIs.

## Exit Criteria

- Worker completion, cleanup, and receipt evidence remain bound to daemon
  tickets and daemon-issued transfer ids.
- Public client and runtime-session consumers still submit `TransferIntent`
  and consume `TransferReceipt` without route selection.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

Tighten worker/backend completion evidence and cleanup flow without restoring
old direct client paths.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: profile bootstrap closure or shared buffer lifecycle cleanup.
