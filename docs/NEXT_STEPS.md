# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: daemon/worker production startup. Runtime sessions should
be able to drive daemon and worker socket clients on the production path
without restoring old runtime or planner entry points.

## Exit Criteria

- Production socket setup wires runtime, execution, profile, and worker clients
  into one `TurboBusRuntimeSession`.
- Worker execution still consumes only daemon-issued `ExecutionTicket` payloads.
- Public client and runtime-session consumers still submit `TransferIntent`
  and consume `TransferReceipt` without route selection.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

Close daemon/worker startup gaps in the runtime production path without adding
server validation wrappers or compatibility APIs.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: H2D/D2H system main path closure.
