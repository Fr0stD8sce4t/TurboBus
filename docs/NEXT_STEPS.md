# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: H2D/D2H system main path closure. `fetch_h2d()` and
`offload_d2h()` should drive daemon-issued direct, relay, and pooled execution
through `TurboBusRuntimeSession` without application route selection.

## Exit Criteria

- H2D and D2H public runtime methods submit `TransferIntent` and return
  `TransferReceipt` from worker/backend completion or explicit failure.
- Direct fallback, relay, and pooled paths execute only daemon-issued
  `ExecutionTicket` payloads.
- Public client and runtime-session consumers still submit `TransferIntent`
  and consume `TransferReceipt` without route selection.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

Close H2D/D2H runtime execution gaps in the system production path without
adding benchmark, paper-validation, or server-validation entry points.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: runtime feedback into scheduler load accounting.
