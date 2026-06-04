# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: complete adapter submission/receipt consumption through
`TurboBusRuntimeSession`. Production adapters must stay on the public runtime
session path while worker/backend execution remains on daemon-issued plans.

## Exit Criteria

- Offload and vLLM adapters submit `TransferIntent` through
  `TurboBusRuntimeSession` and consume `TransferReceipt` from the same session.
- Adapter-side code does not choose physical routes or bypass daemon-issued
  execution tickets.
- Runtime session receipt waiting remains the production path for completion
  consumption.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

Route adapter submission and receipt waiting through
`TurboBusRuntimeSession` without restoring old direct client paths.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: worker/backend completion evidence cleanup or profile bootstrap
closure.
