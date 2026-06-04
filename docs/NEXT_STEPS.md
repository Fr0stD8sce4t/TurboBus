# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: complete worker failure handling into daemon cleanup and
`TransferReceipt` consumption. Worker/backend failures must remain on the
daemon-issued ticket path and produce an explicit failed receipt after daemon
status and cleanup are confirmed.

## Exit Criteria

- Worker failed completions require daemon FAILED status evidence.
- Worker failed completions require cleanup evidence covering the daemon-issued
  lease set and staging release.
- Runtime intent execution returns the daemon failed `TransferReceipt` for
  confirmed worker/backend failure instead of hiding it behind an exception.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

Finish the failure path from daemon-issued `ExecutionTicket` through worker
failure reporting, daemon cleanup, and runtime receipt consumption.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: runtime receipt validation, runtime session production startup, or
adapter submission/receipt consumption through `TurboBusRuntimeSession`.
