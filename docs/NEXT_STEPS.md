# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: complete runtime receipt validation for daemon-issued
execution. `TurboBusRuntimeSession` must only return receipts whose identity,
ticket binding, execution source, and terminal evidence match the submitted
intent.

## Exit Criteria

- Runtime receipt validation checks intent, job, session, ticket, transfer, and
  plan-generation binding.
- Complete receipts still require worker/backend execution and verified byte
  evidence.
- Failed or canceled receipts require worker/backend execution source, error,
  ticket evidence, transfer evidence, and plan-generation evidence.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

Finish runtime receipt validation so complete and failed daemon receipts are
consumed through one checked `TurboBusRuntimeSession` path.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: runtime session production startup or adapter submission/receipt
consumption through `TurboBusRuntimeSession`.
