# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: worker/backend failure cleanup and terminal receipt
closure. Worker-managed transfers now need to keep terminal failure states on
the receipt path instead of falling through to route cleanup and an exception.

## Exit Criteria

- Ownership and cleanup paths retire runtime state only for the owning job,
  session, or buffer.
- Scheduler feedback continues to consume live runtime state rather than
  static plan output.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

`WorkerIntentTransferExecutor.execute_transfer_intent()` should wait
for a receipt when `submit_worker_execution()` returns `status_failed` or
`cleanup_failed`.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: worker/backend execution status into daemon runtime feedback.
