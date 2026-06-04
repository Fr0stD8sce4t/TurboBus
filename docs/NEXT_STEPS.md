# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: worker/backend failure cleanup and terminal receipt
closure. Worker authorization failures now need to be treated as explicit
cleanup-only failures instead of leaking through the worker executor as an
unclassified state.

## Exit Criteria

- Ownership and cleanup paths retire runtime state only for the owning job,
  session, or buffer.
- Scheduler feedback continues to consume live runtime state rather than
  static plan output.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

`turbobus/intent_execution_support.py` and `turbobus/intent_executor.py`
should handle `authorization_failed` as a terminal worker failure path.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: worker/backend execution status into daemon runtime feedback.
