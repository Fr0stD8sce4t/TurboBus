# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: worker/backend execution status into daemon runtime
feedback. Daemon runtime summary data should keep worker/backend completion
source counts current after transfer retirement, and scheduler runtime
metadata should expose those counts directly.

## Exit Criteria

- Ownership and cleanup paths retire runtime state only for the owning job,
  session, or buffer.
- Scheduler feedback continues to consume live runtime state rather than
  static plan output.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

`turbobus/daemon/server.py` should keep completion-source counts aligned with
retired transfer state, and `turbobus/scheduler/load_feedback.py` should
surface those counts in runtime metadata.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: real buffer correctness gate.
