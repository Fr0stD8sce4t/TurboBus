# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: runtime load feedback. Scheduler load metadata should
carry live daemon runtime summary state, including active resource usage and
completion-source counts, instead of only the minimal count set.

## Exit Criteria

- Ownership and cleanup paths retire runtime state only for the owning job,
  session, or buffer.
- Scheduler feedback continues to consume live runtime state rather than
  static plan output.
- Scheduler decision metadata exposes live runtime resource usage and
  completion-source counts.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

`turbobus/scheduler/load_feedback.py` should surface daemon runtime summary
state in the scheduler policy metadata.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: isolation and authority hardening.
