# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: runtime load feedback. Scheduler fairness fallback
should use live daemon runtime pressure, not just a transfer-count snapshot.

## Exit Criteria

- Ownership and cleanup paths retire runtime state only for the owning job,
  session, or buffer.
- Scheduler feedback continues to consume live runtime state rather than
  static plan output.
- Scheduler fairness reacts to live runtime pressure from active transfers,
  relay staging, and relay busy state.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

`turbobus/scheduler/load_feedback.py` should feed live runtime pressure into
fairness fallback and policy metadata.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: isolation and authority hardening.
