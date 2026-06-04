# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: isolation and authority hardening. The runtime feedback
path now feeds scheduler load accounting from live running activity; the next
step is to keep retired cleanup targets owner-verifiable so repeated cleanup
requests can still prove which job, session, buffer, or lease owned the state.

## Exit Criteria

- Ownership and cleanup paths retire runtime state only for the owning job,
  session, or buffer.
- Scheduler feedback continues to consume live runtime state rather than
  static plan output.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

Archive retired job, session, buffer, and reservation targets before runtime
retirement, then let repeated cleanup calls resolve to the archived owner
instead of failing as unknown targets.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: worker failure cleanup and receipt closure.
