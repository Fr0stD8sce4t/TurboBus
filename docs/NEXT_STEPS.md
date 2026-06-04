# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: isolation and authority hardening. The runtime feedback
path now feeds scheduler load accounting from live running activity; the next
step is to tighten ownership and cleanup so runtime state stays aligned with
the job, session, and buffer owners that produced it.

## Exit Criteria

- Ownership and cleanup paths retire runtime state only for the owning job,
  session, or buffer.
- Scheduler feedback continues to consume live runtime state rather than
  static plan output.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

Harden transfer/session/buffer retirement so daemon runtime feedback and
cleanup stay aligned with ownership boundaries.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: shared buffer and lease retirement hardening.
