# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: isolation and authority hardening. Reservation cleanup
should not let a non-owner advance global cleanup state when the target has
already retired.

## Exit Criteria

- Ownership and cleanup paths retire runtime state only for the owning job,
  session, or buffer.
- Scheduler feedback continues to consume live runtime state rather than
  static plan output.
- Cleanup of retired reservations still requires owner validation before any
  no-op or global cleanup side effects are allowed.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

`turbobus/daemon/server.py` should require owner validation before retired
reservation cleanup can return a no-op response.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: real H2D / D2H execution path closure.
