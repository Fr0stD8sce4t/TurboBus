# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: isolation and authority hardening around daemon session
close handling. Missing-session close paths in `turbobus/daemon/server.py`
must validate retired session ownership before returning a no-op or unknown
response.

## Exit Criteria

- Ownership and cleanup paths retire runtime state only for the owning job,
  session, or buffer.
- Scheduler feedback continues to consume live runtime state rather than
  static plan output.
- Missing-session close handling validates retired session ownership before it
  returns a no-op or unknown response.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

`turbobus/daemon/server.py` session close handling should validate retired
session ownership before responding to already-closed sessions.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: real H2D / D2H execution path closure.
