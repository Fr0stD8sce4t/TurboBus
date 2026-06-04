# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: complete profile bootstrap closure. Runtime bootstrap
must feed daemon scheduling and worker/backend execution without application
route selection.

## Exit Criteria

- Runtime profile bootstrap installs profile data into daemon scheduling and
  daemon-issued worker/backend execution payloads.
- Public client and runtime-session consumers still submit `TransferIntent`
  and consume `TransferReceipt` without route selection.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

Close remaining profile bootstrap gaps without restoring old runtime or planner
entry points.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: shared buffer lifecycle cleanup or daemon/worker production startup.
