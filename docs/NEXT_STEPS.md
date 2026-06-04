# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: isolation and authority hardening. The runtime feedback
path now feeds scheduler load accounting from live running activity; the next
step is to keep worker failure cleanup able to close receipts even when the
first status report fails, so cleanup and receipt closure stay coupled.

## Exit Criteria

- Ownership and cleanup paths retire runtime state only for the owning job,
  session, or buffer.
- Scheduler feedback continues to consume live runtime state rather than
  static plan output.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

When worker status reporting fails after execution, retry the terminal
`transfer_status()` update after cleanup so the daemon still receives the
failure evidence needed to close the receipt path.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: delete the old `turbobus/client_transfer.py` export layer.
