# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: isolation and authority hardening. The runtime feedback
path now feeds scheduler load accounting from live running activity; the next
step is to harden runtime-session startup so buffer registration rolls back
any partial daemon registrations if the batch fails.

## Exit Criteria

- Ownership and cleanup paths retire runtime state only for the owning job,
  session, or buffer.
- Scheduler feedback continues to consume live runtime state rather than
  static plan output.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

`TurboBusRuntimeSession._register_pending_buffers()` should clean up any
buffers it already registered if a later buffer registration fails.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: worker/backend failure cleanup and terminal receipt closure.
