# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: complete worker/backend runtime feedback into daemon
scheduling. Scheduler load accounting must use real admitted/running transfer
state, not delayed or synthetic activity.

## Exit Criteria

- Daemon runtime state distinguishes queued, delayed, running, and active
  transfers.
- Scheduler policy metadata receives delayed, queued, running, and active
  counts from daemon runtime feedback.
- Delayed admission promotion replans without counting the same transfer's old
  reservations, leases, staging records, or paths as active load.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

Finish the scheduler load-accounting closure for daemon-issued execution:
admitted submitted transfers and running worker/backend transfers count as
active execution; delayed transfers remain visible as waiting work but do not
consume active bytes or relay busy feedback.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: worker failure handling to cleanup/receipt, runtime receipt
validation, or adapter submission/receipt consumption through
`TurboBusRuntimeSession`.
