# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

The current task is to harden scheduler runtime policy now that the Python
runtime path, profile bootstrap, worker execution path, and daemon control
state are connected. The scheduler must make route, fallback, admission, and
reschedule decisions from daemon-owned state only; applications and adapters
must continue to submit `TransferIntent` and consume `TransferReceipt`.

## Exit Criteria

- `DaemonScheduler` treats relay availability, quota, busy relay state,
  fairness fallback, and direct fallback as one daemon-owned policy path.
- Reschedule uses the same policy inputs as initial scheduling and does not
  preserve stale leases, tickets, or admission state.
- Profile misses remain explicit direct fallback decisions and do not look like
  relay-capable plans.
- Worker/backend execution remains limited to daemon-issued
  `ExecutionTicket`.
- No application, adapter, benchmark, or validation code chooses direct, relay,
  pooled, target GPU, or relay GPU paths.

## Current Code Work

- Start from `turbobus/scheduler/daemon.py` and the reschedule/admission call
  sites in `turbobus/daemon/server.py`.
- Keep the old `client_transfer.py` file deleted. Do not recreate it as a
  compatibility export layer.
- Do not add mock profile data, fake correctness gates, benchmark helpers, or
  paper-validation code while validating this path.

## Next Entry

Inspect `turbobus/scheduler/daemon.py`, then follow its call path from
`TurboBusDaemon.plan_transfer()` and `TurboBusDaemon.reschedule_transfer()`.
