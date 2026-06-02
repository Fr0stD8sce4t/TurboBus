# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

The current task is to continue system implementation by tightening runtime
load and topology feedback into daemon-first scheduling. Scheduler decisions
should use daemon-owned topology/profile/load state, while applications and
adapters continue to submit only `TransferIntent` and consume
`TransferReceipt`.

## Exit Criteria

- Daemon scheduling has a clear load/topology input path that does not depend
  on application-side physical route choices.
- Runtime load updates can influence direct, relay, pooled, or delayed
  decisions without worker/backend code choosing paths.
- Existing runtime/session/adapters continue to submit `TransferIntent` and
  consume `TransferReceipt`.
- No benchmark, paper-validation, experiment, or compatibility shim code is
  added during this pass.

## Current Code Work

- Start from `turbobus/scheduler/daemon.py`, `turbobus/daemon/server.py`, and
  daemon topology/profile/load helpers.
- Keep the old `client_transfer.py` file deleted. Do not recreate it as a
  compatibility export layer.
- Do not add mock native backends, fake correctness gates, benchmark helpers,
  or paper-validation code while validating this path.

## Next Entry

Trace how daemon-owned topology, cached profile, reservations, and runtime
load feed scheduler decisions.
