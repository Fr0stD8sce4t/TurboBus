# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: worker/backend failure cleanup and terminal receipt
closure. Terminal worker/backend completion evidence now needs to flow into
the daemon runtime record so runtime feedback and scheduler views reflect the
real execution source.

## Exit Criteria

- Ownership and cleanup paths retire runtime state only for the owning job,
  session, or buffer.
- Scheduler feedback continues to consume live runtime state rather than
  static plan output.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

`turbobus/daemon/server.py` should retain terminal `completion_source` and
`completion_evidence` in the runtime queue record and bump the runtime state
version when supplemental evidence arrives.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: session/job/buffer registration into the TransferIntent execution
path.
