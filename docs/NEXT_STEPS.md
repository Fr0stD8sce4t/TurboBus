# TurboBus Next Steps

This file is the only active forward plan. Keep it short and remove completed
history instead of appending old plans.

## Current Main Target

Intent-to-worker execution loop.

`TransferIntent` submission must lead to daemon-issued plans, worker or backend
execution, status reporting, `TransferReceipt` creation, and cleanup. A receipt
must not report completion from scheduling alone.

## Current Status

- Intent-backed transfers require worker/backend completion evidence.
- Public `TurboBusClient` can execute H2D and D2H intents through daemon-issued
  worker plans without application-side physical route selection.
- Public intent execution now refuses delayed or expired admissions before data
  movement.
- Public worker failure and success paths release relay reservations and
  staging state in local integration coverage.

## Remaining Work For This Target

- Add timeout or stale-session cleanup coverage for public-intent execution.
- Confirm the final receipt and daemon profile state after that timeout cleanup.

## Exit Criteria

- Receipts for executable transfers are complete only after worker/backend
  completion or explicit failure.
- Leases and staging records are released on success, failure, and timeout.
- Tests fail if an intent-backed transfer completes from scheduling state alone
  or executes an expired/delayed plan.

## Next Step

Close timeout cleanup coverage for public-intent execution. Do not move to the
real buffer correctness gate until this target is closed.
