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
- Public `TurboBusClient` can be configured with a worker intent executor that
  submits `TransferIntent`, executes the daemon-issued plan, and returns the
  final receipt.
- H2D and D2H public-intent integration tests prove worker completion,
  executed receipt metadata, and daemon status updates without application-side
  physical route selection.

## Remaining Work For This Target

- Verify delayed or expired admissions cannot execute until rescheduled.
- Confirm lease and staging cleanup for public-intent worker execution in
  success and failure cases.

## Exit Criteria

- Receipts for executable transfers are complete only after worker/backend
  completion or explicit failure.
- Leases and staging records are released on success, failure, and timeout.
- Tests fail if an intent-backed transfer completes from scheduling state alone
  or executes an expired/delayed plan.

## Next Step

Close delayed/expired admission behavior for public-intent execution. Do not
move to the real buffer correctness gate until this target is closed.
