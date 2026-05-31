# TurboBus Next Steps

This file is the only active forward plan. Keep it short: remove completed
history instead of moving it to an appendix.

## Current Main Target

Intent-to-worker execution loop.

`TransferIntent` submission must lead to daemon-issued plans, worker or backend
execution, status reporting, `TransferReceipt` creation, and cleanup. A receipt
must not report completion from scheduling alone.

## Done In This Target

- Intent-backed transfers now require worker/backend execution evidence before
  they can be marked complete.
- Worker status reports mark completion as `worker` execution.
- Direct daemon-ticketed backend fallback marks completion as `backend`
  execution.
- Transfer receipts expose `metadata.completion_source` and
  `metadata.executed` so later benchmark and paper-validation work can reject
  intent-only evidence.
- Integration tests cover the guard: an intent-only completion update is
  rejected, while worker-backed completion produces a completed receipt.

## Remaining Work For This Target

- Replace client-side intent submission that only returns the initial daemon
  receipt with a public execution path that waits for worker/backend completion
  when executable buffers and worker/backend clients are available.
- Add one H2D and one D2H public-intent integration test that goes through
  `TurboBusClient` or its worker-managed equivalent and proves completion,
  receipt metadata, and cleanup.
- Keep delayed or expired admissions from executing until rescheduled.

## Exit Criteria

- Receipts for executable transfers are complete only after worker/backend
  completion or explicit failure.
- Leases and staging records are released on success, failure, and timeout.
- Tests fail if an intent-backed transfer completes from scheduling state alone.

## Next Step

Finish the public intent execution path for H2D and D2H without adding
application-side physical route controls.
