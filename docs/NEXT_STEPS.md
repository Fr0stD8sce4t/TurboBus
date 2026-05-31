# TurboBus Next Steps

This file is the only active forward plan. Keep it short and remove completed
history instead of appending old plans.

## Current Main Target

Real buffer correctness gate.

`TransferReceipt` evidence must prove that daemon-issued worker or backend
execution moved the requested bytes correctly. The next code work should verify
buffer contents across the public intent path instead of accepting status-only
completion.

## Current Status

- The intent-to-worker execution loop is closed for local integration coverage.
- Executable intent receipts complete only after worker or backend completion
  evidence, or terminal failure/cancelation.
- Public H2D and D2H intent execution goes through daemon-issued worker plans.
- Delayed admission, expired plans, worker failure, success, and stale-session
  timeout cleanup now have focused coverage.

## Exit Criteria

- Public intent transfers verify destination bytes for worker and backend
  execution paths.
- A completed receipt records executed and verified byte evidence.
- Tests fail if worker/backend status reports completion without matching
  buffer contents.

## Next Step

Add the real buffer correctness gate for the public intent path. Do not move to
benchmark repair until completed receipts require verified bytes.
