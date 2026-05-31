# TurboBus Next Steps

This file is the only active forward plan. Keep it short and remove completed
history instead of appending old plans.

## Current Main Target

Real buffer correctness gate.

Completed receipts for intent-backed transfers must prove executed and verified
bytes. Status-only worker/backend completion is no longer enough.

## Current Status

- The daemon rejects intent completion without worker/backend source and
  verified byte evidence.
- `TransferReceipt.metadata` now records `verified`, `verified_bytes`, and
  `content_match`.
- Public intent worker and direct backend paths pass local verified-byte
  evidence into daemon receipts.
- Local tests cover missing evidence and content-mismatch rejection.

## Remaining Work For This Target

- Add native CUDA/readback evidence so real GPU transfers produce verified
  source/destination byte proof instead of relying on test fixture evidence.
- Run the public intent H2D and D2H correctness checks on a CUDA server.

## Exit Criteria

- Public intent transfers verify destination bytes for worker and backend
  execution paths on real CUDA buffers.
- A completed receipt records executed and verified byte evidence.
- Tests fail if worker/backend status reports completion without matching
  buffer contents.

## Next Step

Implement the native CUDA/readback verifier for the public intent path. Do not
move to benchmark repair until real CUDA transfers produce verified receipts.
