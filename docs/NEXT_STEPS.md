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
- Public intent worker and direct backend paths pass verification evidence into
  daemon receipts.
- The native extension now exposes CUDA readback comparison through
  `verify_transfer`; Python backend, direct fallback, and worker CUDA executor
  call that verifier after daemon-issued execution completes.
- Local tests cover verifier plumbing, missing evidence, and content-mismatch
  rejection.

## Remaining Work For This Target

- Build the native CUDA extension on a CUDA server.
- Run public intent H2D and D2H correctness checks against real GPU buffers.

## Exit Criteria

- Public intent transfers verify destination bytes for worker and backend
  execution paths on real CUDA buffers.
- A completed receipt records executed and verified byte evidence.
- Tests fail if worker/backend status reports completion without matching
  buffer contents.

## Next Step

Run the native CUDA build and public intent H2D/D2H correctness checks on a
CUDA server. Do not move to benchmark repair until those checks pass.
