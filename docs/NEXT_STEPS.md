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
- Local tests cover daemon receipt rejection for missing or mismatched evidence.
  Real buffer correctness is not considered verified until CUDA server checks
  run against real buffers.
- CUDA server validation has confirmed native extension build, native direct
  H2D correctness, and native direct D2H correctness on GPU 0.

## Remaining Work For This Target

- Run public intent H2D and D2H correctness checks against real GPU buffers for
  backend execution.
- Run worker relay and pooled correctness once GPU 5 and GPU 6 are idle; they
  are the available NVLink pair for this machine.

## Exit Criteria

- Public intent transfers verify destination bytes for worker and backend
  execution paths on real CUDA buffers.
- A completed receipt records executed and verified byte evidence.
- Tests fail if worker/backend status reports completion without matching
  buffer contents.

## Next Step

Run public intent backend H2D/D2H correctness checks on GPU 0. Do not move to
benchmark repair until public intent receipts prove executed and verified bytes.
