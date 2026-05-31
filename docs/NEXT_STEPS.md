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
- `python -m turbobus.verification` now drives the public `TransferIntent` API
  and can require either backend or worker execution as a verification fixture;
  the intent itself does not choose direct, relay, pool, target GPU, or relay GPU
  as a physical route.

## Remaining Work For This Target

- Build the native CUDA extension on a CUDA server.
- Run public intent H2D and D2H correctness checks against real GPU buffers for
  both backend and worker execution.

## Exit Criteria

- Public intent transfers verify destination bytes for worker and backend
  execution paths on real CUDA buffers.
- A completed receipt records executed and verified byte evidence.
- Tests fail if worker/backend status reports completion without matching
  buffer contents.

## Next Step

Run the native CUDA build and these public intent correctness checks on a CUDA
server. Do not move to benchmark repair until they pass:

```bash
python -m turbobus.verification --direction h2d --execution-path backend
python -m turbobus.verification --direction d2h --execution-path backend
python -m turbobus.verification --direction h2d --execution-path worker --chunk-bytes 262144
python -m turbobus.verification --direction d2h --execution-path worker --chunk-bytes 262144
```
