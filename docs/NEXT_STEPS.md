# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

Real buffer correctness gate.

## Exit Criteria

- Public intent H2D and D2H verify destination bytes on real CUDA buffers.
- Worker relay and pooled paths verify destination bytes on real CUDA buffers.
- Completed receipts expose worker/backend execution source, verified bytes,
  and content-match evidence.
- Benchmark and paper validation reject receipts that only report scheduled or
  completed intent without executed and verified byte evidence.

## Current Code Work

- Complete receipts are now rejected at schema construction unless they carry
  worker/backend source, execution, verified-byte, and content-match evidence.
- Direct backend and worker CUDA execution now require backend
  `verify_transfer`; stats-only evidence is not accepted as buffer correctness.
- The old manual `turbobus.verification` helper-socket route selector has been
  removed from the active code path.
- Continue server-side real CUDA checks for public intent backend H2D/D2H and
  worker relay/pooled paths.
- Do not add mock CUDA, fake correctness gates, or local-only replacement
  validators while waiting for server GPU availability.

## Next Step

Run the real CUDA public intent and relay/pool correctness checks on a P2P
capable GPU pair when the server GPUs are available.
