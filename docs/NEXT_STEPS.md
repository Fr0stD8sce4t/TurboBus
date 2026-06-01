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

- Keep receipt evidence mandatory through daemon, worker, backend, public
  client, adapter, benchmark, and paper-validation paths.
- Remove or tighten any remaining workload boundary that can turn intent-only
  status into successful workload completion.
- Do not add mock CUDA, fake correctness gates, or local-only replacement
  validators.

## Next Step

Continue real CUDA public intent H2D/D2H and relay/pool checks on the server
when the required GPUs are available. If code must continue before that, audit
remaining non-public receipt constructors for completed receipts without
evidence.
