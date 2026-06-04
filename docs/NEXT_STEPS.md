# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: real H2D / D2H execution path closure. Offload block
construction should require explicit CPU and GPU backing so benchmark-facing
adapters cannot hide a missing endpoint behind `None`.

## Exit Criteria

- Ownership and cleanup paths retire runtime state only for the owning job,
  session, or buffer.
- Scheduler feedback continues to consume live runtime state rather than
  static plan output.
- Benchmark-facing offload adapters require explicit CPU and GPU backing at
  block construction time.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

`turbobus/offload/store.py`, `turbobus/offload/blocks.py`, and the benchmark
adapters should require real paired backing objects before transfer blocks can
be created.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: real H2D / D2H execution path closure.
