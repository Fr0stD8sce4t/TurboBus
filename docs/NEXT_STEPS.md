# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: real H2D / D2H execution path closure. Offload adapter
submission should go through `TurboBusRuntimeSession.fetch_h2d()` and
`offload_d2h()` so the public runtime session API owns the transfer path.

## Exit Criteria

- Ownership and cleanup paths retire runtime state only for the owning job,
  session, or buffer.
- Scheduler feedback continues to consume live runtime state rather than
  static plan output.
- Offload adapter submissions use the public runtime session H2D / D2H methods
  with preserved intent identity and adapter metadata.
- Adapter transfer context and offload / inference / vLLM adapter creation are
  owned solely by `TurboBusRuntimeSession`.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

`turbobus/runtime_session.py` should own the sole adapter construction path
that feeds `fetch_h2d()` / `offload_d2h()` and the offload / inference / vLLM
adapters.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: real H2D / D2H execution path closure.
