# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

The current task is to make the Python runtime path usable as one system
entry point: open a daemon-managed session, register job and buffers, submit
`TransferIntent`, execute daemon-issued tickets through worker/backend code,
and return `TransferReceipt`. Do not add tests, benchmarks, paper validation,
or experiment tooling during this stage.

## Exit Criteria

- `TurboBusRuntimeSession` is the public API for session/job/buffer lifecycle
  and H2D/D2H intent submission.
- The old `turbobus/client_transfer.py` module is removed after the split; it
  must not remain as a compatibility export layer.
- Applications and adapters submit `TransferIntent` and consume
  `TransferReceipt`; they do not choose direct, relay, pooled, target GPU, or
  relay GPU paths.
- Direct, relay, and pooled execution stay daemon scheduling outcomes.
- Completed receipts come from worker/backend completion or explicit failure,
  not synthetic local evidence.

## Current Code Work

- Finish the first structural split of the old `client_transfer.py` code into:
  `runtime_session.py`, `intent_executor.py`, `direct_fallback.py`,
  `buffer_registration.py`, `worker_managed.py`, and shared execution helpers.
- Update imports to the new owning modules. Do not keep old files that only
  re-export moved symbols.
- Export `TurboBusRuntimeSession` from `turbobus/__init__.py`.
- After this split compiles, the next main target is profile bootstrap through
  CUDA backend/runtime helpers into daemon `put_profile`.

## Next Entry

Start from `turbobus/runtime_session.py`, `turbobus/intent_executor.py`,
`turbobus/direct_fallback.py`, and `turbobus/worker_managed.py`. Verify that
the runtime session path builds `TransferIntent` and delegates execution to
`WorkerIntentTransferExecutor` without letting callers choose physical routes.
