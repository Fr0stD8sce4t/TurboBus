# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

The current task is to finish the system-level Python runtime path before any
experiment work: open a daemon-managed session, register job and buffers,
bootstrap daemon profile data, submit `TransferIntent`, execute daemon-issued
tickets through worker/backend code, and return `TransferReceipt`. Do not add
tests, benchmarks, paper validation, or experiment tooling during this stage.

## Exit Criteria

- `TurboBusRuntimeSession` is the public API for session/job/buffer lifecycle
  and H2D/D2H intent submission.
- The old `turbobus/client_transfer.py` module is removed after the split; it
  must not remain as a compatibility export layer.
- Applications and adapters submit `TransferIntent` and consume
  `TransferReceipt`; they do not choose direct, relay, pooled, target GPU, or
  relay GPU paths.
- `TurboBusRuntimeSession` can reuse a fresh daemon profile or collect native
  CUDA profile data and write it to daemon `put_profile` before scheduling.
- Direct, relay, and pooled execution stay daemon scheduling outcomes.
- Completed receipts come from worker/backend completion or explicit failure,
  not synthetic local evidence.

## Current Code Work

- Keep the old `client_transfer.py` file deleted. Do not recreate it as a
  compatibility export layer.
- Keep profile bootstrap on the real native/runtime path: daemon `get_profile`
  cache hit, native CUDA profile on miss or force, daemon `put_profile` write.
- Do not add mock profile data, fake correctness gates, benchmark helpers, or
  paper-validation code while validating this path.
- The next code entry is production startup cleanup for daemon and worker
  sockets after the profile bootstrap path is checked.

## Next Entry

Start from `turbobus/runtime_session.py`, `turbobus/backends/cuda.py`, and
`turbobus/runtime_engine.py`. Verify that profile bootstrap is triggered by
the runtime session without letting callers choose physical transfer routes.
