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
- Daemon and worker CLIs start production socket services instead of preview,
  helper, or smoke-test-only modes.

## Current Code Work

- Keep the old `client_transfer.py` file deleted. Do not recreate it as a
  compatibility export layer.
- Keep profile bootstrap on the real native/runtime path: daemon `get_profile`
  cache hit, native CUDA profile on miss or force, daemon `put_profile` write.
- Keep daemon and worker startup paths as socket services. Do not add dry-run
  startup wrappers or request-count test modes as production CLI behavior.
- Keep offload and vLLM adapters on `TurboBusRuntimeSession`: adapters submit
  `TransferIntent`, consume `TransferReceipt`, and receive buffer/session
  registration from the runtime session.
- Keep worker/runtime resource lifecycle explicit: changed CPU shared-memory or
  CUDA IPC handles must be re-registered, and worker-bound resources must close
  predictably after H2D/D2H execution.
- Do not add mock profile data, fake correctness gates, benchmark helpers, or
  paper-validation code while validating this path.
- The next code entry is daemon control-plane state consistency for admission,
  receipts, and cleanup.

## Next Entry

Start from `turbobus/daemon/server.py`, `turbobus/daemon/dispatch.py`, and
`turbobus/scheduler/daemon.py`. Check profile misses, delayed admission,
receipt completion, and cleanup state without adding tests or experiment code.
