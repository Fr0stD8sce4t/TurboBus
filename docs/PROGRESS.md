# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The production path is being kept on the daemon-first route:
`TransferIntent` submission, daemon scheduling, daemon-issued
`ExecutionTicket`, worker/backend completion, and `TransferReceipt`
consumption. `TurboBusRuntimeSession` remains the public runtime entry for
session, job, buffer, profile bootstrap, intent submission, worker execution,
receipt wait, and cleanup wiring. The old `client_transfer.py`,
`turbobus.control`, route-shaped transfer request, manual relay reservation,
manual session relay selection, worker shortcut, transfer-mode, broad daemon
client, buffer self-registration, and pure re-export compatibility entry
points remain removed.

Server validation, benchmark work, paper validation, experiments, and new test
code remain deferred until the full system implementation pass is complete.
Current progress should continue through code reading, implementation,
refactoring, and existing minimal local checks without adding server test
commands or making server validation a current entry point.

## Completed This Round

- Audited the public `turbobus.worker` package boundary during the
  daemon-first closure pass.
- Removed package-level exports for worker data-plane request/result models,
  staging pools, resource binders, CUDA executors, and codec helpers.
- Updated `TurboBusRuntimeSession` to import worker executor, resource binder,
  lifecycle client, and socket client from the modules that own those
  implementations.
- Kept the public worker package focused on worker service and full lifecycle
  entry points instead of partial data-plane construction tools.

## Validation

- `python -m py_compile turbobus\worker\__init__.py
  turbobus\runtime_session.py turbobus\worker\process.py
  turbobus\worker\endpoint.py turbobus\worker\socket_client.py
  turbobus\worker\transport.py` passed.
- `rg -n "from \.worker import|from turbobus\.worker import" turbobus`
  found no production imports from the package-level worker export surface.
- `rg -n "WorkerTransferRequest|WorkerTransferResult|WorkerStagingPool|WorkerDataPlaneResourceBinder|WorkerDataPlaneResourceBinding|WorkerDataPlaneResources|CudaWorkerExecutor|decode_worker_request_envelope|encode_worker_request_envelope"
  turbobus\worker\__init__.py turbobus\runtime_session.py` confirmed the
  removed package exports are absent and runtime session imports the needed
  implementation objects from owning modules.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests still contain old production-path assumptions such as removed
  transfer request, manual reservation, worker shortcut, broad daemon client,
  manual daemon planning, adapter policy hints, old `runtime_engine` imports,
  public worker internals, package-level worker data-plane exports, and
  compatibility entry points. Current-stage constraints defer test migration
  until the system implementation pass is complete.
- A final system-code closure audit still needs to continue looking for
  compatibility drift, application-side physical route controls, or public
  bypasses around daemon-issued execution tickets.

## Next Main Target

Continue the system-code closure audit for the daemon-first path while keeping
tests, benchmarks, paper validation, server validation, and experiments
deferred.
