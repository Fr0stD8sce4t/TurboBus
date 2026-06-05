# TurboBus Next Steps

This is the only active per-round forward plan. Keep it short and replace
completed state instead of appending history.

## Current Main Target

Real H2D / D2H execution path closure before experiments.

Current code target: production daemon and worker startup for daemon-issued
H2D / D2H execution. Mixed direct-plus-relay completion evidence and buffer
lifecycle evidence are now preserved through backend/worker completion,
daemon receipts, runtime feedback, and runtime-session cleanup.

## Exit Criteria

- Buffer registration and cleanup keep shared pinned CPU and CUDA IPC GPU
  ownership scoped to the session, job, and transfer.
- Receipt metadata and runtime feedback preserve buffer open, close, cleanup,
  and release evidence from real worker/backend completion or explicit failure.
- Production daemon and worker socket startup use daemon-owned topology and
  ticketed execution paths, without synthetic production topology or
  application-side relay ownership.
- Offload, inference, model-loading, training, and vLLM adapters remain on
  `TurboBusRuntimeSession` and do not receive direct/relay/pool/target/relay
  policy controls.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

Focus on the production transfer boundary:

- `turbobus/intent_executor.py`
- `turbobus/direct_fallback.py`
- `turbobus/daemon/server.py`
- `turbobus/daemon/receipts.py`
- `turbobus/runtime_session.py`
- `turbobus/worker/lifecycle.py`
- `turbobus/worker/resources.py`
- `turbobus/worker/endpoint.py`
- `turbobus/worker/socket_client.py`
- `turbobus/topology/discovery.py`

The main implementation gap is now production startup: daemon and worker socket
processes should start from daemon-owned topology discovery, reject synthetic
production topology, bind worker identity where available, and execute only
daemon-issued tickets.

## Next Entry

Start at the daemon/worker production startup path and keep it tied to
`TurboBusRuntimeSession` execution. Do not add server-validation commands,
benchmark adapters, or application-side route controls.
