# TurboBus Next Steps

This is the only active per-round forward plan. Keep it short and replace
completed state instead of appending history.

## Current Main Target

Real H2D / D2H execution path closure before experiments.

Current code target: scheduler load feedback and cross-job isolation for
daemon-issued H2D / D2H execution. Mixed direct-plus-relay completion evidence,
buffer lifecycle evidence, and production worker startup evidence are now
preserved through backend/worker completion, daemon receipts, runtime feedback,
and runtime-session cleanup.

## Exit Criteria

- Buffer registration and cleanup keep shared pinned CPU and CUDA IPC GPU
  ownership scoped to the session, job, and transfer.
- Receipt metadata and runtime feedback preserve buffer open, close, cleanup,
  and release evidence from real worker/backend completion or explicit failure.
- Scheduler decisions consume live queued/running/active transfer state,
  relay lease state, staging usage, completion sources, and job weights.
- Offload, inference, model-loading, training, and vLLM adapters remain on
  `TurboBusRuntimeSession` and do not receive direct/relay/pool/target/relay
  policy controls.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

Focus on the production transfer boundary:

- `turbobus/daemon/server.py`
- `turbobus/scheduler/`
- `turbobus/scheduler/load_feedback.py`
- `turbobus/daemon/leases.py`
- `turbobus/intent_executor.py`
- `turbobus/runtime_session.py`

The main implementation gap is now scheduler load accounting: daemon planning
should use real queued/running/active transfer state, relay leases, staging
usage, completion source history, and job weights when choosing direct, relay,
and mixed pooled paths.

## Next Entry

Start at scheduler load feedback and relay isolation state in the daemon. Do
not add server-validation commands, benchmark adapters, or application-side
route controls.
