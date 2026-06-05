# TurboBus Next Steps

This is the only active per-round forward plan. Keep it short and replace
completed state instead of appending history.

## Current Main Target

Real H2D / D2H execution path closure before experiments.

Current code target: buffer lifetime evidence for daemon-issued H2D / D2H
execution. Mixed direct-plus-relay completion evidence is now preserved through
daemon terminal status, receipts, and runtime feedback summaries.

## Exit Criteria

- Buffer registration and cleanup keep shared pinned CPU and CUDA IPC GPU
  ownership scoped to the session, job, and transfer.
- Receipt metadata and runtime feedback preserve buffer open, close, cleanup,
  and release evidence from real worker/backend completion or explicit failure.
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

The main implementation gap is now buffer lifecycle evidence: shared pinned CPU
buffers and CUDA IPC GPU buffers should carry open, close, cleanup, and
session-owned release evidence through worker/backend completion, daemon
receipts, and runtime-session cleanup.

## Next Entry

Start at worker/backend resource lifecycle metadata and runtime-session cleanup.
Preserve mixed pooled execution evidence, and do not add server-validation
commands or application-side route controls.
