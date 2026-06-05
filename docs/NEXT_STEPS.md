# TurboBus Next Steps

This is the only active per-round forward plan. Keep it short and replace
completed state instead of appending history.

## Current Main Target

Real H2D / D2H execution path closure before experiments.

Current code target: daemon receipt and runtime feedback closure for
daemon-issued mixed pooled transfer execution. The in-process runtime path and
production worker socket path now both let relay workers return completion and
cleanup evidence without independently completing the whole mixed transfer.

## Exit Criteria

- Daemon terminal status and receipt metadata include merged real completion
  or explicit failure evidence for every planned byte.
- Runtime feedback observes queued/running/active direct and relay paths from
  daemon state, not static plan output alone.
- Buffer registration and cleanup keep shared pinned CPU and CUDA IPC GPU
  ownership scoped to the session, job, and transfer.
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
- `turbobus/intent_execution_support.py`

The main implementation gap is now terminal evidence preservation: daemon
completion, receipts, and runtime feedback should keep direct and relay child
completion evidence for mixed pooled execution instead of flattening it to a
single worker/backend source.

## Next Entry

Start at daemon terminal completion normalization and receipt metadata. Preserve
the worker socket deferred-terminal path, and do not add server-validation
commands or application-side route controls.
