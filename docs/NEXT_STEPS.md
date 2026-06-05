# TurboBus Next Steps

This is the only active per-round forward plan. Keep it short and replace
completed state instead of appending history.

## Current Main Target

Real H2D / D2H execution path closure before experiments.

Current code target: daemon-issued mixed pooled transfer execution. A single
`TransferIntent` whose scheduler decision contains direct and relay
assignments must execute all assigned chunks, report worker/backend evidence,
clean up daemon and worker state, and return one valid `TransferReceipt`.

## Exit Criteria

- `WorkerIntentTransferExecutor` executes daemon-planned direct-only,
  relay-only, and mixed pooled plans without choosing physical routes.
- Direct assignments in a pooled plan are executed by backend exact-plan code
  under the daemon-issued ticket, not by application-side path choice.
- Relay assignments in the same pooled plan are executed through worker
  authorization, worker CUDA execution, status reporting, and cleanup.
- Daemon terminal status and receipt metadata include real completion or
  explicit failure evidence for every planned byte.
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
- `turbobus/worker/lifecycle.py`
- `turbobus/worker/cuda_executor.py`
- `turbobus/runtime_session.py`
- `cpp/src/executor_cuda.cu`

The main implementation gap is that direct-only and relay-worker execution
exist as separate paths, while mixed pooled direct-plus-relay execution still
needs one daemon-issued transfer lifecycle and one receipt.

## Next Entry

Start at `WorkerIntentTransferExecutor.execute_transfer_intent()`: keep daemon
plans authoritative, split execution by daemon assignment type, execute direct
and relay chunks for the same transfer, and report combined completion or
explicit failure back to the daemon.
