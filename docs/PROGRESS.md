# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The active route is no longer paper-validation tooling. The current code work
is the system-level Python runtime path: keep the old `client_transfer.py`
deleted, drive transfers through `TurboBusRuntimeSession`, bootstrap daemon
profile data, run daemon/worker as production socket services, and connect
upper adapters to the runtime session without application-side path selection.

## Completed This Round

- Added `TurboBusRuntimeSession.submit_transfer_intent()` and
  `wait_transfer_receipt()` so adapters can use the system runtime as their
  intent client.
- Added runtime-session construction helpers for `AdapterTransferContext`,
  `OffloadStore`, and `InferenceKVSlotAdapter`.
- Connected the vLLM KV connector to `TurboBusRuntimeSession` instead of a raw
  socket `TurboBusClient`.
- Changed vLLM save/restore CPU backings in the connector path to
  `SharedPinnedCpuBuffer` objects and registered per-layer CUDA IPC buffers
  through the runtime session.
- Kept physical route choice out of offload and vLLM adapter code.

## Validation

- `python -m py_compile turbobus/runtime_session.py turbobus/offload_store.py turbobus/adapters/inference.py turbobus/adapters/vllm.py turbobus/adapters/vllm_backing_pool.py turbobus/adapters/vllm_kv_connector.py`
  passed.
- `python -c "from turbobus import TurboBusRuntimeSession; from turbobus.offload_store import AdapterTransferContext, OffloadStore; from turbobus.adapters.vllm_kv_connector import TurboBusConnector; print('imports ok')"`
  passed.
- `python -m unittest test.python.unit.test_offload_store test.python.unit.test_vllm_kv_connector_main_path`
  passed.
- `git diff --check` passed with Windows line-ending warnings only.

## Remaining Risk

- The runtime session profile bootstrap has not yet been validated on a CUDA
  multi-GPU server.
- vLLM runtime-session save/restore still needs real CUDA, native extension,
  daemon, and worker socket validation.
- Shared CPU buffer reuse and CUDA IPC cleanup need a focused lifecycle pass.

## Next Main Target

Harden worker/runtime resource lifecycle for shared CPU buffers, CUDA IPC
device buffers, receipts, and cleanup.
