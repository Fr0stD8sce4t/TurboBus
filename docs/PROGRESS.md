# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The system-level runtime path submits `TransferIntent`, uses daemon scheduling,
issues `ExecutionTicket` plans, and keeps the old `client_transfer.py` file
deleted. `TurboBusRuntimeSession.open()` and `open_socket()` are the public
system entries: they own daemon clients, optional worker socket clients,
session/job/buffer registration, profile bootstrap, intent submission, and
receipt waits without application-side relay selection.

Model loading, training offload, inference KV, vLLM KV, vLLM connector, and
lower-level vLLM integration paths now construct their workload adapters from
`TurboBusRuntimeSession` instead of requiring application code to assemble
daemon clients, transfer contexts, or buffer registration manually.
`OffloadStore` accepts only runtime-session-owned clients whose job and session
identity match the adapter context.

Runtime-session submit and wait paths verify returned receipts belong to the
runtime job/session and that receipt ticket metadata matches the daemon-issued
ticket id before the application consumes them. Adapter-owned offload handles
now apply the same ownership checks on submit and wait, and reject closed
runtime sessions before submitting or waiting for receipt state.

Daemon, scheduler, worker, and backend paths keep execution bound to
daemon-issued tickets. Completed transfer tickets are archived for receipt and
release evidence, removed from active execution-ticket state, and cleanup paths
retire affected transfers from runtime scheduling while keeping terminal
receipt data available to authenticated owners.

Server-only validation remains deferred until after the full system
implementation pass. Current code work should continue through code reading,
implementation, refactoring, and existing minimal local checks without adding
server test commands or server-validation gates.

## Completed This Round

- Bound `ReceiptTransferHandle` to its runtime-session-owned client before
  submit and wait, so closed sessions cannot accept later adapter transfer
  operations.
- Added adapter-level receipt validation for intent id, job id, session id,
  adapter context ownership, daemon ticket metadata, and complete-receipt
  evidence before adapter code consumes a `TransferReceipt`.
- Updated the active plan files to keep the next entry on daemon cleanup,
  release, and delayed admission system code, with server validation deferred.

## Validation

- `python -m py_compile turbobus\offload_store.py
  turbobus\adapters\model_loading.py turbobus\adapters\training_offload.py
  turbobus\adapters\inference.py turbobus\adapters\vllm.py
  turbobus\adapters\vllm_kv_connector.py` passed.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain deferred until the full system implementation
  pass is complete.
- Existing unit coverage may still include old fake-client assumptions for
  `OffloadStore`; do not weaken production runtime-session ownership checks to
  satisfy those fake clients during this implementation stage.

## Next Main Target

Continue the code implementation pass by inspecting daemon cleanup, release,
and delayed admission paths for stale execution-ticket or scheduling-state
reuse while keeping server validation deferred until the full system
implementation pass is complete.
