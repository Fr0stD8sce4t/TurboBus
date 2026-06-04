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
`TurboBusRuntimeSession`. Adapter-owned offload handles verify receipt
job/session/intent/ticket ownership before consuming `TransferReceipt`
objects, and closed runtime sessions reject later adapter submit or wait calls.

Daemon, scheduler, worker, and backend paths keep execution bound to
daemon-issued tickets. Completed transfer tickets are archived for receipt and
release evidence, removed from active execution-ticket state, and cleanup paths
retire affected transfers from runtime scheduling while keeping terminal
receipt data available to authenticated owners.

Reservation release, reservation cleanup, expired leases, job cleanup, buffer
cleanup, and session cleanup now share active-transfer retirement behavior:
once a transfer is terminal and has no remaining reservations, the daemon drops
active execution tickets, clears admission lease ids, removes queue records,
and prevents delayed admission from promoting stale transfer state.

Server-only validation remains deferred until after the full system
implementation pass. Current code work should continue through code reading,
implementation, refactoring, and existing minimal local checks without adding
server test commands or server-validation gates.

## Completed This Round

- Extended daemon active-transfer retirement so cleaned or terminal transfers
  drop active execution tickets and clear admission lease ids instead of only
  leaving the runtime queue.
- Linked reservation release/cleanup to active-transfer retirement after the
  last reservation is gone, while preserving status, intent, scheduling
  decision, completion evidence, and archived completion tickets for receipts.
- Updated the active plan files to keep the next entry on production
  daemon/worker startup and socket paths, with server validation deferred.

## Validation

- `python -m py_compile turbobus\daemon\server.py
  turbobus\worker\lifecycle.py turbobus\worker\models.py` passed.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain deferred until the full system implementation
  pass is complete.
- Production daemon/worker startup and socket ownership still need inspection
  to ensure they expose the unified runtime-session path without old manual
  relay or compatibility entry points.

## Next Main Target

Continue the code implementation pass by inspecting daemon and worker
production startup/socket paths while keeping server validation deferred until
the full system implementation pass is complete.
