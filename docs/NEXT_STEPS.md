# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

The next task is to tighten isolation and authority boundaries around
daemon-issued `ExecutionTicket` execution and transfer cleanup after runtime
load feedback has been split into scheduler-owned code.

## Exit Criteria

- Workers and data-plane code execute only daemon-issued `ExecutionTicket`
  plans with matching job, session, buffer, ticket, and plan-generation data.
- Daemon release and cleanup keep completed receipts consumable while retiring
  lease, ticket, admission, peer, and runtime state consistently.
- Applications and adapters still submit only `TransferIntent` and consume
  `TransferReceipt`.
- No benchmark, paper-validation, experiment, server-validation, compatibility
  shim, or export layer code is added during this pass.

## Current Code Work

- Inspect `turbobus/daemon/server.py`, `turbobus/worker/lifecycle.py`,
  `turbobus/worker/validation.py`, `turbobus/worker/resources.py`, and
  `turbobus/worker/cuda_executor.py` for authority and cleanup boundaries.
- Keep offload and vLLM adapter handoff owned by `TurboBusRuntimeSession`; vLLM
  connector prefix state uses the daemon runtime session id while preserving
  the connector engine id only as metadata.
- Keep the old `client_transfer.py`, `turbobus/worker/helper.py`,
  `turbobus/daemon/protocol.py`, and `turbobus/worker_managed.py` files
  deleted. Do not recreate compatibility export layers.
- Continue code implementation and refactoring without adding server test
  commands or using server validation as the current entry point.

## Next Entry

Continue the code implementation pass by inspecting isolation and authority
hardening in daemon-issued ticket execution and cleanup. Keep the work focused
on system code; defer tests, benchmarks, paper-validation, experiments, and
server validation until the full system implementation pass is complete.
