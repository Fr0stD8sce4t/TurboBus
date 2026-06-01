# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The active route is the system-level Python runtime path. The old
`client_transfer.py` file remains deleted, transfers run through
`TurboBusRuntimeSession`, profile bootstrap writes daemon profile data, daemon
and worker CLIs run socket services, and upper adapters use the runtime session
without application-side path selection.

## Completed This Round

- Daemon status updates to `running` or `complete` now require an admitted,
  non-expired transfer plan.
- Intent transfer status updates to `running` or `complete` now require an
  existing daemon-issued `ExecutionTicket`.
- Profile-miss direct fallback is reflected in admission metadata instead of
  looking like a generic direct plan.
- Failed or canceled transfers now move admission state to a terminal state, so
  cleanup receipts no longer look admitted or delayed.

## Validation

- `python -m py_compile turbobus/daemon/server.py turbobus/daemon/dispatch.py turbobus/scheduler/daemon.py`
  passed.
- `python -m unittest test.python.integration.test_paper_main_path` passed.
- `python -m unittest test.python.integration.test_daemon_socket` passed with
  expected platform skips.
- `python -m unittest test.python.integration.test_client_worker_transfer`
  passed with one expected platform skip.

## Remaining Risk

- Runtime-session, vLLM, CUDA IPC, and native CUDA behavior still need a real
  CUDA multi-GPU server with daemon and worker socket services.
- Scheduler runtime policy still needs a focused pass so initial scheduling and
  reschedule share one daemon-owned relay/quota/fallback rule path.

## Next Main Target

Harden scheduler runtime policy for relay availability, quota, busy relay
state, fairness fallback, and reschedule.
