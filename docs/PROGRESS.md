# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The active route is the system-level Python runtime path. The old
`client_transfer.py` file remains deleted, transfers run through
`TurboBusRuntimeSession`, profile bootstrap writes daemon profile data, daemon
and worker CLIs run socket services, and upper adapters use the runtime session
without application-side path selection.

## Completed This Round

- Scheduler relay policy now separates available, deferred, and filtered relay
  candidates in daemon-owned metadata.
- When delayed admission is allowed, the scheduler prefers currently available
  relays and only plans against deferred relays when no available relay exists.
- Profile misses and unusable relay profiles still resolve to explicit direct
  fallback decisions.
- Reschedule now clears old leases and tickets before creating the replacement
  plan, and leaves the transfer in delayed admission if reschedule fails.

## Validation

- `python -m py_compile turbobus/scheduler/daemon.py turbobus/daemon/server.py turbobus/daemon/dispatch.py`
  passed.
- `python -m unittest test.python.unit.test_daemon_scheduler` passed.
- `python -m unittest test.python.integration.test_paper_main_path` passed.
- `python -m unittest test.python.integration.test_daemon_socket` passed with
  expected platform skips.
- `python -m unittest test.python.integration.test_client_worker_transfer`
  passed with one expected platform skip.

## Remaining Risk

- Scheduler policy has not yet been validated against a real CUDA multi-GPU
  server with concurrent jobs and live relay load.
- Worker authorization and cleanup still need a focused isolation pass to make
  sure stale tickets and leases cannot execute after cleanup or reschedule.

## Next Main Target

Harden isolation and authority across daemon-issued tickets, worker
authorization, status reporting, and cleanup.
