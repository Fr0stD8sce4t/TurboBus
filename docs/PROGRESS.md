# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The active route is the system-level Python runtime path. The old
`client_transfer.py` file remains deleted, transfers run through
`TurboBusRuntimeSession`, profile bootstrap writes daemon profile data, daemon
and worker CLIs run socket services, and upper adapters use the runtime session
without application-side path selection.

## Completed This Round

- Daemon cleanup, release, and transfer status paths now receive socket peer
  identity and validate session, job, buffer, or lease ownership.
- Worker authorization payload parsing now rejects a top-level `transfer_id`
  that does not match the daemon-issued ticket metadata.
- Relay intent tickets now expire with the earliest active relay lease instead
  of outliving their lease authority.
- Worker/backend status completion remains tied to admitted transfers and
  daemon-issued tickets.

## Validation

- `python -m py_compile turbobus/daemon/server.py turbobus/daemon/dispatch.py turbobus/worker/validation.py turbobus/worker/lifecycle.py turbobus/worker/models.py`
  passed.
- `python -m unittest test.python.integration.test_worker_helper` passed.
- `python -m unittest test.python.integration.test_client_worker_transfer`
  passed with one expected platform skip.
- `python -m unittest test.python.integration.test_daemon_socket` passed with
  expected platform skips.
- `python -m unittest test.python.integration.test_paper_main_path` passed.
- `python -m unittest test.python.unit.test_daemon_scheduler` passed.

## Remaining Risk

- Peer isolation has not yet been validated with separate OS users or
  containers on the real daemon socket.
- Runtime/native profile and plan conversion still need a focused pass before
  real CUDA multi-GPU validation.

## Next Main Target

Harden the runtime/native boundary for profile bootstrap, tensor validation,
native plan conversion, and CUDA worker execution.
