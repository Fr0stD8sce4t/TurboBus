# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The system-level runtime path submits `TransferIntent`, uses daemon scheduling,
issues `ExecutionTicket` plans, and keeps the old `client_transfer.py` file
deleted. Daemon planning now uses one daemon-owned topology snapshot for both
the scheduling snapshot id and relay eligibility, and scheduler decisions carry
topology metadata beside runtime load metadata. Worker authorization-failure
cleanup now requires daemon-issued ticket context before it can touch daemon
reservation or session state. Worker socket/service envelopes can no longer
request session-wide cleanup. Reservation release for intent transfers now
rechecks stored completion evidence against the current daemon-issued ticket.
Daemon and worker socket servers now create owner-only Unix socket files on
POSIX platforms and refuse to unlink non-socket paths during startup.
Production daemon instances now require authenticated socket peers and refuse
to serve on platforms where the current Unix credential mechanism is
unavailable.

## Completed This Round

- Added a production peer-authentication policy to `TurboBusDaemon`.
- `create_production_daemon()` now enables authenticated peer requirements for
  production daemon instances.
- Daemon socket startup now fails before serving when Unix sockets or supported
  peer credentials are unavailable.
- Daemon request dispatch now rejects unauthenticated peers before ownership,
  lease, ticket, or transfer-state handlers run.

## Validation

- `python -m py_compile turbobus\daemon\server.py turbobus\daemon\startup.py turbobus\daemon\__main__.py` passed.
- `python -m unittest test.python.integration.test_daemon_state` passed.
- `python -m unittest test.python.integration.test_daemon_socket` passed with
  expected platform skips.
- `python -m unittest test.python.unit.test_topology_provider.TopologyProviderTest.test_production_startup_selects_eligible_relays_from_provider_inventory`
  passed.
- `python -m unittest test.python.unit.test_topology_provider` still fails in
  the existing CLI parser test because that test omits the already-required
  `--socket-path` argument; this was not changed in this round.
- `git diff --check` passed.

## Remaining Risk

- Real CUDA/native multi-GPU execution has not been validated in this local
  environment.
- The current production peer credential implementation is Linux `SO_PEERCRED`
  only; unsupported platforms now fail closed instead of weakening isolation.

## Next Main Target

Tighten cross-job isolation and daemon authority in the daemon/worker
production path.
