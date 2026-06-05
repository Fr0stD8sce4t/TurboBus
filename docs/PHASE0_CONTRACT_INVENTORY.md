# Archived Phase 0 Contract Inventory

This file is retained only as historical context. It is not an active plan and
must not be used to drive current implementation.

The Phase 0 inventory described an earlier migration away from route-shaped
runtime APIs toward daemon-first contract objects. Many file paths and
recommendations in that inventory are now obsolete, including references to
removed modules such as `turbobus/runtime.py`, `turbobus/offload_store.py`,
`turbobus/daemon/scheduler.py`, `turbobus/worker/helper.py`, and
`turbobus/client_transfer.py`.

Current implementation guidance lives in:

- `AGENTS.md`
- `docs/TURBOBUS_ROADMAP.md`
- `docs/NEXT_STEPS.md`
- `docs/PROGRESS.md`

The current system target is real daemon-issued H2D / D2H execution, including
mixed pooled direct-plus-relay plans, worker/backend completion evidence,
cleanup, runtime feedback, and `TransferReceipt` generation.
