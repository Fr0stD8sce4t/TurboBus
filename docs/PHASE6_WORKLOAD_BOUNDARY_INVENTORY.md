# Archived Phase 6 Workload Boundary Inventory

This file is retained only as historical context. It is not an active plan and
must not be used to drive current implementation.

The old Phase 6 inventory described a local daemon-first workload boundary and
paper-validation shape. That phase language is now retired because the system
path still needs code closure before benchmark, paper-validation, server
validation, or experiment work should drive implementation.

Current implementation guidance lives in:

- `AGENTS.md`
- `docs/TURBOBUS_ROADMAP.md`
- `docs/NEXT_STEPS.md`
- `docs/PROGRESS.md`

The current system target is real daemon-issued H2D / D2H execution, including
mixed pooled direct-plus-relay plans, worker/backend completion evidence,
cleanup, runtime feedback, and `TransferReceipt` generation.

Workload adapters remain important, but they should be advanced only through
the unified `TurboBusRuntimeSession` API after the transfer path itself is
complete enough for real buffers and receipts.
