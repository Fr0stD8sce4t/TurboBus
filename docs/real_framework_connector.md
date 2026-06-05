# Real Framework Connector Plan

TurboBus needs thin connectors for real frameworks, not framework logic inside
the native transfer engine.

## Connector Rules

- Keep request, token, and policy logic above the backend layer.
- Translate framework lifecycle events into runtime-session buffer
  registration, `TransferIntent` submission, and `TransferReceipt`
  consumption.
- Report bytes, chunks, timing, and fallback reasons back to the framework or
  benchmark layer.
- Avoid hard-wiring framework scheduler behavior into the executor.
- Do not expose daemon route, relay, target, lease, or ticket policy to
  framework code.

## Shared Connector Shape

Each connector should be able to:

- register framework-owned buffers;
- submit save or restore requests;
- wait for completion;
- query transfer stats;
- handle cleanup when a request finishes or is canceled;
- consume receipts through `TurboBusRuntimeSession`.

## First Targets

1. vLLM
   - KV prefix save/restore.
   - Layer-aware block references.
   - Real request lifecycle integration.

2. Model loading
   - Weight bucket registration.
   - Prefetch into GPU memory.
   - Packed and unpacked layouts.

3. Training offload
   - Parameter and optimizer bucket movement.
   - Prefetch and offload paths.

## Architecture Principle

Framework connectors should be replaceable. If a connector needs direct access
to planner internals or native executor details, the connector design should be
split again.
