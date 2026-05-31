# TurboBus Agent Instructions

TurboBus is a paper-reproduction system project for:

TurboBus: Pooling PCIe Bandwidth for LLM Workloads via Scale-Up Fabrics.

The target system pools idle PCIe bandwidth in a multi-GPU server for large
model memory movement. Applications submit transfer intent. A privileged
per-node daemon discovers machine topology, schedules cross-job transfers,
issues execution tickets, and records completion. Workers and backend data
planes execute exact daemon-issued plans.

## Active Direction

Use `docs/NEXT_STEPS.md` as the only active forward plan.

The old phase-by-phase plan has been retired. Do not restart from Phase 0 and
do not continue the old Phase 7 artifact-chain work unless it is reintroduced
as a small validator around real server execution.

Current first task: implement the intent-to-worker execution loop.

Every code change should move the project closer to:

- real daemon-issued H2D and D2H execution;
- relay and pooled worker/backend data movement;
- receipts created after worker/backend completion;
- runtime load feedback into scheduling;
- cross-job isolation during shared relay use;
- vLLM, model-loading, and offload workloads using real buffers.

## System Contract

The production path must satisfy these contracts:

- Applications describe what data must move, not which physical route to use.
- The daemon is the production scheduling authority.
- The scheduler is the only component that creates production transfer plans.
- Topology is discovered by daemon-owned providers.
- Synthetic topology is used only by explicit tests and fixtures.
- Workers execute only daemon-issued ExecutionTickets.
- Direct, relay, and pooled paths are scheduling outcomes.
- Adapters depend on the public client API and shared schema objects.
- Benchmarks and examples call the public client API.
- Paper evidence must prove executed and verified bytes, not just scheduled
  intent.

## Current Code Path To Inspect First

- `turbobus/daemon/server.py`: transfer submission, planning, worker
  authorization, status update, receipt creation, cleanup.
- `turbobus/daemon/dispatch.py`: daemon request routing.
- `turbobus/client_transfer.py`: worker-managed transfer client path.
- `turbobus/worker/lifecycle.py`: authorization, execution, status, cleanup.
- `turbobus/worker/models.py`: worker request and completion envelopes.
- `turbobus/worker/validation.py`: ticket and lease validation.
- `turbobus/worker/cuda_executor.py`: CUDA worker execution.
- `benchmarks/model_loading.py`, `benchmarks/training_offload.py`, and
  `benchmarks/paper_validation.py`: must not pass paper validation with
  intent-only receipts.

## Retired Work

Do not add or rebuild:

- standalone Phase 7 result checkers;
- comparison-only tools;
- synthetic evidence assemblers;
- bundle gates;
- acceptance inventories;
- artifact ingestion commands;
- dry-run server wrappers whose output is the deliverable;
- compatibility wrappers for removed Runtime or planner APIs;
- application-side target GPU, relay GPU, direct, relay, pool, or mode controls;
- ROCm/HIP backend work before the CUDA reproduction path is end-to-end.

These items can return only when they directly validate a real execution path.

## Core Objects

Use these shared objects across the control plane, scheduler, data plane,
adapters, tests, and experiments:

- JobIdentity: user, job, process, container, and session identity.
- BufferHandle: registered CPU or GPU buffer owned by a job.
- TransferIntent: requested movement, direction, byte ranges, workload kind,
  priority, and policy hints.
- TopologySnapshot: daemon-discovered GPU, PCIe, NUMA, and fabric state.
- SchedulingDecision: daemon-selected chunk-level plan and fallback reason.
- ExecutionTicket: daemon authorization for a worker or backend to execute one
  exact plan.
- TransferReceipt: completion state, bytes, timing, path split, and errors.

Tests may construct invalid variants of these objects only when the purpose is
to validate rejection behavior.

## Anti-Drift Rules

Rewrite changes that:

- put physical route selection into application code;
- bypass daemon scheduling to create production plans;
- complete transfers without worker/backend completion evidence;
- make synthetic topology a production fallback;
- let adapters choose path policy;
- add benchmark-only APIs to core modules;
- add tests that do not protect a contract, integration path, or real workload;
- place framework policy inside CUDA or HIP execution code.

Prefer breaking changes when the existing code encodes the wrong architecture.

## Coding Rules

- Prefer simple, testable interfaces over compatibility shims.
- Keep client API, daemon control plane, scheduler, topology discovery, worker
  data plane, backend execution, and framework adapters separate.
- Keep native data movement framework-agnostic.
- Do not let benchmark scripts become the system.
- Keep direct transfer fallback available as a scheduler result or explicit
  failure fallback.
- Add focused tests that protect the daemon-first execution contract.
- For documentation-only changes, `git diff --check` is sufficient.
