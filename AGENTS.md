# TurboBus Agent Instructions

TurboBus is a paper-reproduction system project for:

TurboBus: Pooling PCIe Bandwidth for LLM Workloads via Scale-Up Fabrics.

The target system pools idle PCIe bandwidth in a multi-GPU server for large
model memory movement. Applications submit transfer intent. A privileged
per-node daemon discovers machine topology, schedules cross-job transfers,
issues execution tickets, and records completion. Workers and backend data
planes execute exact daemon-issued plans.

## Active Direction

Use `docs/NEXT_STEPS.md` as the only active per-round implementation plan.
Use `docs/TURBOBUS_ROADMAP.md` for the complete system reproduction route.

The old phase-by-phase plan has been retired. Do not restart from Phase 0,
Phase 6, or Phase 7 documents. Historical phase inventories are not current
plans and must not drive implementation.

Current system priority: close the daemon-issued H2D / D2H execution path,
including mixed pooled direct-plus-relay plans, so one `TransferIntent` can
produce a daemon `SchedulingDecision`, daemon-issued `ExecutionTicket`,
worker/backend execution, terminal status, cleanup, and a real
`TransferReceipt`.

Every code change should move the project closer to:

- real daemon-issued H2D and D2H execution;
- direct, relay, and mixed pooled worker/backend data movement;
- receipts created from worker/backend completion or explicit failure;
- runtime load feedback into scheduling;
- cross-job isolation during shared relay use;
- vLLM, model-loading, and offload workloads using real registered buffers
  through `TurboBusRuntimeSession`.

## System Contract

The production path must satisfy these contracts:

- Applications describe what data must move, not which physical route to use.
- The daemon is the production scheduling authority.
- The scheduler is the only component that creates production transfer plans.
- Topology is discovered by daemon-owned providers.
- Synthetic topology is used only by explicit tests and fixtures.
- Workers execute only daemon-issued `ExecutionTicket` objects.
- Direct, relay, and pooled paths are scheduling outcomes.
- Adapters use the public `TurboBusRuntimeSession` API and shared schema
  objects.
- Benchmarks and examples must not define core architecture.
- Paper evidence must prove executed and verified bytes, not just scheduled
  intent.

## Current Code Path To Inspect First

- `turbobus/daemon/server.py`: transfer submission, planning, worker
  authorization, status update, receipt creation, cleanup, and runtime
  feedback.
- `turbobus/daemon/dispatch.py`: daemon request routing.
- `turbobus/runtime_session.py`: production system-level API.
- `turbobus/intent_executor.py`, `turbobus/direct_fallback.py`, and
  `turbobus/buffer_registration.py`: intent-to-execution bridge, backend
  direct execution, and registered buffer conversion.
- `turbobus/backends/cuda.py`, `turbobus/native_runtime.py`,
  `turbobus/native_plan.py`, and `turbobus/profiling/bootstrap.py`: native CUDA
  backend, exact-plan conversion, profile bootstrap, and daemon profile import.
- `turbobus/worker/lifecycle.py`: authorization, execution, status, cleanup.
- `turbobus/worker/models.py`: worker request and completion envelopes.
- `turbobus/worker/validation.py`: ticket and lease validation.
- `turbobus/worker/cuda_executor.py`: CUDA worker execution.
- `cpp/src/executor_cuda.cu`: native direct, relay, and pooled transfer engine.
- `turbobus/adapters/`, `turbobus/offload/`, and `turbobus/adapters/vllm*`:
  framework-facing runtime-session integration after the transfer path closes.

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

These items can return only as validators around real execution after the core
system path is complete.

## Core Objects

Use these shared objects across the control plane, scheduler, data plane,
adapters, tests, and experiments:

- `JobIdentity`: user, job, process, container, and session identity.
- `BufferHandle`: registered CPU or GPU buffer owned by a job.
- `TransferIntent`: requested movement, direction, byte ranges, workload kind,
  priority, and policy hints.
- `TopologySnapshot`: daemon-discovered GPU, PCIe, NUMA, and fabric state.
- `SchedulingDecision`: daemon-selected chunk-level plan and fallback reason.
- `ExecutionTicket`: daemon authorization for a worker or backend to execute
  one exact plan.
- `TransferReceipt`: completion state, bytes, timing, path split, and errors.

Tests may construct invalid variants of these objects only when the purpose is
to validate rejection behavior. During the current system implementation pass,
do not add new tests unless the active plan explicitly moves into the test and
validation stage.

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
- After a refactor, delete old files that only re-export moved symbols. Update
  imports to the new owning modules instead of preserving compatibility export
  layers.
- Keep client API, daemon control plane, scheduler, topology discovery, worker
  data plane, backend execution, and framework adapters separate.
- Keep native data movement framework-agnostic.
- Do not let benchmark scripts become the system.
- Keep direct transfer fallback available as a scheduler result or explicit
  failure fallback.
- For documentation-only changes, `git diff --check` is sufficient.
