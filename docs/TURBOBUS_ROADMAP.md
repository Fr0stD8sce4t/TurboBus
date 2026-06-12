# TurboBus Roadmap

This roadmap describes the complete system-code route for reproducing
TurboBus. It replaces retired phase-by-phase, artifact-only, benchmark-first,
and validation-first plans. Use `docs/NEXT_STEPS.md` for the current
implementation target.

## Goal

Reproduce TurboBus as a system that pools idle PCIe bandwidth for LLM memory
movement by routing CPU-GPU transfers through relay GPUs over scale-up fabric.

The code must prove through system structure and executed transfer paths that:

- applications submit transfer intent, not physical routes;
- daemon and scheduler produce all production transfer plans;
- PCIe fabric capacity and live load feed direct, relay, and mixed pooled path
  selection;
- block-level transfer work is split, issued, executed, tracked, cleaned up,
  and summarized by receipts;
- workers and backend data planes execute daemon-issued tickets or exact
  daemon-issued plans only;
- model loading, KV cache, training state, and optimizer offload use
  `TurboBusRuntimeSession` without route control.

## Goal Mode Rules

Each implementation round must start from `docs/NEXT_STEPS.md`. If target
state conflicts across files, use this priority:

1. `docs/NEXT_STEPS.md`;
2. `docs/PROGRESS.md`;
3. `AGENTS.md`;
4. this roadmap.

Each round must close one independently describable system capability loop.
Do not count a local bug fix, field rename, helper move, import cleanup,
boundary tightening, or documentation-only update as a system capability loop.

Stop when the current main target is closed. Do not auto-start the next roadmap
item unless it is a minimal blocker for the active target.

The lead agent owns final judgement for each round. Sub-agents may inspect or
edit disjoint areas in parallel, but the lead agent must integrate the result,
verify the active target, and decide whether the system capability loop is
actually closed.

Suggested parallel split for large rounds:

- scheduler/control-plane agent: scheduling decisions, tickets, leases, daemon
  runtime records, and load feedback;
- worker/data-plane agent: worker lifecycle, backend execution, CUDA executor
  envelopes, completion evidence, and cleanup;
- session/adapter-boundary agent: `TurboBusRuntimeSession`, buffer ownership,
  adapter boundaries, and API surface contraction;
- verification agent: minimal existing checks that directly cover the active
  target, plus diff and staging audit.

Use sub-agents only for bounded, non-overlapping work. Do not let a sub-agent
choose the main target, change the roadmap order, add benchmark validation, or
define architecture from examples.

## Completion Judgement

A round is complete only when all of the following are true:

- `docs/NEXT_STEPS.md` and `docs/PROGRESS.md` agree on the current target state;
- the code change adds one independently describable system capability loop;
- daemon/scheduler ownership of production plans remains intact;
- applications, adapters, benchmarks, workers, and CUDA code do not gain route
  choice over direct, relay, pool, target GPU, or relay GPU;
- no benchmark, example, paper-validation, server-validation, mock, fake
  receipt, synthetic evidence, or dry-run deliverable was added for the current
  system-code stage;
- the minimal relevant existing checks passed, or the failure is stated with a
  concrete external blocker;
- the staged diff contains only the active round's files;
- the final answer reports the target, closed capability, key files, checks,
  remaining validation risk, commit id, and push result.

## Architecture Guardrails

- The daemon and scheduler are the only production source of transfer plans.
- Applications, benchmarks, examples, and adapters may submit
  `TransferIntent` and consume `TransferReceipt` only.
- Workers, data planes, and CUDA executors execute `ExecutionTicket` objects or
  exact daemon-issued plans only.
- Applications, benchmarks, examples, adapters, workers, and CUDA executors must
  not choose direct, relay, pool, target GPU, or relay GPU routes.
- Do not restore old `Runtime` or planner compatibility APIs.
- Do not restore single-process, single-job, or manual relay production routes.
- Synthetic topology, fake receipts, JSON artifacts, and dry-run output are not
  reproduction evidence.
- Benchmarks and examples must not define core architecture.

## System Capability Sequence

1. PCIe shared-fabric bandwidth pool

   Discover or import daemon-owned PCIe hierarchy, model shared roots and
   upstream links, attach per-link capacity, sample or report PCIe load, and
   expose available pooled bandwidth to daemon scheduling.

2. Block-level scheduling and dynamic path allocation

   Split large transfers into blocks, allocate blocks across direct, relay, and
   mixed pooled paths using PCIe bandwidth-pool state plus runtime load, and
   preserve a daemon-owned plan contract.

3. Daemon block runtime, tickets, leases, progress, and receipts

   Convert scheduled blocks into execution tickets, track block attempts and
   leases, aggregate progress and partial failures, clean up ownership, and
   produce receipts from real completion or explicit failure.

4. Worker/backend block execution

   Execute daemon-issued direct, relay, and mixed pooled block plans through
   worker, backend, and CUDA paths. Completion evidence must match the issued
   block plan.

5. Buffer lifecycle closure

   Register shared pinned CPU buffers and CUDA IPC GPU buffers, bind them to
   job/session ownership, open them only for authorized tickets, and release
   them on success, failure, cleanup, and session close.

6. Runtime session production closure

   Keep `TurboBusRuntimeSession` as the single production entry for daemon
   socket clients, worker clients, profile/bootstrap state, session/job
   registration, buffer registration, adapter construction, transfer
   submission, and receipt consumption.

7. Workload adapter closure

   Connect model loading, KV cache, training state, and optimizer offload to
   real buffer registration and transfer receipts. Optimizer and training state
   paths must not remain workload-kind labels without real state movement.

8. Validation and evaluation

   After the system path is complete, add or repair tests, benchmarks, server
   validation, paper validation, and paper experiments around real executed
   evidence. Do not use JSON artifacts, synthetic topology, fake receipts, or
   dry-run wrappers as reproduction proof.

## Current Stage Boundary

The current stage advances system-code reproduction only. Do not advance
benchmark, example, paper validation, server validation, new tests, mock gates,
fake receipts, synthetic evidence, dry-run deliverables, or comparison-only
tools unless `docs/NEXT_STEPS.md` explicitly moves the project into that stage.

Adapter migration is allowed only when it directly blocks the active system
target.

## Deferred Direction

The following work is deferred until the CUDA reproduction path is end-to-end:

- standalone result checkers;
- comparison-only tools;
- evidence JSON assemblers;
- bundle gates;
- acceptance inventories;
- artifact ingestion wrappers;
- server-run wrappers whose dry-run output is the main deliverable;
- ROCm/HIP backend work.

These can return later only as thin validators around real server execution.
