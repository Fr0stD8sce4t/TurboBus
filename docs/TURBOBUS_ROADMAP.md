# TurboBus Roadmap

This roadmap describes the complete system-code route for reproducing
TurboBus. It replaces the retired phase-by-phase and artifact-only plans.
Use `docs/NEXT_STEPS.md` for the current implementation target.

## Goal

Reproduce TurboBus as a system that pools idle PCIe bandwidth for LLM memory
movement by routing CPU-GPU transfers through relay GPUs over scale-up fabric.

The code must prove:

- real H2D and D2H bytes move through daemon-issued plans;
- direct, relay, and mixed pooled paths execute through worker/backend code;
- chunk-level path split, timing, cleanup, and correctness are observable;
- cross-job scheduling reacts to live load and respects ownership;
- vLLM KV, model loading, and offload workloads run through
  `TurboBusRuntimeSession` without application-side physical route control.

## System Implementation Sequence

1. Runtime session authority

   `TurboBusRuntimeSession` owns production startup for daemon socket clients,
   worker clients, profile bootstrap, session/job registration, buffer
   registration, adapter construction, and receipt consumption.

2. Daemon-issued H2D / D2H execution lifecycle

   A `TransferIntent` must produce a daemon `SchedulingDecision`, a bound
   `ExecutionTicket`, worker/backend execution, status updates, cleanup, and a
   `TransferReceipt` from real completion or explicit failure.

3. Native direct, relay, and mixed pooled data path closure

   Direct-only, relay-only, and mixed pooled plans must execute as exact
   daemon-issued plans. Native runtime, backend conversion, worker execution,
   and receipt evidence must agree on the same chunk-level contract.

4. Buffer lifetime closure

   Shared pinned CPU buffers and CUDA IPC GPU buffers must be registered,
   opened by the correct process, used only inside daemon-issued plans, and
   released on success, failure, cleanup, and session close.

5. Production daemon and worker startup

   Daemon and worker socket processes must start from production topology
   discovery, reject synthetic production topology, bind peer identity where
   available, and execute ticketed transfers without application-side relay
   ownership.

6. Scheduler and runtime load feedback

   Scheduler decisions must consume live queued/running/active transfer state,
   relay leases, staging usage, completion sources, and job weights so
   cross-job sharing is observable and isolated.

7. Cross-job isolation and ownership hardening

   Job, session, buffer, lease, and cleanup ownership must stay bound to
   authenticated peers or explicit daemon-owned identities so shared relay use
   does not weaken isolation.

8. Framework adapter closure

   Offload, inference, model-loading, training, and vLLM adapters must register
   real buffers through `TurboBusRuntimeSession`, submit H2D/D2H transfer
   intent, and consume `TransferReceipt` without seeing route, relay, or target
   policy.

9. Validation and evaluation

   After the system path is complete, add or repair tests, benchmarks, paper
   validation, server validation, and paper experiments around real executed
   evidence. Do not use JSON artifacts, synthetic topology, fake receipts, or
   dry-run wrappers as reproduction proof.

## Current Priority Interpretation

Treat items 1 through 7 as system implementation work. Treat items 8 and 9 as
follow-on work. If a change mainly improves benchmark usability, paper
reporting, or comparison output but does not close a system contract above, it
is out of order for the current pass.

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
