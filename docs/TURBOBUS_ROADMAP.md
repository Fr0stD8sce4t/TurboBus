# TurboBus Roadmap

This roadmap has been reduced to the active paper-reproduction path. Historical
phase plans were removed so future work does not repeatedly reload completed
or artifact-only tasks.

## Goal

Reproduce TurboBus as a system that pools idle PCIe bandwidth for LLM memory
movement by routing CPU-GPU transfers through relay GPUs over scale-up fabric.

The code must prove:

- real H2D and D2H bytes move through daemon-issued plans;
- relay and pooled paths use worker/backend execution;
- chunk-level path split, timing, and correctness are observable;
- cross-job scheduling reacts to load and respects ownership;
- vLLM KV, model loading, and offload workloads run through the same public
  intent API without application-side physical route control.

## Active Sequence

1. Intent-to-worker execution loop.
2. Real buffer correctness gate.
3. Benchmark data-plane repair.
4. Runtime load feedback.
5. Isolation and authority hardening.
6. Real LLM workload closure.
7. Paper evaluation from real evidence.

Use `docs/NEXT_STEPS.md` for the concrete cuts and current first task.

## Retired Direction

Do not rebuild the old Phase 7 artifact chain before real execution is proven:

- standalone result checkers;
- comparison-only tools;
- evidence JSON assemblers;
- bundle gates;
- acceptance inventories;
- artifact ingestion wrappers;
- server-run wrappers whose dry-run output is the main deliverable.

These can return later only as thin validators around real server runs.
