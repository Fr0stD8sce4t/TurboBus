# vLLM Integration Plan

This document describes the vLLM adapter target for the rewritten TurboBus
system.

## Goal

Support real vLLM KV prefix save and restore through the official connector
lifecycle, with daemon-approved relay use when needed.

## Required Behavior

- observe real vLLM KV cache tensors;
- map request block ids to TurboBus block references;
- save prefixes into pinned CPU backing;
- restore prefixes into newly allocated KV cache blocks;
- report stats, timing, and fallback reason;
- respect daemon isolation policy without choosing route, target, relay, or
  pool policy in vLLM code.

## Connector Responsibilities

- bind to the active vLLM KV cache state;
- register save and restore metadata;
- track per-request block ids and prefix keys;
- manage CPU backing allocation or reuse;
- emit save and restore events;
- clean up saved prefixes when a request or session ends.
- submit data movement only through `TurboBusRuntimeSession`.

## Integration Points

The connector should hook into the real vLLM lifecycle around:

- KV cache initialization;
- slot allocation;
- request completion;
- load and save entry points;
- connector metadata construction.

## Data Model

The vLLM adapter should represent:

- request id;
- prefix key;
- block ids per layer;
- CPU slot mapping;
- GPU slot mapping;
- lane or lane-like index when needed;
- byte count per block or range.

## What The Adapter Must Not Do

- do not replace the vLLM scheduler;
- do not embed daemon policy in framework code;
- do not assume the client owns relay GPUs directly;
- do not make connector events the source of global transfer policy.
- do not expose direct, relay, pool, target GPU, or relay GPU controls.

## Future Validation Target

After the system transfer path is complete, the vLLM integration should be
validated with:

- unit tests for block-id extraction and prefix mapping;
- a small real-framework smoke path when the server environment is ready.
