# TurboBus System Design

## Design Goal

Build a daemon-managed PCIe bandwidth pooling system for LLM workloads.

The design should support:

- direct CPU-to-target GPU transfer;
- relay CPU-to-relay-GPU-to-target-GPU transfer;
- chunked and pipelined transfer execution;
- application isolation;
- cross-job relay sharing;
- CUDA scale-up fabrics first;
- framework adapters for vLLM, model loading, and training offload.

## Core Layers

### Client API

The client API is a thin submission layer.

Responsibilities:

- register pinned CPU buffers and destination GPU buffers;
- submit `TransferIntent` objects;
- wait for transfer completion;
- fetch transfer stats;
- expose framework-facing adapters through `TurboBusRuntimeSession`.

The client must not decide direct, relay, pooled, target GPU, or relay GPU
policy.

### Privileged Daemon

The daemon is the authority for relay sharing.

Responsibilities:

- discover machine topology;
- observe current utilization;
- track jobs, sessions, and users;
- create direct, relay, and mixed pooled transfer plans;
- choose relay GPUs;
- issue execution tickets;
- issue relay leases;
- enforce quota and isolation;
- reclaim stale resources;
- publish cached profiles.

### Worker Or Helper

The worker/helper performs daemon-ticketed data movement when the client should
not directly see relay GPUs.

Responsibilities:

- own relay GPU access;
- manage staging buffers;
- execute daemon-issued `ExecutionTicket` plans;
- validate lease tokens;
- clean up staging buffers.

### Backend Layer

Backends implement the actual copy operations.

Required backend capabilities:

- topology discovery;
- peer capability discovery;
- H2D, D2H, and P2P copy;
- staging buffer allocation;
- timing and stats collection;
- handle export and import for safe cross-process use when needed.

## Planner Model

Planner inputs:

- request bytes;
- chunk size;
- direction;
- direct bandwidth estimates;
- relay PCIe estimates;
- relay fabric estimates;
- current utilization;
- relay permissions;
- fallback policy.

Planner outputs:

- direct path chunk ranges;
- relay path chunk ranges;
- mixed pooled direct-plus-relay assignment groups;
- lease requirements;
- estimated completion time;
- fallback mode.

The planner must be backend-neutral.

## Data Path

1. Runtime session registers job, session, and buffers.
2. Application or adapter submits `TransferIntent`.
3. Daemon validates identity, topology, profiles, and ownership.
4. Scheduler creates direct, relay, or mixed pooled `SchedulingDecision`.
5. Daemon issues `ExecutionTicket` objects for exact planned execution.
6. Worker and backend execute only the daemon-issued plan.
7. Daemon records completion or explicit failure evidence.
8. Runtime session consumes one `TransferReceipt`.

## Isolation Rules

- A job cannot borrow another job's relay GPU without daemon approval.
- Relay staging buffers must not leak data between jobs.
- Lease expiry must trigger cleanup.
- Unauthorized requests must fail cleanly or fall back.

## Implementation Rule

If a feature requires the client or adapter to choose direct, relay, pooled,
target GPU, or relay GPU policy, it is not a final-system feature. It may be a
temporary development check, but not the production architecture.
