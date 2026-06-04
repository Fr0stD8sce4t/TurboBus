# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The daemon-first path remains the production route:
`TransferIntent` submission, daemon scheduling, daemon-issued
`ExecutionTicket`, worker/backend completion, and `TransferReceipt`
consumption through `TurboBusRuntimeSession`.

Server validation, benchmark work, paper validation, experiments, and new test
code remain deferred until the full system implementation pass is complete.

## Completed This Round

- Made daemon runtime feedback distinguish delayed queued transfers from
  admitted/running active execution.
- Scheduler load accounting now receives delayed transfer counts while active
  bytes and relay busy feedback are based on admitted submitted transfers and
  running worker/backend transfers.
- Delayed admission replanning now excludes the same transfer's old transfer
  records, active paths, reservations, leases, staging records, and resource
  summaries before asking the scheduler for a new plan.
- Added no test, experiment, benchmark, paper-validation, server-validation, or
  compatibility export-layer code.

## Validation

- `python -m py_compile` passed for daemon server, scheduler load feedback, and
  daemon scheduler modules.
- Searches confirmed delayed transfer counts and admission-aware active
  execution checks are confined to daemon runtime feedback and scheduler load
  metadata.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests, examples, and benchmarks still contain old production-path
  assumptions; current-stage constraints defer migration until system
  implementation is complete.

## Next Main Target

Continue with one concrete implementation boundary: worker failure handling to
cleanup/receipt, runtime receipt validation, or adapter submission/receipt
consumption through `TurboBusRuntimeSession`.
