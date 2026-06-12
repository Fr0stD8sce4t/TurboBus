# TurboBus Progress

Keep this file short and forward-looking. Replace current state after each
completed system capability loop. Do not accumulate implementation history.

## Current State

- The project is in system-code reproduction. Benchmark runs, examples, paper
  validation, server validation, new tests, mock gates, fake evidence,
  synthetic evidence, and dry-run deliverables remain deferred.
- The current code already has daemon-owned transfer intent submission,
  scheduler decisions, execution tickets, lease validation, worker status
  updates, block runtime metadata, terminal receipts, and a daemon transfer
  lifecycle boundary for terminal persistence, finalization, cleanup contract,
  archive records, and recovery payloads.
- Worker/backend execution now consumes daemon-issued block runtime metadata
  through exact `ExecutionTicket` objects. CUDA worker completion and failure
  paths return block-level progress evidence, and the daemon consumes that
  evidence into block runtime state, cleanup evidence, archive, recovery, and
  receipts.
- Shared buffer ownership is now part of the daemon-issued execution lifecycle.
  Worker resource retention evidence flows through cleanup and daemon receipt
  state for registered pinned CPU buffers and CUDA IPC GPU buffers.
- Runtime startup, session/job registration, buffer registration, transfer
  submission, worker dispatch, receipt consumption, cleanup, and close now have
  a `TurboBusRuntimeSession` entrypoint record. It ties managed service state,
  daemon registration, daemon-issued execution, finalized receipts, and buffer
  lifecycle into one production boundary without exposing route choice.
- Workload adapters now consume `TurboBusRuntimeSession` buffers and receipts
  through adapter lifecycle evidence. Model loading, training offload, and vLLM
  KV connector evidence now record adapter intent/receipt bindings back into
  the RuntimeSession entrypoint record. Runtime evidence validation rejects
  missing RuntimeSession, missing adapter evidence records, fake evidence, or
  exposed route policy.
- The low-level `OffloadStore -> ReceiptTransferHandle` path now also records
  submit and wait receipt bindings into the RuntimeSession entrypoint record,
  so adapter code cannot consume direct receipt handles without RuntimeSession
  evidence.
- Adapter construction helpers now record `AdapterTransferContext` creation
  into the RuntimeSession entrypoint record. Adapter lifecycle validation now
  requires the RuntimeSession snapshot to prove both adapter construction and
  adapter receipt evidence before accepting lifecycle evidence.
- The next work stays inside system-code refactoring. The active direction is
  to converge production boundaries around `TurboBusRuntimeSession`, adapter
  lifecycle evidence, and daemon-issued receipts without starting validation or
  benchmark work.
- Goal-mode progress must close complete system capability loops. Small fixes,
  field moves, helper relocation, import cleanup, and documentation updates do
  not count unless they finish the same active loop.

## Remaining Risk

- PCIe load is currently derived from daemon active path state. Hardware counter
  sampling can replace or enrich it later, but must report explicit unknown
  state when unavailable.
- Functional validation, server validation, benchmark execution, paper
  validation, vLLM validation, and multi-GPU execution remain deferred to the
  later validation and evaluation stage.
- Existing unrelated dirty files may be present. Each goal-mode round must
  inspect `git status`, avoid reverting unrelated work, and stage only files for
  the active target.
- Hardware-backed CUDA, vLLM, multi-GPU, server, benchmark, and paper
  validation remain unproven. This is an external validation risk, not a reason
  to add mock gates, fake receipts, synthetic evidence, or dry-run substitutes.

## Next Main Target

Converge the production boundary around `TurboBusRuntimeSession` and adapter
lifecycle evidence.

This is a system-code refactor target. Next inspect remaining runtime-looking
runtime lifecycle and close/recovery paths under `turbobus/runtime/` and
`turbobus/runtime_session.py`. Tighten any path that can produce receipt
evidence, recovery evidence, or lifecycle state without a RuntimeSession
entrypoint record. Validation may resume only when benchmark, example, paper
validation, server validation, vLLM validation, and multi-GPU execution are
explicitly allowed.
