# TurboBus Progress

Keep this file short and forward-looking. Replace current state after each
completed system capability loop. Do not accumulate implementation history.

## Current State

- The project is in system-code reproduction. Benchmark runs, examples, paper
  validation, server validation, vLLM validation, new tests, mock gates, fake
  evidence, synthetic evidence, and dry-run deliverables remain deferred.
- The production transfer body already has daemon-owned intent submission,
  scheduler decisions, execution tickets, worker/backend execution evidence,
  block runtime metadata, cleanup, recovery payloads, and terminal receipts.
- `TurboBusRuntimeSession` is the production entrypoint for runtime startup,
  session/job registration, buffer registration, adapter construction,
  transfer submission, receipt consumption, recovery, cleanup, and close
  evidence.
- Adapter construction, handle submit/wait, model loading, training offload,
  and vLLM KV lifecycle evidence record RuntimeSession entrypoint adapter
  evidence records and consume real `TransferReceipt` objects.
- Offload lifecycle public exports no longer expose raw receipt trace or raw
  handle receipt collection. Public receipt trace helpers require
  `TurboBusRuntimeSession`, validate receipt ownership, record adapter
  evidence, and return RuntimeSession-bound snapshots. Daemon recovery in
  adapter lifecycle evidence is reduced to RuntimeSession-bound scalar
  summaries; daemon queue, ticket, lease, buffer, cleanup, and
  completion-evidence internals stay out of adapter-facing recovery evidence.
- Public offload block snapshots now expose only structural block state.
  Public offload batch snapshots must generate and validate RuntimeSession
  adapter evidence before exposing receipt, ticket, decision, topology, and
  byte-summary fields.
- Public adapter transfer stats reads now return RuntimeSession-bound stats
  snapshots with adapter evidence records and receipt contracts. Raw direct and
  relay counts remain private inputs to lifecycle evidence construction.
- Model loading and training lifecycle range/binding extras now expose only
  structural buffer, tensor, bucket, and range summaries. Receipt, ticket,
  decision, topology, and transfer-state identity must come from
  RuntimeSession adapter evidence records and receipt contracts.
- vLLM connector close, saved-prefix reads, connector event reads, lifecycle
  event caches, and backing summaries remain bound to RuntimeSession evidence.
  Public vLLM summaries expose scalar counts, evidence ids, and close-binding
  flags only. Raw receipt, ticket, decision, topology, runtime entrypoint, and
  receipt-contract identities stay inside the internal validation chain.

## Remaining Risk

- Functional validation, server validation, benchmark execution, paper
  validation, vLLM validation, and multi-GPU execution remain deferred to the
  later validation and evaluation stage.
- Hardware-backed CUDA, vLLM, multi-GPU, server, benchmark, and paper
  validation remain unproven. This is an external validation risk, not a reason
  to add mock gates, fake receipts, synthetic evidence, or dry-run substitutes.
- PCIe load is currently derived from daemon active path state. Hardware
  counter sampling can replace or enrich it later, but must report explicit
  unknown state when unavailable.

## Next Main Target

Converge the production boundary around `TurboBusRuntimeSession` and adapter
lifecycle evidence.

Next inspect remaining adapter construction snapshots and offload public
batch/stats snapshots for any runtime-looking identity, route policy, or
receipt-contract detail that should stay behind `TurboBusRuntimeSession`
adapter evidence records.
