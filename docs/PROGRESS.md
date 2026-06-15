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
  Package-level offload exports now expose only the adapter lifecycle evidence
  builder; raw RuntimeSession receipt trace helpers stay internal to direct
  module users.
- Public offload block info objects, block snapshots, and block wildcard
  exports now expose only structural block state. Raw transfer identity and
  receipt/ticket/decision/topology/job/session fields stay out of public block
  objects. Public offload transfer handles now expose only RuntimeSession-bound
  wait, stats, evidence id, and scalar snapshots. Receipt-bearing handles stay
  internal to offload lifecycle evidence. Public offload batches and block
  views keep raw handles out of public fields, repr, compare, and
  adapter-facing handle lists. Public offload batch snapshots, handle stats,
  store transfer stats, and vLLM transfer stats aggregations generate and
  validate RuntimeSession adapter evidence internally, then expose only scalar
  counts, adapter evidence ids, receipt states, and byte summaries.
- Raw RuntimeSession entrypoint records, receipt contracts, receipt ids,
  intent ids, ticket ids, decision ids, topology ids, and route policy stay
  behind RuntimeSession adapter evidence validation. Raw direct and relay
  counts remain private inputs to lifecycle evidence construction.
- vLLM KV lifecycle request bindings and runtime buffer binding extras now
  expose only structural summaries, adapter evidence ids, and recorded flags.
  RuntimeSession entrypoint records, adapter evidence records, receipt
  contracts, raw receipt/intent ids, and nested route policy are rejected from
  adapter lifecycle extras.
- Model loading and training lifecycle range/binding extras now expose only
  structural buffer, tensor, bucket, and range summaries. Receipt, ticket,
  decision, topology, and transfer-state identity must come from
  RuntimeSession adapter evidence records and receipt contracts.
- vLLM connector close, saved-prefix reads, connector event reads, lifecycle
  event caches, and backing summaries remain bound to RuntimeSession evidence.
  Public vLLM summaries expose scalar counts, evidence ids, and close-binding
  flags only. Raw receipt, ticket, decision, topology, runtime entrypoint, and
  receipt-contract identities stay inside the internal validation chain. vLLM
  saved-prefix cleanup/recovery and backing lifecycle summaries now keep
  RuntimeSession entrypoint records, adapter evidence records, receipt
  contracts, raw daemon recovery details, and route policy inside internal
  validation while exposing only adapter evidence ids, counts, sources, and
  recorded flags.
- Connector public event/cache reads now recursively reject nested RuntimeSession
  entrypoints, adapter evidence records, receipt contracts, raw receipt,
  ticket, decision, topology, transfer, close-entrypoint, and route-policy
  fields. Runtime evidence public snapshot validators accept only
  RuntimeSession-bound scalar summaries and reject old public
  entrypoint/adapter-record helper paths.
- Adapter package and vLLM connector wildcard exports now expose connector
  classes and safe summary readers only. Saved-prefix dataclasses, connector
  metadata, request metadata, and prefix store mutation records stay behind
  module-internal direct imports.

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

Next inspect remaining offload store/block public methods that still return
internal block handles or runtime-looking transfer state instead of scalar
RuntimeSession-bound summaries.
