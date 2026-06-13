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
  handle receipt collection. Public receipt trace helpers now require
  `TurboBusRuntimeSession`, validate receipt ownership, record adapter evidence,
  and return RuntimeSession-bound snapshots.
- vLLM connector close now consumes the real `TurboBusRuntimeSession.close`
  response before emitting free backing cleanup summaries. Free backing cleanup
  evidence and public close events require a RuntimeSession close entrypoint
  record and continue to reject route policy exposure.

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

Next inspect remaining adapter-facing recovery and lifecycle summary reads in
`turbobus/adapters/`, `turbobus/offload/`, and `turbobus/runtime/`. Tighten any
path that can emit runtime-looking evidence without consuming
`TurboBusRuntimeSession` entrypoint adapter evidence records or close
entrypoint records.
