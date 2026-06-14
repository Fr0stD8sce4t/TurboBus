# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead
of appending history.

## Goal Mode Rule

Each round must derive one current main target from this file first, then
`docs/PROGRESS.md`, then `AGENTS.md`, then `docs/TURBOBUS_ROADMAP.md`.

Each round must close one complete system capability loop. Small fixes, helper
moves, field renames, import cleanup, and documentation updates do not count
unless they finish the same capability loop.

Do not start benchmark, example, paper validation, server validation, vLLM
validation, new tests, substitute validation entrypoints, fake receipt paths,
synthetic evidence, or dry-run deliverables in the current stage.

## Current Main Target

Converge the production boundary around `TurboBusRuntimeSession` and adapter
lifecycle evidence.

Keep the single production entrypoint, remove duplicated runtime-looking paths,
and keep adapter validation bound to `TurboBusRuntimeSession` adapter evidence
records, snapshots, and real receipts.

## Current Code Work

- `turbobus/runtime_session.py`: remain the production entrypoint for startup,
  registration, transfer submission, receipt consumption, adapter construction,
  recovery, close, and session-level evidence.
- `turbobus/runtime/session_records.py`: keep entrypoint snapshots aligned with
  adapter context, intent, receipt, adapter evidence, recovery, and cleanup
  records. RuntimeSession close records must identify
  `TurboBusRuntimeSession.close` and keep route policy hidden.
- `turbobus/offload/lifecycle.py`: expose only RuntimeSession-bound adapter
  lifecycle and receipt trace helpers. Raw receipt trace extraction and handle
  receipt collection stay private. Daemon recovery in adapter lifecycle
  evidence is a RuntimeSession-bound scalar summary only; daemon queue,
  ticket, lease, buffer, cleanup, and completion-evidence internals stay out of
  adapter-facing lifecycle evidence. Package-level offload exports expose only
  the adapter lifecycle evidence builder; raw RuntimeSession receipt trace
  helpers stay internal to direct module users.
- `turbobus/offload/handles.py`: keep low-level receipt-bearing handle
  submit/wait bindings internal to offload. Public transfer handles expose only
  RuntimeSession-bound wait, stats, evidence id, and scalar snapshots; raw
  client, intent, receipt, and runtime entrypoint state stay private.
- `turbobus/offload/store.py`, `turbobus/offload/handles.py`, and
  `turbobus/offload/blocks.py`: public batch and transfer stats snapshots use
  RuntimeSession-bound adapter evidence internally, but expose only scalar
  counts, adapter evidence ids, receipt states, and byte summaries. Public
  batches keep raw handles out of dataclass fields, repr, compare, and
  adapter-facing handle lists; internal lifecycle evidence still consumes
  receipt-bearing handles. Public block info objects, block snapshots, and
  wildcard exports remain structural and do not expose runtime-looking receipt
  or transfer identity fields.
- `turbobus/runtime/evidence.py`: reject missing RuntimeSession records, fake
  evidence, exposed route policy, lifecycle identity drift, public batch
  snapshots, public transfer stats snapshots, and lifecycle range/binding
  extras or daemon recovery details that are not bound to RuntimeSession
  adapter evidence. Range, request, and buffer binding extras must not carry
  nested runtime entrypoint, adapter evidence record, receipt contract, raw
  receipt, intent, ticket, decision, topology, transfer, daemon recovery, close
  entrypoint, or route-policy fields. Public snapshot validators accept
  RuntimeSession-bound scalar summaries only and reject old public
  entrypoint/adapter-record helper paths.
- `turbobus/adapters/`: consume RuntimeSession-bound evidence only. Adapters
  must not create route, plan, relay, pool, target-GPU policy, or their own
  receipt/ticket/decision summaries inside range/binding extras. vLLM public
  events, saved-prefix snapshots, backing lifecycle summaries, vLLM KV
  request/buffer binding extras, and transfer stats aggregations expose only
  RuntimeSession-bound scalar counts, evidence ids, receipt states, byte
  summaries, structure summaries, and close-binding flags; raw receipt,
  ticket, decision, topology, runtime entrypoint, adapter evidence record, and
  receipt-contract identities stay internal to validation. vLLM prefix store
  cleanup/recovery and backing lifecycle summaries consume internal
  RuntimeSession evidence, but public/cache-crossing summaries expose only
  adapter evidence ids, counts, sources, and recorded flags. Connector event
  cache reads recursively reject nested RuntimeSession identity and raw receipt,
  ticket, decision, topology, transfer, close-entrypoint, and route-policy
  fields before returning public records. Adapter package and vLLM connector
  wildcard exports expose connector classes and safe summary readers only;
  saved-prefix dataclasses, connector metadata, request metadata, and prefix
  store mutation records stay behind module-internal direct imports.

## Next Entry

Continue the same production-boundary refactor in the current code path. Next
inspect remaining offload store/block public attributes and adapter-facing
methods for raw handles, RuntimeSession entrypoint records, adapter evidence
records, receipt contracts, route policy, or raw transfer identity instead of
scalar RuntimeSession-bound summaries.

## Remaining Risk

- Hardware-backed CUDA, vLLM, multi-GPU, server, benchmark, and paper
  validation remain deferred.
- PCIe load is still derived from daemon active path state until hardware
  counter sampling is added later.
- Goal-mode rounds must still inspect `git status`, avoid unrelated dirty
  files, and stage only the active target.

## Completion Standard

The current round is complete only when the code closes one production-boundary
capability loop, `docs/NEXT_STEPS.md` and `docs/PROGRESS.md` agree, no
benchmark or validation substitute was added, minimal relevant checks passed,
and the staged diff contains only this round's files.
