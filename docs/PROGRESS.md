# TurboBus Progress

## Current State

Current main target is still to finish the core system body before adapter,
benchmark, or paper-validation work.

The production path now centers more clearly on `TurboBusRuntimeSession`.
Runtime session owns session/job/buffer registration, daemon profile bootstrap,
`TransferIntent` submission, daemon-issued execution, and `TransferReceipt`
consumption. Mixed pooled execution is already present in code: direct chunks
run through backend exact-plan execution, relay chunks run through worker
authorization/execution, and terminal daemon completion merges both evidence
paths into one receipt contract.

`TurboBusClient` is no longer a production transfer submission path. It remains
only as a terminal-receipt compatibility boundary, which removes one remaining
public path that could make benchmark-style code look like a valid production
entry.

Daemon receipt waiting is now aligned with that boundary: `wait_transfer_receipt`
without a timeout blocks until terminal state instead of returning the current
non-terminal snapshot. Executor-side non-direct plans without relay lease
tokens no longer return submit-stage payload receipts; they are turned into
explicit daemon failure and then consumed through the same terminal receipt
path.

## Remaining Risk

- The daemon execution lifecycle still spans several modules, and admission,
  ticket reissue, worker authorization, terminal completion, and cleanup can
  still be made more explicit as one owned path.
- Mixed pooled execution exists in code, but the system still needs a cleaner
  single contract across Python plan handling, backend direct execution, worker
  relay execution, and merged receipt evidence.
- Runtime session, daemon, and executor still duplicate some receipt parsing and
  exact-plan assumptions, which leaves room for boundary drift even though the
  major production entry split has been reduced.
- Native CUDA execution, worker socket execution, shared pinned CPU buffers,
  and CUDA IPC buffer lifetime are wired into the code path but remain pending
  later end-to-end server/CUDA validation after the system path is complete.
- Benchmarks, examples, adapters, and older tests still reflect pre-runtime-
  session assumptions and remain intentionally deferred.

## Next Main Target

Continue tightening the runtime-session-first production path, then narrow the
daemon-to-worker lifecycle so direct, relay, and mixed pooled execution behave
as one explicit daemon-owned contract.
