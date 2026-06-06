# TurboBus Progress

## Current State

Current main target is still to finish the core system body before adapter,
benchmark, or paper-validation work.

Per-round implementation should now be judged by full system capability
closure, not by isolated bug-style hardening. A round is only on target when it
adds one independently describable production capability boundary, such as one
complete execution mode, one complete buffer lifetime path, or one complete
runtime-session startup/execution path.

The production path now centers more clearly on `TurboBusRuntimeSession`.
Runtime session owns session/job/buffer registration, daemon profile bootstrap,
`TransferIntent` submission, daemon-issued execution, and `TransferReceipt`
consumption. Mixed pooled execution is already present in code: direct chunks
run through backend exact-plan execution, relay chunks run through worker
authorization/execution, and terminal daemon completion merges both evidence
paths into one receipt contract.

`TurboBusRuntimeSession` now also owns a managed production socket lifecycle.
It can start a production daemon socket service plus worker socket service,
wait for both sockets to become ready, connect through the same runtime-session
API surface, and shut both owned services down during session close. This turns
runtime-session-to-daemon/worker startup into a production-owned code path
instead of assuming sockets are always provisioned externally.

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

Mixed pooled receipt evidence now preserves more of the worker-side lifecycle.
Deferred worker completion is no longer reduced to verification bytes alone:
the merged mixed receipt path now also carries worker cleanup evidence,
staging-slot evidence, staging release evidence, and daemon running-update
context into the final relay-side completion evidence.

## Remaining Risk

- The daemon execution lifecycle still spans several modules, and admission,
  ticket reissue, worker authorization, terminal completion, and cleanup can
  still be made more explicit as one owned path.
- Mixed pooled execution exists in code, but the system still needs a cleaner
  single contract across Python plan handling, backend direct execution, worker
  relay execution, and merged receipt evidence.
- Runtime session, daemon, and executor still duplicate some receipt parsing,
  exact-plan assumptions, and execution-path decisions, which leaves room for
  boundary drift even though the major production entry split has been reduced.
- Shared pinned CPU buffers and CUDA IPC GPU buffers still need a fuller
  end-to-end lifetime closure from registration/open through execution and
  release evidence in final receipts.
- Native CUDA execution, worker socket execution, shared pinned CPU buffers,
  and CUDA IPC buffer lifetime are wired into the code path but remain pending
  later end-to-end server/CUDA validation after the system path is complete.
- Benchmarks, examples, adapters, and older tests still reflect pre-runtime-
  session assumptions and remain intentionally deferred.

## Next Main Target

Continue finishing the system body in larger closures: prefer one full direct /
relay / mixed execution closure or one full buffer-lifecycle closure per round
on top of the now managed runtime-session-to-daemon/worker production path,
rather than incremental bug-style tightening alone.
