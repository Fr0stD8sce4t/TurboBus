# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G22 are complete.
- G22 mixed pooled execution hardening is present: worker/backend execution now
  exports native path-level direct and relay stats, relay-device stats, and a
  unified path-level completion contract through daemon normalization and
  `TransferReceipt` metadata.
- Auto-advance continues with G23 as the only active target.

## Remaining Risk

- G23 cross-job admission and fairness closure is not complete: daemon admission
  and scheduler decisions still need a tighter shared model of live queued,
  running, active, lease, relay, and job pressure so pooled PCIe sharing is fair
  across jobs and bound to job/session ownership.
- End-to-end CUDA, vLLM, multi-GPU, server, benchmark, and paper-validation
  evidence remains deferred. Future validation work must use real executed
  daemon/worker/backend evidence, not fake receipts, synthetic evidence, JSON
  artifacts, or dry-run output.

## Next Main Target

G23 cross-job admission and fairness closure.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
