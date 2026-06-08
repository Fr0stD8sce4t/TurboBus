# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G19 are complete.
- G19 trusted topology closure is present: CUDA/NVML topology discovery now
  records PCIe and fabric bandwidth sources, relay eligibility rejects relay
  paths without trusted PCIe/fabric bandwidth, and daemon relay discovery exposes
  trusted topology counts for scheduler-facing state.
- Auto-advance continues with G20 as the only active target.

## Remaining Risk

- G20 profile measurement closure is not complete: native direct, relay, and
  fabric timing still needs to be bound to daemon-ingested profile records and
  the trusted topology snapshot from G19.
- End-to-end CUDA, vLLM, multi-GPU, server, benchmark, and paper-validation
  evidence remains deferred. Future validation work must use real executed
  daemon/worker/backend evidence, not fake receipts, synthetic evidence, JSON
  artifacts, or dry-run output.

## Next Main Target

G20 profile measurement closure.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
