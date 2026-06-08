# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, examples,
  paper validation, server validation, new tests, fake evidence, synthetic
  evidence, and dry-run deliverables remain deferred.
- G1 through G14 are complete.
- G14 vLLM KV lifecycle closure is present: vLLM KV save/restore stores and
  emits lifecycle evidence derived from validated daemon `TransferReceipt`
  objects, including receipt ids, ticket ids, decision ids, topology ids,
  transfer ids, completion sources, direct/relay bytes, and prefix ownership
  context.
- Auto-advance continues with G15 as the only active target.

## Remaining Risk

- G15 prefix store productionization is not complete: prefix storage still needs
  a closed production lifecycle around ownership, capacity, eviction, cleanup,
  and lifecycle evidence reuse.
- Model loading takeover, training-state offload, and final auditable receipt
  closure remain later-stage work.
- Alternative verification paths, fake receipts, synthetic evidence, benchmark
  work, and dry-run deliverables remain out of scope for the current
  system-body pass.

## Next Main Target

G15 prefix store productionization.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
