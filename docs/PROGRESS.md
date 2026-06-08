# TurboBus Progress

## Current State

- The project is in code-function implementation. Functional validation,
  benchmark runs, examples, paper validation, server validation, new tests, mock
  gates, fake evidence, synthetic evidence, and dry-run deliverables remain
  deferred.
- G1 through G36 are complete.
- G36 multi-tenant isolation strengthening is present: worker status and
  completion evidence now carries daemon-issued owner binding, and daemon
  ticket evidence normalization rejects owner-binding mismatches before storing
  runtime status or completion evidence.
- Auto-advance continues with G37 as the only active target.

## Remaining Risk

- G37 vLLM KV code integration strengthening is not complete: vLLM KV adapter
  paths still need tighter runtime-session-only transfer submission and receipt
  consumption without physical route exposure.
- Functional validation, server validation, benchmark execution, paper
  validation, and multi-GPU execution remain deferred.

## Next Main Target

G37 vLLM KV code integration strengthening.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
