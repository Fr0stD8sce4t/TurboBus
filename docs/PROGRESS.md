# TurboBus Progress

## Current State

- Main target remains the core system body, not adapters, benchmarks, or paper
  work.
- Per-round progress counts only when one full production capability closes.
- The production entry is centered on `TurboBusRuntimeSession`.
- Managed runtime-session-owned daemon and worker socket startup is already the
  base path for the next closures.
- Buffer registration -> execution -> cleanup -> final `TransferReceipt`
  lifetime evidence is now bound through the daemon receipt path, including
  runtime buffer registration snapshots plus worker/direct resource evidence.
- Direct, relay, and mixed terminal receipts now expose a more uniform
  daemon-owned completion contract view instead of leaving failure/cleanup
  evidence split across per-mode shapes.

## Remaining Risk

- Daemon execution still spans several modules and needs a cleaner owned path.
- Relay-only execution still needs one clearer end-to-end closure expressed as
  a single mode-owned path rather than only as part of the shared contract.
- Scheduler/runtime load feedback still needs to consume more of the real
  queued/running/active transfer state as one owned path.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current system implementation.

## Next Main Target

Keep finishing the system body in larger closures. Prefer exactly one of these
per round:

- one complete execution-mode closure;
- one complete runtime-session-owned startup/execution/cleanup closure.
- one complete scheduler/runtime load-feedback closure.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
