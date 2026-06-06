# TurboBus Progress

## Current State

- Main target remains the core system body, not adapters, benchmarks, or paper
  work.
- Per-round progress counts only when one full production capability closes.
- The production entry is centered on `TurboBusRuntimeSession`.
- Managed runtime-session-owned daemon and worker socket startup is already the
  base path for the next closures.
- Mixed pooled receipt evidence already preserves more worker-side lifecycle
  context, so the remaining work should expand system closure rather than keep
  polishing that local path.

## Remaining Risk

- Daemon execution still spans several modules and needs a cleaner owned path.
- Direct, relay, and mixed execution still need a tighter single receipt and
  cleanup contract.
- Shared pinned CPU and CUDA IPC GPU buffers still need end-to-end lifetime
  closure in final transfer receipts.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current system implementation.

## Next Main Target

Keep finishing the system body in larger closures. Prefer exactly one of these
per round:

- one complete execution-mode closure;
- one complete buffer-lifetime closure;
- one complete runtime-session-owned startup/execution/cleanup closure.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
