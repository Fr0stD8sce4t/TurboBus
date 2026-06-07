# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, paper
  validation, and server validation remain deferred.
- `TurboBusRuntimeSession` stays the intended production entry, and
  daemon-issued execution, receipt formation, cleanup, and runtime feedback
  remain on the real production path.
- `TurboBusRuntimeSession.open_production_socket()` now attaches to a live
  daemon, probes for a worker socket, and starts a runtime-owned worker service
  when the worker is missing, so runtime session authority no longer depends on
  the caller pre-assembling the relay-capable worker path by hand.

## Remaining Risk

- Mixed direct + relay daemon-issued execution still needs one full lifecycle
  closure across execution, failure, cleanup, receipt formation, and runtime
  feedback.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish one full daemon-issued mixed direct + relay execution lifecycle across
`TransferIntent -> SchedulingDecision -> ExecutionTicket -> worker/backend
execution -> TransferReceipt`.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
