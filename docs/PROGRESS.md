# TurboBus Progress

## Current State

- The project is still in system-body implementation; benchmarks, paper
  validation, and server validation remain deferred.
- `TurboBusRuntimeSession` stays the intended production entry, and
  daemon-issued execution, receipt formation, cleanup, and runtime feedback
  remain on the real production path.
- Worker-issued reservation cleanup now carries daemon-issued `owner_binding`
  back into the daemon cleanup path, so reservation cleanup authorization is
  checked against daemon-issued owner scope for live, staging, and archived
  reservation state instead of relying only on the worker socket peer.

## Remaining Risk

- Registered shared pinned CPU buffers and CUDA IPC GPU buffers still need one
  full lifetime closure across registration, worker binding, cleanup, session
  close, and receipt-visible retention.
- Server, CUDA, benchmark, and adapter validation remain later-stage risks and
  do not block current implementation rounds.

## Next Main Target

Finish one full registered buffer lifetime lifecycle across runtime session,
daemon, worker resource binding, execution cleanup, and receipt retention.

Progress-file rule:

- keep this file short and forward-looking;
- after each completed sub-goal, replace old current-state text with the new
  current state;
- do not accumulate completed implementation history here.
