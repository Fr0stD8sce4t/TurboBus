# TurboBus Progress

## Current State

Current main target: system implementation before experiments.

The production path is being kept on the daemon-first route:
`TransferIntent` submission, daemon scheduling, daemon-issued
`ExecutionTicket`, worker/backend completion, and `TransferReceipt`
consumption. `TurboBusRuntimeSession` remains the public runtime entry for
session, job, buffer, profile bootstrap, intent submission, worker execution,
receipt wait, and cleanup wiring. The old `client_transfer.py`,
`turbobus.control`, route-shaped transfer request, manual relay reservation,
manual session relay selection, worker shortcut, transfer-mode, broad daemon
client, buffer self-registration, and pure re-export compatibility entry
points remain removed.

Server validation, benchmark work, paper validation, experiments, and new test
code remain deferred until the full system implementation pass is complete.
Current progress should continue through code reading, implementation,
refactoring, and existing minimal local checks without adding server test
commands or making server validation a current entry point.

## Completed This Round

- Audited runtime configuration ownership during the daemon-first closure
  pass.
- Moved `RuntimeOptions` from `runtime_engine.py` to
  `runtime_options.py`, the module that now owns runtime configuration.
- Updated production imports in runtime session, intent executor, direct
  fallback, profile bootstrap, and worker CUDA execution to import from
  `turbobus.runtime_options`.
- Deleted the old `runtime_engine.py` file instead of keeping a compatibility
  export layer.
- Exported `RuntimeOptions` from the top-level `turbobus` package as part of
  the system runtime API.

## Validation

- `python -m py_compile turbobus\runtime_options.py
  turbobus\runtime_session.py turbobus\intent_executor.py
  turbobus\direct_fallback.py turbobus\profile.py
  turbobus\worker\cuda_executor.py turbobus\__init__.py` passed.
- `rg -n "runtime_engine" turbobus` found no production references to the
  removed module.
- `rg --files turbobus | rg "runtime_engine\.py|runtime_options\.py"` found
  only `turbobus\runtime_options.py`.
- `git diff --check` passed with only Git line-ending conversion warnings.

## Remaining Risk

- CUDA/native execution, vLLM runtime behavior, relay/pooled execution, and
  server-only behavior remain unverified until the full system implementation
  pass is complete.
- Existing tests still contain old production-path assumptions such as removed
  transfer request, manual reservation, worker shortcut, broad daemon client,
  manual daemon planning, adapter policy hints, old `runtime_engine` imports,
  and compatibility entry points. Current-stage constraints defer test
  migration until the system implementation pass is complete.
- A final system-code closure audit still needs to continue looking for
  compatibility drift, application-side physical route controls, or public
  bypasses around daemon-issued execution tickets.

## Next Main Target

Continue the system-code closure audit for the daemon-first path while keeping
tests, benchmarks, paper validation, server validation, and experiments
deferred.
