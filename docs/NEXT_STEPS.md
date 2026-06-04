# TurboBus Next Steps

This is the only active forward plan. Keep it short and replace completed
state instead of appending history.

## Current Main Target

System implementation before experiments.

Current code target: complete the socket-backed production runtime session
entry. Production adapters must connect to both daemon and worker socket
services so worker/backend execution remains on daemon-issued plans.

## Exit Criteria

- `TurboBusRuntimeSession` exposes a production socket opener that requires
  daemon and worker socket paths.
- vLLM KV connector uses the production socket opener and cannot silently fall
  back to an in-process worker client.
- vLLM TurboBus config requires non-empty daemon and worker socket paths.
- No test, experiment, benchmark, paper-validation, or server-validation code
  is added during this system implementation pass.

## Current Code Work

Finish the production runtime session startup path from socket configuration to
daemon role clients and worker socket client.

## Next Entry

After this target is complete, continue with one concrete implementation
boundary: adapter submission/receipt consumption through
`TurboBusRuntimeSession` or profile bootstrap closure.
