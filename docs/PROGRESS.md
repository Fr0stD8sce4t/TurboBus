# TurboBus Progress

## Current State

Current main target: real buffer correctness gate.

The daemon, worker, backend, public client, adapter support, benchmarks, and
paper-validation paths reject completed intent transfers that lack execution
and verified-byte evidence. Complete `TransferReceipt` construction now also
requires worker/backend source, executed bytes, verified bytes, and
content-match evidence. Native direct H2D and D2H CUDA readback have been
validated on the server. Public intent backend and relay/pool CUDA validation
still need server runs.

## Completed This Round

- Enforced complete-receipt evidence in `TransferReceipt.__post_init__`.
- Updated benchmark, public example, offload-store, vLLM, and schema fixtures
  so valid complete receipts carry worker/backend completion evidence.
- Kept benchmark and paper-validation paths rejecting intent-only completion.

## Validation

- `python -m unittest test.python.e2e.test_model_loading_benchmark test.python.e2e.test_training_offload_benchmark test.python.e2e.test_benchmark_daemon_support test.python.e2e.test_paper_validation`
  passed.
- `python -m unittest test.python.unit.test_contract_schema test.python.unit.test_schema test.python.unit.test_public_client_api test.python.unit.test_offload_store test.python.unit.test_vllm_kv_connector_main_path test.python.e2e.test_public_intent_example test.python.integration.test_paper_main_path test.python.integration.test_daemon_state test.python.integration.test_daemon_socket`
  passed.
- `git diff --check` passed.

## Remaining Risk

- Public intent backend H2D/D2H correctness still needs real CUDA server
  validation.
- Worker relay and pooled correctness still need real CUDA validation on a P2P
  capable GPU pair.

## Next Main Target

Continue the real buffer correctness gate until public intent backend,
worker-relay, and pooled paths all prove executed and verified bytes on real
CUDA buffers.
