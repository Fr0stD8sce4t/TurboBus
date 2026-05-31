# TurboBus Progress

## Current State

Active plan: `docs/NEXT_STEPS.md`.

Current main target: real buffer correctness gate.

## Completed This Round

- Added a public `TransferIntent` CUDA correctness verifier in
  `turbobus.verification`.
- The verifier can require backend or worker execution through its daemon test
  fixture while keeping physical route selection out of `TransferIntent`.
- The verifier checks destination contents and requires receipt metadata to
  report executed, verified, matching bytes.

## Validation

- `python -m unittest test.python.e2e.test_verification`
  passed: 12 tests.
- `python -m unittest test.python.integration.test_paper_main_path test.python.integration.test_client_worker_transfer`
  passed: 50 tests, 1 skipped.
- `python -m py_compile turbobus\verification.py test\python\e2e\test_verification.py`
  passed.
- `git diff --check`
  passed with local CRLF warnings only.

## Remaining Risk

- Native CUDA extension build was not run locally because `nvcc`, `cmake`, and
  `pybind11` are unavailable in this Windows environment.
- Real GPU H2D/D2H public intent correctness for backend and worker execution
  still needs server validation.
- Real vLLM workload execution has not been run.

## Next Main Target

Continue the real buffer correctness gate by running the native CUDA build and
public intent backend/worker H2D/D2H correctness checks on a CUDA server.
