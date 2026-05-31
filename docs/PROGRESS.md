# TurboBus Progress

## Current State

Active plan: `docs/NEXT_STEPS.md`.

Current main target: real buffer correctness gate.

## Completed This Round

- Removed local fake/native-readback test hooks that were standing in for real
  CUDA correctness.
- Removed the local e2e verification fixture test file.
- Kept production native CUDA `verify_transfer` code and daemon receipt evidence
  gates intact.

## Validation

- `python -m unittest test.python.unit.test_backend_cuda test.python.unit.test_worker_cuda_executor`
  passed: 21 tests.
- `python -m unittest test.python.integration.test_paper_main_path test.python.integration.test_client_worker_transfer`
  passed: 50 tests, 1 skipped.
- `python -m py_compile test\python\unit\test_backend_cuda.py test\python\unit\test_worker_cuda_executor.py test\python\integration\test_client_worker_transfer.py test\python\integration\test_paper_main_path.py`
  passed.
- `git diff --check`
  passed with local CRLF warnings only.

## Remaining Risk

- Native CUDA extension build was not run locally because `nvcc`, `cmake`, and
  `pybind11` are unavailable in this Windows environment.
- Real GPU H2D/D2H public intent correctness still needs server validation.
- Real vLLM workload execution has not been run.

## Next Main Target

Continue the real buffer correctness gate by running the native CUDA build and
real public intent H2D/D2H correctness checks on a CUDA server.
