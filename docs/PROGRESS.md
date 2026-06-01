# TurboBus Progress

## Current State

Active plan: `docs/NEXT_STEPS.md`.

Current main target: real buffer correctness gate.

## Completed This Round

- Recorded server evidence that the native CUDA extension builds and exports
  `verify_transfer`.
- Recorded server evidence that native direct H2D and D2H transfers verify
  1,048,576 matching bytes through `native_cuda_readback`.
- Confirmed GPU 0/1 cannot be used for relay validation because CUDA P2P is not
  available between them.
- Confirmed GPU 5/6 are the machine's NVLink pair, but relay validation must
  wait until they are idle.

## Validation

- Server: `python -m pip install -e .` built and installed `turbobus._turbobus`.
- Server: native module import reported `has verify_transfer: True`.
- Server: native direct H2D verification returned `verified_bytes: 1048576`,
  `content_match: True`, `verification_source: native_cuda_readback`.
- Server: native direct D2H verification returned `verified_bytes: 1048576`,
  `content_match: True`, `verification_source: native_cuda_readback`.
- Server: `CUDA_VISIBLE_DEVICES=0,1` reported CUDA P2P unavailable between the
  two RTX 4090 devices.
- Server: `nvidia-smi topo -m` reported GPU 5 and GPU 6 connected by NV4.

## Remaining Risk

- Real GPU H2D/D2H public intent correctness still needs server validation.
- Real worker relay/pool correctness still needs GPU 5/6 to be idle.
- Real vLLM workload execution has not been run.

## Next Main Target

Continue the real buffer correctness gate by running public intent backend
H2D/D2H correctness checks on GPU 0, then worker relay/pool checks on GPU 5/6
when that NVLink pair is idle.
