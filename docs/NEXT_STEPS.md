# TurboBus Next Steps

This is the only active per-round implementation plan. Replace state instead of
appending history.

## Current Main Target

G27 model loading real integration closure.

Model weight loading must register real runtime-session CPU/GPU buffers, map
manifest tensors to byte ranges, submit only `TransferIntent`, consume only
`TransferReceipt`, and record a stable model-loading lifecycle that binds
manifest tensors, bucket ranges, runtime buffers, receipt ids, ticket ids, byte
counts, and path split evidence without exposing direct, relay, pool, target
GPU, or relay GPU choice to the adapter.

## Current Code Work

- `turbobus/adapters/model_loading.py`: manifest tensor mapping, bucket/range
  registration, model-load lifecycle evidence, and receipt aggregation.
- `turbobus/model_manifest.py`: model tensor descriptors and manifest span
  accounting.
- `turbobus/offload/store.py`: bucket batch conversion into daemon-submitted
  `TransferIntent`.
- `turbobus/offload/lifecycle.py`: adapter lifecycle evidence derived from real
  `TransferReceipt` objects.
- `turbobus/runtime_session.py`: model-loading adapter construction through the
  single production runtime-session entry.

Round rules:

- Start each round with `git status`, then read `AGENTS.md`,
  `docs/TURBOBUS_ROADMAP.md`, `docs/NEXT_STEPS.md`, and `docs/PROGRESS.md`.
- Choose the single round target from this file first and `docs/PROGRESS.md`
  second.
- Finish one full system closure per round, not local bug-style fixes.
- Treat a round as complete only when the system gains one independently
  describable production capability on the current target path.
- Do not advance benchmark, example, paper-validation, server-validation, new
  test, dry-run, fake receipt, synthetic evidence, or replacement verification
  entry work during the current system-body pass.
- Update this file and `docs/PROGRESS.md` after each completed closure.
- Keep only active and next work here. Do not append completed history.

## Next Entry

After G27 is complete, continue automatically to G28 as the only current target.

## Auto-Advance Policy

Auto-advance is active for the paper-reproduction system queue.

Remaining auto-advance target queue:

- G27 model loading real integration closure.
- G28 training offload real integration closure.
- G29 unified reproduction evidence model.
- G30 real-execution validation and evaluation entry recovery.
