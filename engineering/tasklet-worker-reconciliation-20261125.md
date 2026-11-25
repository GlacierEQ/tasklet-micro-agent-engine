# Tasklet worker reconciliation — 2026-11-25

Continuation of `engineering/tasklet-worker-ownership-20261124`, base
`9a289f517624ef58459af289010227fda39c703e`. Exact source and existing
mixed-admission tests reconstructed from GitHub readback in an isolated local
Python 3.13.5 environment; Git blob hashes matched
`599997948dd771b0b3919baf8577f94bfcd9af8d` and
`b84015d83678c1c0b8a21e00f6b5d59266e443c0` before modification.
Desktop Commander was not retried; no new CI spending was initiated.

## Evidence and behavior

- Original mixed-admission tests: 2 passed.
- Additional real-thread and asyncio tests reproduced three failures: successful
  late timeout result lost, successful late cancellation result lost, and retry
  after a late exception with an already performed side effect.
- Preserve the original timeout/cancellation status; record the late handler
  outcome separately. A successful late result replays under the existing
  idempotency key; an ambiguous late failure rejects blind redispatch.
- Preserve reconciled records against tasklet-ID reuse. Retain capacity until
  worker exit is established; cancelled asyncio wrappers alone are not proof.
- Local focused verification: `PYTHONPATH=src python -m pytest -q tests`
  in the isolated two-test-file reconstruction: 8 passed, including 100 mixed
  asynchronous dispatches with a registry-held slot. Compileall passed.

## Remaining and next cursor

This is process-local reconciliation, not durable cross-session deduplication,
external provider readback, or proof of external side effects. A cancelled worker
wrapper can remain unresolved conservatively. No full repository suite, Ruff,
Python matrix, merge, deployment, or unattended APEX/Spiral integration was
verified here. Continue with the full repository integrity and test suite,
then integrate Tasklet receipt reconciliation into the existing APEX/Spiral
continuation path using a real interrupted, recovered mission. Do not rediscover
or retry exhausted Desktop Commander access; use the local hash-verified route
or an already authorized execution environment.

## Functional execution proof

Local Python 3.13.5 executed the real Tasklet runtime against two actual
source files. A synchronous handler generated a SHA-256 inventory on disk,
with an independently counted execution marker. The original caller timed
out, the worker finished, and a repeated idempotency key recovered the
completed inventory. Independent file readback matched the returned result;
exactly one underlying operation executed and active occupancy returned to
zero. This proves local late-result recovery for this workload, not external
provider reconciliation or cross-session durability.
