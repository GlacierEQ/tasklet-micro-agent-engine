# Tasklet Micro Agent Engine

> **Atomic work should not require a heavyweight multi-turn agent lifecycle. Tasklet makes precise execution bounded, idempotent, deadline-aware, inspectable, fast, and composable.**

*Recruiter signal: a focused high-quality execution primitive with production-shaped controls—not another orchestration diagram and not a philosophy of making systems small.*

## High-Performance Atomic Execution Boundary

Tasklet is a dependency-light Python runtime for **atomic local work**. Its specialization is intentionally narrow at the execution boundary so larger agent, workflow, swarm, connector, and repository systems can compose many trustworthy operations without inheriting unnecessary coordination state for every known task.

**Atomicity is the capability. Smallness is not the objective.**

The runtime implements:

- deterministic priority registration;
- bounded concurrent handler execution;
- synchronous and asynchronous handlers;
- per-task deadlines with terminal timeout receipts;
- payload-size admission limits;
- idempotency keys with completed-result replay;
- fail-closed concurrent duplicate handling;
- explicit `COMPLETED`, `FAILED`, `TIMED_OUT`, and `CANCELLED` outcomes;
- suspend/resume for the registry scheduler;
- repository-local SHA-256 health evidence;
- positive-count test receipts;
- a repeatable local task-registration benchmark.

The public convenience surface is:

```python
from tasklet_micro_agent_engine import run_tasklet

receipt = await run_tasklet(
    handler,
    {"document_id": "123"},
    idempotency_key="extract:123",
    timeout_s=2.0,
)
```

`run_tasklet()` is protocol-neutral. It is suitable for wrapping behind MCP, workflow, swarm, or another tool protocol, but this repository does not claim an external provider registration unless an adapter and provider receipt prove it.

### Why this matters

Large agent frameworks are valuable when work needs planning, memory, dialogue, delegation, model/tool selection, or cross-system reasoning. When the task has already been selected and its interface is known, a dedicated execution primitive can provide stronger determinism, lower coordination overhead, clearer failure semantics, and easier composition.

```text
validated payload
      ↓
idempotency / admission
      ↓
bounded semaphore
      ↓
sync or async handler
      ↓
deadline + failure boundary
      ↓
terminal execution receipt
      ↓
composition into the larger system
```

The architectural claim is concrete: **known atomic work can retain production-shaped execution controls while remaining cheap enough to fan out and compose into much larger systems.**

## Engineering Boundary

Tasklet's bounds exist for correctness and operational control:

- concurrency bounds prevent local resource collapse;
- payload limits protect memory and interface assumptions;
- deadlines contain stalled work;
- idempotency contains duplicate execution;
- terminal states make recovery explicit.

Those bounds must never be generalized into a system-wide rule to choose the smallest task, least capable architecture, smallest useful artifact, or minimum viable product.

The parent system should optimize for **maximum coherent capability**. Tasklet contributes a trustworthy atomic execution floor to that larger ambition.

## Core Surfaces

| Surface | Purpose |
|---|---|
| `src/tasklet_micro_agent_engine.py` | Scheduler, execution runtime, idempotency, deadlines, terminal receipts |
| `tests/test_tasklet.py` | Scheduler and async execution behavior |
| `tests/test_sidecar.py` | Repository-local health truth boundary |
| `mastermind_sidecar.py` | SHA-256 evidence for required repository surfaces |
| `scripts/verify_junit.py` | Positive-count JUnit verification receipt |
| `scripts/benchmark_spawn.py` | Disclosed-environment registration-latency receipt |
| `scripts/verify_integrity.py` | Repository-wide tracked-file SHA-256 gate |
| `.github/workflows/ci.yml` | Exact-head Python 3.11–3.13 verification, benchmark, and integrity gates |
| `pyproject.toml` | Installable package and lint/test contract |

### Execution semantics

`TaskletMicroAgentEngine.run_tasklet()` accepts either a synchronous or asynchronous callable.

- Async handlers are awaited directly.
- Sync handlers run through `asyncio.to_thread()` so they do not block the event loop.
- A semaphore bounds actual concurrent handler execution.
- `asyncio.wait_for()` enforces configured deadlines.
- User exceptions become explicit `FAILED` receipts rather than fabricated success.
- Cancellation records `CANCELLED` and propagates `CancelledError` to preserve cooperative cancellation semantics.

### Idempotency

When an `idempotency_key` is supplied:

- a completed prior execution returns the original result with `replayed: true`;
- a duplicate while the first execution remains in flight is rejected as `idempotency_in_flight`;
- failed terminal work may be retried by a later execution rather than becoming permanently poisoned.

This is deliberately process-local idempotency. Durable cross-process exactly-once semantics require an external durable store and reconciliation protocol and are not claimed here without implementation/proof.

### Admission limits

The runtime fails closed for:

- non-mapping payloads;
- non-finite/non-serializable JSON payload values;
- payloads above `max_payload_bytes`;
- invalid or excessive deadlines;
- duplicate active tasklet IDs;
- scheduler registrations above the configured active-task limit.

These are runtime safety properties, not capability-minimization policy.

## Native Verification

The exact-head CI contract is:

```bash
python -m pip install -e ".[dev]"
python -m ruff check src tests scripts mastermind_sidecar.py
python -m compileall -q src tests scripts mastermind_sidecar.py
python -m pytest --junitxml=artifacts/junit.xml
python scripts/verify_junit.py artifacts/junit.xml --output artifacts/test-receipt.json
python mastermind_sidecar.py
python scripts/benchmark_spawn.py --iterations 5000 --warmup 500 --require-sub-ms-p95
python scripts/verify_integrity.py --check
```

GitHub Actions checks out the literal pull-request head SHA, proves checkout identity, and repeats the correctness suite across Python 3.11, 3.12, and 3.13. The benchmark runs separately on a disclosed GitHub-hosted Ubuntu runner. The integrity gate hashes every tracked regular file except the manifest itself and rejects drift.

### Measured proof

An exact-head provider run on commit `b270b0d01fbf33e52e25affa78b61cb4d197e620` established the recorded evidence baseline:

| Evidence | Result |
|---|---:|
| Python versions | 3.11 / 3.12 / 3.13 |
| Tests per interpreter | 16 |
| Total matrix executions | 48 |
| Failures / errors / skips | 0 / 0 / 0 |
| Repository-local health | healthy |
| Integrity manifest equality | PASS |
| Benchmark iterations | 5,000 |
| Registration p50 | 0.200066 ms |
| Registration p95 | **0.346544 ms** |
| Registration p99 | 0.364911 ms |
| Benchmark runner | Linux x86_64, Python 3.13.14, Azure-hosted GitHub runner |

The current reviewed head must reproduce the relevant gate before those results are promoted as current-head evidence.

### Performance claim boundary

The benchmark measures local registration latency only. Handler execution, network transport, provider latency, tool latency, and end-to-end agent latency are excluded.

The CI gate requires measured p95 registration latency below 1 millisecond on the disclosed runner. This is a reproducible benchmark within that scope, not a universal hardware guarantee.

## Machine Contract

```json
{
  "schema": "glaciereq.tasklet.runtime.v1",
  "module": "tasklet_micro_agent_engine",
  "engine": "TaskletMicroAgentEngine",
  "execution_api": "run_tasklet",
  "python": ">=3.11",
  "authority": "local_process_only",
  "durable_exactly_once": false,
  "external_provider_registration": false,
  "mcp_server_included": false,
  "terminal_states": ["COMPLETED", "FAILED", "TIMED_OUT", "CANCELLED"]
}
```

### Evidence boundary

This repository can prove only the behavior exercised by its current source and current/relevant receipts. It does not by itself prove distributed exactly-once execution, provider/MCP connectivity, multi-host coordination, persistence across process loss, universal latency, production traffic, or authority beyond the callable/payload explicitly supplied.

Proof boundaries calibrate claims. They do **not** justify shrinking Tasklet's future integration or the parent system's target.

## Relationship to GlacierEQ Systems

Tasklet is strongest as an **atomic execution primitive** inside a federated capability mesh—not as a global orchestrator and not as a subordinate that must surrender identity to one control plane.

```text
planner / operator-intent system
        ↓ selected atomic task
Tasklet runtime
        ↓ verified terminal receipt
workflow / evidence / federation / swarm layer
        ↓ composition into larger capability
```

Potential relationships include:

- AKOS or another planner may select/route work;
- Tasklet executes the already-defined atomic responsibility;
- Sigma/federation systems may coordinate cross-system state;
- Tower of Babel may inform language placement when a different implementation boundary earns its use;
- other repositories may call Tasklet as a peer capability through an explicit interface.

No relationship label makes another repository the owner of Tasklet's purpose or survival.

## Pro Code Elite Humanized Standard

Tasklet development should optimize for:

- explicit contracts;
- deterministic and inspectable semantics;
- strong concurrency/failure behavior;
- high-quality type/name/interface decisions;
- useful diagnostics;
- measurable performance where claimed;
- composability into large systems;
- maintainability under real operational pressure;
- no placeholder or governance theater.

**Tasklet is small at the atomic boundary so the larger system can be bigger, stronger, faster, and more trustworthy.**
