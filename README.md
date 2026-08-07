# Tasklet Micro Agent Engine

> **Small work should not require a heavyweight agent runtime. Tasklet makes atomic execution bounded, idempotent, deadline-aware, inspectable, and cheap to schedule.**

*Recruiter signal: a focused agent-infrastructure runtime, not another orchestration diagram.*

## The Smallest Useful Execution Boundary

Tasklet is a dependency-free Python runtime for **atomic local work**. It is designed for jobs that are too small to justify a full multi-turn agent lifecycle but still need the controls that toy executors usually omit.

The runtime implements:

- deterministic priority registration;
- bounded concurrent handler execution;
- synchronous **and** asynchronous handlers;
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

`run_tasklet()` is protocol-neutral. It is suitable for wrapping behind MCP or another tool protocol, but this repository does **not** claim an MCP server or external provider registration unless a separate adapter and provider receipt prove it.

### Why this matters

Large agent frameworks are valuable when work needs planning, memory, dialogue, delegation, or broad tool choice. They add unnecessary state and coordination cost when the job is already known and bounded.

Tasklet isolates that smaller execution class:

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
```

The architectural claim is therefore narrow and testable: **atomic work can retain production-shaped execution controls without inheriting a heavyweight agent lifecycle.**

## Inside the Bounded Runtime

*Engineering review: implementation, failure behavior, and evidence.*

### Core surfaces

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
- User exceptions are converted to bounded `FAILED` receipts rather than escaping as fabricated success.
- Cancellation records `CANCELLED` and propagates `CancelledError` to preserve cooperative cancellation semantics.

### Idempotency

When an `idempotency_key` is supplied:

- a completed prior execution returns the original result with `replayed: true`;
- a duplicate while the first execution remains in flight is rejected as `idempotency_in_flight`;
- failed terminal work may be retried by a later execution rather than becoming permanently poisoned.

This is deliberately process-local idempotency. Durable cross-process exactly-once semantics are **not** claimed without an external durable store and reconciliation protocol.

### Admission limits

The runtime fails closed for:

- non-mapping payloads;
- non-finite/non-serializable JSON payload values;
- payloads above `max_payload_bytes`;
- invalid or excessive deadlines;
- duplicate active tasklet IDs;
- scheduler registrations above the configured active-task limit.

### Native verification

The exact-head CI contract is:

```bash
python -m pip install -e ".[dev]"
python -m ruff check src tests scripts mastermind_sidecar.py
python -m compileall -q src tests scripts mastermind_sidecar.py
python -m pytest --junitxml=artifacts/junit.xml
python scripts/verify_junit.py artifacts/junit.xml \
  --output artifacts/test-receipt.json
python mastermind_sidecar.py
python scripts/benchmark_spawn.py \
  --iterations 5000 \
  --warmup 500 \
  --require-sub-ms-p95
python scripts/verify_integrity.py --check
```

GitHub Actions checks out the **literal pull-request head SHA**, proves checkout identity, and repeats the correctness suite across Python **3.11, 3.12, and 3.13**. The benchmark runs separately on a disclosed GitHub-hosted Ubuntu runner. The integrity gate hashes every tracked regular file except the manifest itself and rejects drift.

### Measured proof

An exact-head provider run on commit `b270b0d01fbf33e52e25affa78b61cb4d197e620` established the current pre-promotion evidence baseline:

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

The final promotion rule is stronger than this historical receipt: **the current reviewed head must independently reproduce the complete green gate.**

### Performance claim boundary

The benchmark measures **local registration latency only**. Handler execution, network transport, provider latency, tool latency, and end-to-end agent latency are excluded.

The CI gate requires measured **p95 registration latency below 1 millisecond on the disclosed runner**. The recorded baseline above clears that threshold. This is a reproducible bounded benchmark, not a universal hardware guarantee.

## Enter Through the Contract

*Machine/toolchain surface: stable names, bounded authority, and non-claims.*

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
  "terminal_states": [
    "COMPLETED",
    "FAILED",
    "TIMED_OUT",
    "CANCELLED"
  ]
}
```

### Evidence boundary

This repository can prove its scheduler, local execution behavior, disclosed-runner registration benchmark, repository health, and tracked-file integrity when the current-head CI receipts are green.

It does **not** by itself prove:

- distributed exactly-once execution;
- provider or MCP connectivity;
- multi-host coordination;
- persistence across process loss;
- universal sub-millisecond latency;
- production deployment or customer traffic;
- autonomous authority beyond the callable and payload explicitly supplied by the caller.

Those are separate engineering layers and require separate receipts.

## Relationship to the GlacierEQ System

Tasklet is strongest as a **narrow execution primitive**, not as another global orchestrator.

```text
AKOS / governed control plane
        ↓ bounded task
Tasklet runtime
        ↓ terminal receipt
Evidence / federation layer
```

- **AKOS** can decide whether a bounded action is authorized.
- **Tasklet** can execute the already-selected atomic work.
- **Sigma / federation layers** can own integration state across systems.
- **Tower of Babel** can determine when a different language boundary is justified.

That separation is intentional: Tasklet owns **small execution**, not global policy, memory, company identity, or unrestricted tool authority.

## Promotion Authority

Tasklet does not self-promote from README text. The authoritative promotion state is the **current exact-head GitHub Actions result plus its JUnit, health, benchmark, and integrity artifacts**.

A green current-head run earns TEST evidence for the bounded local runtime and the disclosed-runner latency claim. A failing, stale, or absent current-head run leaves those claims unpromoted even when an older receipt was green.
