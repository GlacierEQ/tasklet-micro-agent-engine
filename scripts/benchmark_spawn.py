#!/usr/bin/env python3
"""Measure tasklet registration latency and emit a truthful benchmark receipt."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import statistics
import sys
import time
from pathlib import Path

from tasklet_micro_agent_engine import TaskletMicroAgentEngine


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(
        len(ordered) - 1,
        max(0, int((len(ordered) - 1) * fraction)),
    )
    return ordered[index]


def benchmark(
    iterations: int,
    warmup: int,
    *,
    commit: str,
) -> dict[str, object]:
    if iterations < 100:
        raise ValueError("iterations must be >= 100")
    if warmup < 0:
        raise ValueError("warmup must be >= 0")
    if not commit:
        raise ValueError("commit identity is required")

    engine = TaskletMicroAgentEngine(max_concurrent_tasklets=1)
    for index in range(warmup):
        tasklet_id = f"warmup-{index}"
        result = engine.spawn_tasklet(tasklet_id, {"value": index})
        if not result["ok"]:
            raise RuntimeError(f"warmup registration failed: {result}")
        engine.complete_tasklet(tasklet_id)

    latencies_ms: list[float] = []
    for index in range(iterations):
        tasklet_id = f"bench-{index}"
        started = time.perf_counter_ns()
        result = engine.spawn_tasklet(tasklet_id, {"value": index})
        elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
        if not result["ok"]:
            raise RuntimeError(f"benchmark registration failed: {result}")
        engine.complete_tasklet(tasklet_id)
        latencies_ms.append(elapsed_ms)

    p95_ms = percentile(latencies_ms, 0.95)
    receipt: dict[str, object] = {
        "schema": "glaciereq.tasklet.spawn-benchmark.v1",
        "commit": commit,
        "iterations": iterations,
        "warmup": warmup,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "metric": "local tasklet registration latency; handler execution excluded",
        "p50_ms": round(statistics.median(latencies_ms), 6),
        "p95_ms": round(p95_ms, 6),
        "p99_ms": round(percentile(latencies_ms, 0.99), 6),
        "max_ms": round(max(latencies_ms), 6),
        "mean_ms": round(statistics.fmean(latencies_ms), 6),
        "sub_millisecond_p95": p95_ms < 1.0,
        "truth_boundary": (
            "This benchmark measures local registration only on the disclosed "
            "runner. It is not a network, provider, end-to-end agent, or "
            "universal latency claim."
        ),
    }
    payload = json.dumps(
        receipt,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    receipt["receipt_sha256"] = hashlib.sha256(payload).hexdigest()
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=5000)
    parser.add_argument("--warmup", type=int, default=500)
    parser.add_argument("--require-sub-ms-p95", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--commit",
        default=os.environ.get("VERIFIED_SHA") or os.environ.get("GITHUB_SHA") or "LOCAL",
    )
    args = parser.parse_args()

    try:
        receipt = benchmark(
            args.iterations,
            args.warmup,
            commit=args.commit,
        )
    except (RuntimeError, ValueError) as exc:
        print(f"benchmark failed: {exc}", file=sys.stderr)
        return 1

    text = json.dumps(receipt, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    print(text, end="")
    if args.require_sub_ms_p95 and not receipt["sub_millisecond_p95"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
