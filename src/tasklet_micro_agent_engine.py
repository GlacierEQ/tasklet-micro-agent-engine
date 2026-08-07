"""Tasklet Micro Agent Engine.

A small, dependency-free execution runtime for bounded atomic work. The engine
separates task registration from execution, enforces concurrency and payload
limits, supports deterministic priority selection, provides idempotent async
dispatch, and records terminal outcomes without pretending an external agent or
MCP provider exists.
"""

from __future__ import annotations

import asyncio
import heapq
import inspect
import json
import math
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

TaskHandler = Callable[[dict[str, Any]], Any | Awaitable[Any]]
TERMINAL_STATES = {"COMPLETED", "FAILED", "TIMED_OUT", "CANCELLED"}


class TaskletMicroAgentEngine:
    """Priority tasklet registry with bounded local execution.

    ``spawn_tasklet`` preserves the original registry/scheduler surface.
    ``run_tasklet`` is the execution surface: it admits work only within the
    configured bounds, executes sync or async handlers, applies a deadline,
    preserves idempotency, and emits structured terminal state.
    """

    def __init__(
        self,
        max_concurrent_tasklets: int = 128,
        *,
        max_payload_bytes: int = 64 * 1024,
        max_timeout_s: float = 60.0,
    ) -> None:
        if max_concurrent_tasklets < 1:
            raise ValueError("max_concurrent_tasklets must be >= 1")
        if max_payload_bytes < 1:
            raise ValueError("max_payload_bytes must be >= 1")
        if not math.isfinite(max_timeout_s) or max_timeout_s <= 0:
            raise ValueError("max_timeout_s must be finite and > 0")

        self.max_concurrent_tasklets = max_concurrent_tasklets
        self.max_payload_bytes = max_payload_bytes
        self.max_timeout_s = float(max_timeout_s)
        self.tasklet_registry: dict[str, dict[str, Any]] = {}
        self._heap: list[tuple[int, int, str]] = []
        self._seq = 0
        self._semaphore = asyncio.Semaphore(max_concurrent_tasklets)
        self._state_lock = asyncio.Lock()
        self._idempotency_index: dict[str, str] = {}
        self.metrics = {
            "spawned": 0,
            "rejected": 0,
            "completed": 0,
            "failed": 0,
            "timed_out": 0,
            "cancelled": 0,
            "resumed": 0,
            "idempotent_replays": 0,
        }

    @staticmethod
    def _validate_payload(payload: Mapping[str, Any] | None) -> dict[str, Any]:
        if payload is None:
            return {}
        if not isinstance(payload, Mapping):
            raise TypeError("payload must be a mapping")
        return dict(payload)

    def _payload_size(self, payload: Mapping[str, Any]) -> int:
        try:
            encoded = json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
                default=str,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError("payload must be JSON-serializable and finite") from exc
        return len(encoded)

    def _validate_admission(
        self,
        payload: Mapping[str, Any],
        timeout_s: float | None = None,
    ) -> float | None:
        if self._payload_size(payload) > self.max_payload_bytes:
            raise ValueError("payload exceeds max_payload_bytes")
        if timeout_s is None:
            return None
        if isinstance(timeout_s, bool) or not isinstance(timeout_s, (int, float)):
            raise TypeError("timeout_s must be a finite number")
        timeout = float(timeout_s)
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout_s must be finite and > 0")
        if timeout > self.max_timeout_s:
            raise ValueError("timeout_s exceeds max_timeout_s")
        return timeout

    def _running_count(self) -> int:
        return sum(
            1
            for tasklet in self.tasklet_registry.values()
            if tasklet["status"] == "RUNNING"
        )

    def _record(
        self,
        tasklet_id: str,
        payload: dict[str, Any],
        priority: int,
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        now = time.time()
        record: dict[str, Any] = {
            "payload": payload,
            "priority": int(priority),
            "status": "RUNNING",
            "created_at": now,
            "updated_at": now,
            "snapshot": dict(payload),
            "idempotency_key": idempotency_key,
        }
        self.tasklet_registry[tasklet_id] = record
        self._seq += 1
        heapq.heappush(self._heap, (int(priority), self._seq, tasklet_id))
        self.metrics["spawned"] += 1
        return record

    def spawn_tasklet(
        self,
        tasklet_id: str | None = None,
        payload: Mapping[str, Any] | None = None,
        priority: int = 100,
    ) -> dict[str, Any]:
        """Register a tasklet in the deterministic priority scheduler."""

        start = time.perf_counter()
        normalized = self._validate_payload(payload)
        self._validate_admission(normalized)
        tasklet_id = tasklet_id or str(uuid.uuid4())

        existing = self.tasklet_registry.get(tasklet_id)
        if existing and existing["status"] == "RUNNING":
            self.metrics["rejected"] += 1
            return {
                "ok": False,
                "tasklet_id": tasklet_id,
                "error": "already_running",
                "status": "REJECTED",
            }

        if self._running_count() >= self.max_concurrent_tasklets:
            self.metrics["rejected"] += 1
            return {
                "ok": False,
                "tasklet_id": tasklet_id,
                "error": "concurrency_cap",
                "active_tasklets": self._running_count(),
                "status": "REJECTED",
            }

        self._record(tasklet_id, normalized, priority)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return {
            "ok": True,
            "tasklet_id": tasklet_id,
            "active_tasklets": self._running_count(),
            "spawn_latency_ms": round(elapsed_ms, 6),
            "priority": int(priority),
            "status": "TASKLET_ACTIVE",
        }

    def next_tasklet(self) -> dict[str, Any] | None:
        """Return the highest-priority currently running tasklet."""

        while self._heap:
            priority, _sequence, tasklet_id = heapq.heappop(self._heap)
            record = self.tasklet_registry.get(tasklet_id)
            if not record or record["status"] != "RUNNING":
                continue
            return {
                "tasklet_id": tasklet_id,
                "priority": priority,
                "payload": dict(record["payload"]),
            }
        return None

    def _finish(
        self,
        tasklet_id: str,
        status: str,
        *,
        result: Any = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        record = self.tasklet_registry.get(tasklet_id)
        if not record:
            return {"ok": False, "error": "unknown_tasklet"}
        record["status"] = status
        record["updated_at"] = time.time()
        if result is not None:
            record["result"] = result
        if error is not None:
            record["error"] = error
        return record

    def complete_tasklet(self, tasklet_id: str, result: Any = None) -> dict[str, Any]:
        if tasklet_id not in self.tasklet_registry:
            return {"ok": False, "error": "unknown_tasklet"}
        self._finish(tasklet_id, "COMPLETED", result=result)
        self.metrics["completed"] += 1
        return {"ok": True, "tasklet_id": tasklet_id, "status": "COMPLETED"}

    def suspend_tasklet(self, tasklet_id: str) -> dict[str, Any]:
        record = self.tasklet_registry.get(tasklet_id)
        if not record:
            return {"ok": False, "error": "unknown_tasklet"}
        if record["status"] != "RUNNING":
            return {
                "ok": False,
                "error": f"cannot_suspend_from_{record['status']}",
            }
        record["status"] = "SUSPENDED"
        record["snapshot"] = dict(record.get("payload") or {})
        record["updated_at"] = time.time()
        return {"ok": True, "tasklet_id": tasklet_id, "status": "SUSPENDED"}

    def resume_tasklet(self, tasklet_id: str) -> dict[str, Any]:
        record = self.tasklet_registry.get(tasklet_id)
        if not record:
            return {"ok": False, "error": "unknown_tasklet"}
        if record["status"] != "SUSPENDED":
            return {
                "ok": False,
                "error": f"cannot_resume_from_{record['status']}",
            }
        if self._running_count() >= self.max_concurrent_tasklets:
            self.metrics["rejected"] += 1
            return {
                "ok": False,
                "error": "concurrency_cap",
                "status": "REJECTED",
            }
        record["status"] = "RUNNING"
        record["payload"] = dict(record.get("snapshot") or {})
        record["updated_at"] = time.time()
        self._seq += 1
        heapq.heappush(
            self._heap,
            (int(record.get("priority", 100)), self._seq, tasklet_id),
        )
        self.metrics["resumed"] += 1
        return {
            "ok": True,
            "tasklet_id": tasklet_id,
            "status": "TASKLET_ACTIVE",
            "payload": dict(record["payload"]),
        }

    async def run_tasklet(
        self,
        handler: TaskHandler,
        payload: Mapping[str, Any] | None = None,
        *,
        tasklet_id: str | None = None,
        idempotency_key: str | None = None,
        priority: int = 100,
        timeout_s: float | None = None,
    ) -> dict[str, Any]:
        """Execute one bounded tasklet and return a structured receipt.

        The method accepts synchronous or asynchronous handlers. Synchronous
        handlers execute via ``asyncio.to_thread`` so they do not block the
        event loop. Duplicate completed idempotency keys replay the prior
        terminal result; concurrent duplicates fail closed.
        """

        normalized = self._validate_payload(payload)
        timeout = self._validate_admission(normalized, timeout_s)
        if not callable(handler):
            raise TypeError("handler must be callable")
        if idempotency_key is not None and not idempotency_key.strip():
            raise ValueError("idempotency_key must be non-empty when supplied")

        async with self._state_lock:
            if idempotency_key is not None:
                previous_id = self._idempotency_index.get(idempotency_key)
                if previous_id:
                    previous = self.tasklet_registry[previous_id]
                    if previous["status"] == "COMPLETED":
                        self.metrics["idempotent_replays"] += 1
                        return {
                            "ok": True,
                            "tasklet_id": previous_id,
                            "status": "COMPLETED",
                            "result": previous.get("result"),
                            "replayed": True,
                            "idempotency_key": idempotency_key,
                        }
                    if previous["status"] not in TERMINAL_STATES:
                        self.metrics["rejected"] += 1
                        return {
                            "ok": False,
                            "tasklet_id": previous_id,
                            "status": "REJECTED",
                            "error": "idempotency_in_flight",
                            "idempotency_key": idempotency_key,
                        }

            resolved_id = tasklet_id or str(uuid.uuid4())
            existing = self.tasklet_registry.get(resolved_id)
            if existing and existing["status"] not in TERMINAL_STATES:
                self.metrics["rejected"] += 1
                return {
                    "ok": False,
                    "tasklet_id": resolved_id,
                    "status": "REJECTED",
                    "error": "already_running",
                }
            self._record(
                resolved_id,
                normalized,
                priority,
                idempotency_key=idempotency_key,
            )
            if idempotency_key is not None:
                self._idempotency_index[idempotency_key] = resolved_id

        started = time.perf_counter()
        try:
            async with self._semaphore:
                if inspect.iscoroutinefunction(handler):
                    awaitable = handler(dict(normalized))
                else:
                    awaitable = asyncio.to_thread(handler, dict(normalized))
                if timeout is None:
                    result = await awaitable
                else:
                    result = await asyncio.wait_for(awaitable, timeout=timeout)
        except TimeoutError:
            self._finish(resolved_id, "TIMED_OUT", error="deadline_exceeded")
            self.metrics["timed_out"] += 1
            return {
                "ok": False,
                "tasklet_id": resolved_id,
                "status": "TIMED_OUT",
                "error": "deadline_exceeded",
                "elapsed_ms": round((time.perf_counter() - started) * 1000, 6),
            }
        except asyncio.CancelledError:
            self._finish(resolved_id, "CANCELLED", error="cancelled")
            self.metrics["cancelled"] += 1
            raise
        except Exception as exc:  # noqa: BLE001 - boundary converts failures to receipts
            self._finish(
                resolved_id,
                "FAILED",
                error=f"{type(exc).__name__}: {exc}",
            )
            self.metrics["failed"] += 1
            return {
                "ok": False,
                "tasklet_id": resolved_id,
                "status": "FAILED",
                "error": f"{type(exc).__name__}: {exc}",
                "elapsed_ms": round((time.perf_counter() - started) * 1000, 6),
            }

        self._finish(resolved_id, "COMPLETED", result=result)
        self.metrics["completed"] += 1
        return {
            "ok": True,
            "tasklet_id": resolved_id,
            "status": "COMPLETED",
            "result": result,
            "replayed": False,
            "idempotency_key": idempotency_key,
            "elapsed_ms": round((time.perf_counter() - started) * 1000, 6),
        }

    def efficiency(self) -> dict[str, Any]:
        """Return observed counters without inventing performance percentages."""

        spawned = self.metrics["spawned"]
        rejected = self.metrics["rejected"]
        attempts = spawned + rejected
        accept_rate = (spawned / attempts) if attempts else 1.0
        return {
            **self.metrics,
            "accept_rate": round(accept_rate, 4),
            "active": self._running_count(),
            "registered": len(self.tasklet_registry),
        }


_DEFAULT_ENGINE: TaskletMicroAgentEngine | None = None


def default_engine() -> TaskletMicroAgentEngine:
    """Return the process-local default runtime used by the convenience API."""

    global _DEFAULT_ENGINE
    if _DEFAULT_ENGINE is None:
        _DEFAULT_ENGINE = TaskletMicroAgentEngine()
    return _DEFAULT_ENGINE


async def run_tasklet(
    handler: TaskHandler,
    payload: Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Convenience API for one bounded tasklet execution."""

    return await default_engine().run_tasklet(handler, payload, **kwargs)
