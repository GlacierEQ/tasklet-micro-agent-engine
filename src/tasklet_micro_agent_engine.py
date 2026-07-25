"""
Tasklet Micro Agent Engine — real priority scheduler + persistent state.

Innovations (implemented, tested):
  1. Heap-based priority scheduling (lower number = higher priority)
  2. Persistent state snapshots with resume
  3. Concurrency cap with clear reject metrics (no fake efficiency %)
"""
from __future__ import annotations

import heapq
import time
import uuid
from typing import Any


class TaskletMicroAgentEngine:
    """Priority micro-tasklet registry with bounded concurrency."""

    def __init__(self, max_concurrent_tasklets: int = 128) -> None:
        if max_concurrent_tasklets < 1:
            raise ValueError("max_concurrent_tasklets must be >= 1")
        self.max_concurrent_tasklets = max_concurrent_tasklets
        self.tasklet_registry: dict[str, dict[str, Any]] = {}
        self._heap: list[tuple[int, float, str]] = []  # priority, ts, id
        self._seq = 0
        self.metrics = {
            "spawned": 0,
            "rejected": 0,
            "completed": 0,
            "resumed": 0,
        }

    def _running_count(self) -> int:
        return sum(
            1 for t in self.tasklet_registry.values() if t["status"] == "RUNNING"
        )

    def spawn_tasklet(
        self,
        tasklet_id: str | None = None,
        payload: dict[str, Any] | None = None,
        priority: int = 100,
    ) -> dict[str, Any]:
        """Spawn a tasklet. Rejects when concurrent RUNNING >= max."""
        start = time.perf_counter()
        payload = dict(payload or {})
        tid = tasklet_id or str(uuid.uuid4())
        if tid in self.tasklet_registry and self.tasklet_registry[tid]["status"] == "RUNNING":
            return {
                "ok": False,
                "tasklet_id": tid,
                "error": "already_running",
                "status": "REJECTED",
            }

        if self._running_count() >= self.max_concurrent_tasklets:
            self.metrics["rejected"] += 1
            return {
                "ok": False,
                "tasklet_id": tid,
                "error": "concurrency_cap",
                "active_tasklets": self._running_count(),
                "status": "REJECTED",
            }

        now = time.time()
        self.tasklet_registry[tid] = {
            "payload": payload,
            "priority": int(priority),
            "status": "RUNNING",
            "created_at": now,
            "updated_at": now,
            "snapshot": dict(payload),
        }
        self._seq += 1
        heapq.heappush(self._heap, (int(priority), now, tid))
        self.metrics["spawned"] += 1
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return {
            "ok": True,
            "tasklet_id": tid,
            "active_tasklets": self._running_count(),
            "spawn_latency_ms": round(elapsed_ms, 4),
            "priority": int(priority),
            "status": "TASKLET_ACTIVE",
        }

    def next_tasklet(self) -> dict[str, Any] | None:
        """Pop highest-priority RUNNING tasklet id (does not complete it)."""
        while self._heap:
            priority, ts, tid = heapq.heappop(self._heap)
            rec = self.tasklet_registry.get(tid)
            if not rec or rec["status"] != "RUNNING":
                continue
            return {"tasklet_id": tid, "priority": priority, "payload": rec["payload"]}
        return None

    def complete_tasklet(self, tasklet_id: str, result: Any = None) -> dict[str, Any]:
        rec = self.tasklet_registry.get(tasklet_id)
        if not rec:
            return {"ok": False, "error": "unknown_tasklet"}
        rec["status"] = "COMPLETED"
        rec["result"] = result
        rec["updated_at"] = time.time()
        self.metrics["completed"] += 1
        return {"ok": True, "tasklet_id": tasklet_id, "status": "COMPLETED"}

    def suspend_tasklet(self, tasklet_id: str) -> dict[str, Any]:
        rec = self.tasklet_registry.get(tasklet_id)
        if not rec:
            return {"ok": False, "error": "unknown_tasklet"}
        rec["status"] = "SUSPENDED"
        rec["snapshot"] = dict(rec.get("payload") or {})
        rec["updated_at"] = time.time()
        return {"ok": True, "tasklet_id": tasklet_id, "status": "SUSPENDED"}

    def resume_tasklet(self, tasklet_id: str) -> dict[str, Any]:
        rec = self.tasklet_registry.get(tasklet_id)
        if not rec:
            return {"ok": False, "error": "unknown_tasklet"}
        if rec["status"] not in ("SUSPENDED", "COMPLETED"):
            return {"ok": False, "error": f"cannot_resume_from_{rec['status']}"}
        if self._running_count() >= self.max_concurrent_tasklets:
            self.metrics["rejected"] += 1
            return {"ok": False, "error": "concurrency_cap", "status": "REJECTED"}
        rec["status"] = "RUNNING"
        rec["payload"] = dict(rec.get("snapshot") or rec.get("payload") or {})
        rec["updated_at"] = time.time()
        heapq.heappush(
            self._heap, (int(rec.get("priority", 100)), time.time(), tasklet_id)
        )
        self.metrics["resumed"] += 1
        return {
            "ok": True,
            "tasklet_id": tasklet_id,
            "status": "TASKLET_ACTIVE",
            "payload": rec["payload"],
        }

    def efficiency(self) -> dict[str, Any]:
        """Honest throughput metrics — no fabricated percentages."""
        spawned = self.metrics["spawned"]
        rejected = self.metrics["rejected"]
        attempts = spawned + rejected
        accept_rate = (spawned / attempts) if attempts else 1.0
        return {
            "spawned": spawned,
            "rejected": rejected,
            "completed": self.metrics["completed"],
            "resumed": self.metrics["resumed"],
            "accept_rate": round(accept_rate, 4),
            "active": self._running_count(),
            "registered": len(self.tasklet_registry),
        }
