"""Behavioral and adversarial tests for the Tasklet Micro Agent Engine."""

from __future__ import annotations

import asyncio
import threading
import time
import unittest

from tasklet_micro_agent_engine import TaskletMicroAgentEngine


class SchedulerTests(unittest.TestCase):
    def test_spawn_and_priority_order(self) -> None:
        engine = TaskletMicroAgentEngine(max_concurrent_tasklets=8)
        engine.spawn_tasklet("low", {"job": "a"}, priority=50)
        engine.spawn_tasklet("high", {"job": "b"}, priority=1)
        engine.spawn_tasklet("mid", {"job": "c"}, priority=10)
        first = engine.next_tasklet()
        second = engine.next_tasklet()
        assert first is not None and second is not None
        self.assertEqual(first["tasklet_id"], "high")
        self.assertEqual(second["tasklet_id"], "mid")

    def test_concurrency_cap_rejects_registration(self) -> None:
        engine = TaskletMicroAgentEngine(max_concurrent_tasklets=2)
        self.assertTrue(engine.spawn_tasklet("a", {})["ok"])
        self.assertTrue(engine.spawn_tasklet("b", {})["ok"])
        result = engine.spawn_tasklet("c", {})
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "concurrency_cap")
        self.assertEqual(engine.efficiency()["rejected"], 1)

    def test_suspend_resume_preserves_snapshot(self) -> None:
        engine = TaskletMicroAgentEngine(max_concurrent_tasklets=4)
        engine.spawn_tasklet("t1", {"x": 1}, priority=5)
        self.assertTrue(engine.suspend_tasklet("t1")["ok"])
        result = engine.resume_tasklet("t1")
        self.assertTrue(result["ok"])
        self.assertEqual(result["payload"]["x"], 1)
        self.assertEqual(engine.efficiency()["resumed"], 1)

    def test_completed_tasklet_is_not_resumable(self) -> None:
        engine = TaskletMicroAgentEngine()
        engine.spawn_tasklet("done", {"n": 2})
        self.assertTrue(engine.complete_tasklet("done", result={"sum": 2})["ok"])
        result = engine.resume_tasklet("done")
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "cannot_resume_from_COMPLETED")

    def test_repeated_completion_is_rejected_without_metric_inflation(self) -> None:
        engine = TaskletMicroAgentEngine()
        engine.spawn_tasklet("done", {})
        first = engine.complete_tasklet("done", result="first")
        second = engine.complete_tasklet("done", result="second")
        self.assertTrue(first["ok"])
        self.assertFalse(second["ok"])
        self.assertEqual(second["error"], "cannot_complete_from_COMPLETED")
        self.assertEqual(engine.metrics["completed"], 1)
        self.assertEqual(engine.tasklet_registry["done"]["result"], "first")

    def test_suspended_tasklet_cannot_be_completed(self) -> None:
        engine = TaskletMicroAgentEngine()
        engine.spawn_tasklet("paused", {})
        self.assertTrue(engine.suspend_tasklet("paused")["ok"])
        result = engine.complete_tasklet("paused")
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "cannot_complete_from_SUSPENDED")
        self.assertEqual(engine.metrics["completed"], 0)

    def test_payload_budget_fails_closed(self) -> None:
        engine = TaskletMicroAgentEngine(max_payload_bytes=16)
        with self.assertRaisesRegex(ValueError, "max_payload_bytes"):
            engine.spawn_tasklet(payload={"value": "x" * 100})

    def test_non_finite_payload_is_rejected(self) -> None:
        engine = TaskletMicroAgentEngine()
        with self.assertRaisesRegex(ValueError, "JSON-serializable and finite"):
            engine.spawn_tasklet(payload={"value": float("nan")})

    def test_non_json_payload_object_is_rejected(self) -> None:
        engine = TaskletMicroAgentEngine()
        with self.assertRaisesRegex(ValueError, "JSON-serializable and finite"):
            engine.spawn_tasklet(payload={"value": object()})


class AsyncExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_handler_returns_structured_receipt(self) -> None:
        engine = TaskletMicroAgentEngine()

        async def handler(payload: dict[str, object]) -> int:
            await asyncio.sleep(0)
            return int(payload["value"]) * 2

        result = await engine.run_tasklet(handler, {"value": 21})
        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(result["result"], 42)
        self.assertFalse(result["replayed"])

    async def test_async_callable_object_is_awaited(self) -> None:
        engine = TaskletMicroAgentEngine()

        class Handler:
            async def __call__(self, payload: dict[str, object]) -> str:
                await asyncio.sleep(0)
                return f"done:{payload['value']}"

        result = await engine.run_tasklet(Handler(), {"value": 7})
        self.assertTrue(result["ok"])
        self.assertEqual(result["result"], "done:7")

    async def test_sync_callable_returning_awaitable_is_awaited(self) -> None:
        engine = TaskletMicroAgentEngine()

        def handler(payload: dict[str, object]):
            async def resolved() -> int:
                await asyncio.sleep(0)
                return int(payload["value"]) + 1

            return resolved()

        result = await engine.run_tasklet(handler, {"value": 8})
        self.assertTrue(result["ok"])
        self.assertEqual(result["result"], 9)

    async def test_sync_handler_runs_without_blocking_api_contract(self) -> None:
        engine = TaskletMicroAgentEngine()

        def handler(payload: dict[str, object]) -> str:
            return str(payload["value"]).upper()

        result = await engine.run_tasklet(handler, {"value": "tasklet"})
        self.assertEqual(result["result"], "TASKLET")
        self.assertEqual(engine.metrics["completed"], 1)

    async def test_deadline_produces_terminal_timeout_receipt(self) -> None:
        engine = TaskletMicroAgentEngine(max_timeout_s=1.0)

        async def slow_handler(_payload: dict[str, object]) -> None:
            await asyncio.sleep(0.05)

        result = await engine.run_tasklet(slow_handler, timeout_s=0.001)
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "TIMED_OUT")
        self.assertEqual(result["error"], "deadline_exceeded")
        self.assertFalse(result["execution_continues"])
        self.assertEqual(engine.metrics["timed_out"], 1)

    async def test_deadline_includes_wait_for_concurrency_slot(self) -> None:
        engine = TaskletMicroAgentEngine(max_concurrent_tasklets=1, max_timeout_s=1.0)
        entered = asyncio.Event()
        release = asyncio.Event()

        async def holder(_payload: dict[str, object]) -> str:
            entered.set()
            await release.wait()
            return "held"

        first = asyncio.create_task(engine.run_tasklet(holder))
        await entered.wait()
        started = time.perf_counter()
        # Short timeout: prove deadline covers wait-for-slot, not only handler body.
        # Bound is loose enough for loaded CI/dev hosts (not a micro-benchmark).
        second = await engine.run_tasklet(lambda _payload: "late", timeout_s=0.02)
        elapsed = time.perf_counter() - started
        self.assertFalse(second["ok"])
        self.assertEqual(second["status"], "TIMED_OUT")
        self.assertLess(elapsed, 0.25, f"deadline should fire promptly, elapsed={elapsed}")
        release.set()
        self.assertTrue((await first)["ok"])

    async def test_sync_timeout_reports_worker_continuation_and_holds_capacity(self) -> None:
        engine = TaskletMicroAgentEngine(max_concurrent_tasklets=1, max_timeout_s=1.0)
        worker_started = threading.Event()
        worker_release = threading.Event()

        def slow_sync(_payload: dict[str, object]) -> str:
            worker_started.set()
            worker_release.wait(timeout=1.0)
            return "finished"

        # Worker thread can lag slightly under load; wait for start signal
        # rather than racing a 10ms deadline against thread scheduling.
        timed_out = await engine.run_tasklet(slow_sync, timeout_s=0.05)
        self.assertTrue(
            worker_started.wait(timeout=1.0),
            "sync worker must start before capacity is released",
        )
        self.assertFalse(timed_out["ok"])
        self.assertEqual(timed_out["status"], "TIMED_OUT")
        self.assertTrue(timed_out["execution_continues"])

        blocked = await engine.run_tasklet(lambda _payload: "next", timeout_s=0.05)
        self.assertFalse(blocked["ok"])
        self.assertEqual(blocked["status"], "TIMED_OUT")

        worker_release.set()
        # Wait until capacity frees (worker finishes after release)
        deadline = time.perf_counter() + 1.0
        completed = None
        while time.perf_counter() < deadline:
            completed = await engine.run_tasklet(lambda _payload: "next", timeout_s=0.2)
            if completed.get("ok"):
                break
            await asyncio.sleep(0.01)
        self.assertIsNotNone(completed)
        self.assertTrue(completed["ok"])
        self.assertEqual(completed["result"], "next")

    async def test_handler_failure_is_bounded_as_receipt(self) -> None:
        engine = TaskletMicroAgentEngine()

        def failing_handler(_payload: dict[str, object]) -> None:
            raise RuntimeError("bounded failure")

        result = await engine.run_tasklet(failing_handler)
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "FAILED")
        self.assertIn("RuntimeError", result["error"])
        self.assertEqual(engine.metrics["failed"], 1)

    async def test_idempotent_completed_work_replays_result(self) -> None:
        engine = TaskletMicroAgentEngine()
        executions = 0

        async def handler(payload: dict[str, object]) -> int:
            nonlocal executions
            executions += 1
            return int(payload["value"])

        first = await engine.run_tasklet(
            handler,
            {"value": 7},
            idempotency_key="stable-7",
        )
        second = await engine.run_tasklet(
            handler,
            {"value": 999},
            idempotency_key="stable-7",
        )
        self.assertEqual(first["result"], 7)
        self.assertEqual(second["result"], 7)
        self.assertTrue(second["replayed"])
        self.assertEqual(executions, 1)
        self.assertEqual(engine.metrics["idempotent_replays"], 1)

    async def test_stale_idempotency_pointer_is_recovered(self) -> None:
        engine = TaskletMicroAgentEngine()
        first = await engine.run_tasklet(
            lambda _payload: "first",
            idempotency_key="recoverable",
        )
        del engine.tasklet_registry[first["tasklet_id"]]
        second = await engine.run_tasklet(
            lambda _payload: "second",
            idempotency_key="recoverable",
        )
        self.assertTrue(second["ok"])
        self.assertEqual(second["result"], "second")

    async def test_concurrent_duplicate_idempotency_key_fails_closed(self) -> None:
        engine = TaskletMicroAgentEngine()
        entered = asyncio.Event()
        release = asyncio.Event()

        async def handler(_payload: dict[str, object]) -> str:
            entered.set()
            await release.wait()
            return "done"

        first_task = asyncio.create_task(
            engine.run_tasklet(handler, idempotency_key="same")
        )
        await entered.wait()
        duplicate = await engine.run_tasklet(handler, idempotency_key="same")
        self.assertFalse(duplicate["ok"])
        self.assertEqual(duplicate["error"], "idempotency_in_flight")
        release.set()
        first = await first_task
        self.assertTrue(first["ok"])

    async def test_runtime_managed_tasklet_rejects_external_lifecycle_mutation(self) -> None:
        engine = TaskletMicroAgentEngine()
        entered = asyncio.Event()
        release = asyncio.Event()

        async def handler(_payload: dict[str, object]) -> str:
            entered.set()
            await release.wait()
            return "done"

        task = asyncio.create_task(engine.run_tasklet(handler, tasklet_id="runtime"))
        await entered.wait()
        self.assertEqual(
            engine.complete_tasklet("runtime")["error"],
            "runtime_managed_execution",
        )
        self.assertEqual(
            engine.suspend_tasklet("runtime")["error"],
            "runtime_managed_execution",
        )
        self.assertEqual(
            engine.resume_tasklet("runtime")["error"],
            "runtime_managed_execution",
        )
        release.set()
        self.assertTrue((await task)["ok"])

    async def test_execution_concurrency_never_exceeds_configured_cap(self) -> None:
        engine = TaskletMicroAgentEngine(max_concurrent_tasklets=2)
        active = 0
        observed_max = 0
        lock = asyncio.Lock()

        async def handler(payload: dict[str, object]) -> int:
            nonlocal active, observed_max
            async with lock:
                active += 1
                observed_max = max(observed_max, active)
            await asyncio.sleep(0.01)
            async with lock:
                active -= 1
            return int(payload["value"])

        results = await asyncio.gather(
            *(engine.run_tasklet(handler, {"value": index}) for index in range(8))
        )
        self.assertTrue(all(result["ok"] for result in results))
        self.assertLessEqual(observed_max, 2)

    async def test_timeout_above_policy_is_rejected_before_execution(self) -> None:
        engine = TaskletMicroAgentEngine(max_timeout_s=2.0)

        async def handler(_payload: dict[str, object]) -> None:
            return None

        with self.assertRaisesRegex(ValueError, "max_timeout_s"):
            await engine.run_tasklet(handler, timeout_s=3.0)


if __name__ == "__main__":
    unittest.main()
